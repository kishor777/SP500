import time
import pandas as pd
from datetime import date as _date
from flask import Blueprint, jsonify, render_template

import state
from helpers import clean, _price_changes
from extensions import limiter, require_auth
from db_config import get_engine
from sentiment_engine import load_all_scores, run_sentiment_pass, should_run_sentiment, sentiment_status
from config import (
    RECO_MOMENTUM_WEIGHTS, RECO_FUNDAMENTAL_FIELDS, RECO_VALUATION_FIELDS,
    RECO_GURU_TAG_WEIGHT, RECO_ANALYST_SCORE_MAP,
    RECO_SIGNAL_WEIGHTS, RECO_LABEL_THRESHOLDS, _RECO_TTL,
)

bp = Blueprint("recommendations", __name__)

# ── Scoring helpers ────────────────────────────────────────────────────────────
def _pct_rank(series: pd.Series) -> pd.Series:
    return series.rank(pct=True, na_option="bottom") * 100

def _inv_pct_rank(series: pd.Series) -> pd.Series:
    return (1 - series.rank(pct=True, na_option="top")) * 100

def _compute_recommendations() -> list:
    from sqlalchemy import text as _t

    try:
        engine = get_engine()
        with engine.connect() as conn:
            rows = conn.execute(_t("""
                SELECT i.ticker, i.close
                FROM sp500_intraday i
                INNER JOIN (
                    SELECT ticker, MAX(dt) AS max_dt
                    FROM sp500_intraday WHERE DATE(dt) = CURDATE() GROUP BY ticker
                ) latest ON i.ticker = latest.ticker AND i.dt = latest.max_dt
            """)).fetchall()
        _intraday_latest = {r.ticker: float(r.close) for r in rows}
    except Exception:
        _intraday_latest = {}

    scores = pd.DataFrame(index=state.df.index)

    # 1. Momentum
    mom_rows = {}
    for ticker in state.df.index:
        pc = _price_changes(ticker)
        if pc:
            mom_rows[ticker] = pc
    mom_df = pd.DataFrame(mom_rows).T
    if not mom_df.empty:
        weighted = pd.Series(0.0, index=mom_df.index)
        weights  = RECO_MOMENTUM_WEIGHTS
        w_total  = sum(v for k, v in weights.items() if k in mom_df.columns)
        for k, w in weights.items():
            if k in mom_df.columns:
                weighted += mom_df[k].fillna(0) * (w / w_total)
        scores["momentum"] = _pct_rank(weighted.reindex(state.df.index))
    else:
        scores["momentum"] = 50.0

    # 2. Fundamental quality
    fund_score = pd.Series(0.0, index=state.df.index)
    w_used = 0.0
    for fld, w in RECO_FUNDAMENTAL_FIELDS.items():
        if fld in state.df.columns:
            fund_score += _pct_rank(state.df[fld]) * w
            w_used += w
    scores["fundamental"] = (fund_score / w_used) if w_used else 50.0

    # 3. Valuation
    val_score = pd.Series(0.0, index=state.df.index)
    w_used = 0.0
    for fld, w in RECO_VALUATION_FIELDS.items():
        if fld in state.df.columns:
            col = state.df[fld].clip(lower=0)
            val_score += _inv_pct_rank(col) * w
            w_used += w
    scores["valuation"] = (val_score / w_used) if w_used else 50.0

    # 4. Guru conviction
    TAG_WEIGHT = RECO_GURU_TAG_WEIGHT
    try:
        engine = get_engine()
        with engine.connect() as conn:
            rows = conn.execute(_t("""
                SELECT h.ticker, h.change_tag
                FROM guru_holdings h
                INNER JOIN guru_filing_meta m ON m.slug = h.slug AND m.filing_date = h.filing_date
                WHERE h.filing_date = (
                    SELECT MAX(filing_date) FROM guru_filing_meta m2 WHERE m2.slug = h.slug
                )
                AND h.ticker IS NOT NULL AND h.ticker != ''
            """)).fetchall()
        guru_scores: dict = {}
        for r in rows:
            tk  = r.ticker
            tag = (r.change_tag or "").lower()
            guru_scores.setdefault(tk, {"count": 0, "score": 0.0})
            guru_scores[tk]["count"] += 1
            guru_scores[tk]["score"] += TAG_WEIGHT.get(tag, 1.0)
        if guru_scores:
            max_score = max(v["score"] for v in guru_scores.values()) or 1
            guru_series = pd.Series(
                {tk: v["score"] / max_score * 100 for tk, v in guru_scores.items()}, dtype=float
            ).reindex(state.df.index, fill_value=0.0)
        else:
            guru_series = pd.Series(0.0, index=state.df.index)
        scores["guru"] = guru_series
        guru_counts = pd.Series(
            {tk: v["count"] for tk, v in guru_scores.items()}, dtype=int
        ).reindex(state.df.index, fill_value=0)
    except Exception as e:
        print(f"[reco] guru score error: {e}")
        scores["guru"] = 50.0
        guru_counts = pd.Series(0, index=state.df.index)

    # 5. Analyst consensus
    if "recommendationKey" in state.df.columns:
        scores["analyst"] = state.df["recommendationKey"].map(
            lambda x: RECO_ANALYST_SCORE_MAP.get(str(x).lower().replace(" ", ""), 50) if pd.notna(x) else 50
        ).astype(float)
    else:
        scores["analyst"] = 50.0

    # 6. Sentiment
    engine = get_engine()
    sentiment_map = load_all_scores(engine)
    prelim_order = scores[["momentum", "fundamental", "valuation", "guru", "analyst"]].mean(axis=1)
    sorted_prelim = prelim_order.sort_values(ascending=False).index
    target_tickers = list(dict.fromkeys(list(sorted_prelim[:20]) + list(sorted_prelim[-20:])))

    if should_run_sentiment(sentiment_map, engine):
        def _invalidate_reco_cache():
            state._reco_cache["ts"] = 0.0
        run_sentiment_pass(target_tickers, engine, on_complete=_invalidate_reco_cache)

    sent_series = pd.Series(
        {tk: v["score"] for tk, v in sentiment_map.items()}, dtype=float
    ).reindex(state.df.index, fill_value=50.0)
    scores["sentiment"] = sent_series

    # Composite
    W = RECO_SIGNAL_WEIGHTS
    scores["score"] = sum(scores[k].fillna(50) * w for k, w in W.items())
    scores["score"] = scores["score"].round(1)

    def _label(s):
        return next(lbl for thr, lbl in RECO_LABEL_THRESHOLDS if s >= thr)

    keep = ["longName", "sector", "currentPrice", "marketCap",
            "returnOnEquity", "grossMargins", "trailingPE",
            "revenueGrowth", "recommendationKey"]
    sub = state.df[[c for c in keep if c in state.df.columns]].copy()

    result = []
    _today_str = _date.today().isoformat()
    for rank, ticker in enumerate(scores["score"].sort_values(ascending=False).index, 1):
        row  = sub.loc[ticker] if ticker in sub.index else pd.Series(dtype=object)
        sc   = scores.loc[ticker]
        pc   = mom_rows.get(ticker, {})
        sent = sentiment_map.get(ticker, {})
        _live = _intraday_latest.get(ticker)
        _hist_rows = state.hist_by_ticker.get(ticker) or []
        _prev_close = next((r["close"] for r in reversed(_hist_rows)
                            if r["time"] < _today_str and r["close"] > 0), None)
        _d1 = round((_live - _prev_close) / _prev_close * 100, 2) if _live and _prev_close else None
        result.append({
            "rank":        rank,
            "ticker":      ticker,
            "longName":    str(row.get("longName", "")),
            "sector":      str(row.get("sector", "")),
            "score":       float(sc["score"]),
            "label":       _label(float(sc["score"])),
            "momentum":    round(float(sc["momentum"]), 1),
            "fundamental": round(float(sc["fundamental"]), 1),
            "valuation":   round(float(sc["valuation"]), 1),
            "guru":        round(float(sc["guru"]), 1),
            "analyst":     round(float(sc["analyst"]), 1),
            "sentiment":   round(float(sc["sentiment"]), 1),
            "sentiment_label": sent.get("classification", ""),
            "sentiment_posts": sent.get("post_count", 0),
            "guru_count":  int(guru_counts.get(ticker, 0)),
            "currentPrice": _live if _live else clean(row.get("currentPrice")),
            "marketCap":   clean(row.get("marketCap")),
            "trailingPE":  clean(row.get("trailingPE")),
            "returnOnEquity": clean(row.get("returnOnEquity")),
            "revenueGrowth":  clean(row.get("revenueGrowth")),
            "recommendationKey": str(row.get("recommendationKey", "")),
            "m1": pc.get("m1"), "m3": pc.get("m3"),
            "m6": pc.get("m6"), "y1": pc.get("y1"), "d1": _d1,
        })
    return result


def _get_recommendations() -> list:
    if time.time() - state._reco_cache["ts"] > _RECO_TTL or not state._reco_cache["data"]:
        print("[reco] Computing recommendation scores…")
        state._reco_cache["data"] = _compute_recommendations()
        state._reco_cache["ts"]   = time.time()
        print(f"[reco] Done — {len(state._reco_cache['data'])} stocks scored")
    return state._reco_cache["data"]

# ── Routes ─────────────────────────────────────────────────────────────────────
@bp.get("/recommendations")
def recommendations_page():
    return render_template("recommendations.html")

@bp.get("/api/recommendations")
@limiter.limit("30 per minute")
def recommendations_api():
    return jsonify(_get_recommendations())

@bp.get("/api/sentiment/status")
def sentiment_status_api():
    return jsonify(sentiment_status(get_engine()))

@bp.post("/api/sentiment/run")
@limiter.limit("5 per minute")
@require_auth
def sentiment_run_api():
    from sentiment_engine import _sentiment_running
    if _sentiment_running:
        return jsonify({"ok": False, "message": "Sentiment pass already running."})
    cached = _get_recommendations()
    if not cached:
        return jsonify({"ok": False, "message": "No recommendation data yet — load recommendations first."})
    top20  = [d["ticker"] for d in cached[:20]]
    bot20  = [d["ticker"] for d in cached[-20:]]
    target = list(dict.fromkeys(top20 + bot20))
    def _invalidate():
        state._reco_cache["ts"] = 0.0
    run_sentiment_pass(target, get_engine(), on_complete=_invalidate)
    return jsonify({"ok": True, "message": f"Sentiment pass started for {len(target)} tickers.",
                    "tickers": target})

@bp.get("/api/sentiment/test/<ticker>")
@limiter.limit("10 per minute")
def sentiment_test_api(ticker):
    import re, requests as _req
    ticker = ticker.upper()
    if not re.compile(r'^[A-Z0-9.]{1,10}$').match(ticker):
        return jsonify({"error": "Invalid ticker"}), 400
    results = {}
    st_url = f"https://api.stocktwits.com/api/2/streams/symbol/{ticker}.json"
    for label, verify in [("stocktwits_no_verify", False), ("stocktwits_verify", True)]:
        try:
            r = _req.get(st_url, params={"limit": 5}, timeout=10, verify=verify,
                         headers={"User-Agent": "Mozilla/5.0"})
            results[label] = {"status": r.status_code, "body_len": len(r.text),
                               "body_preview": r.text[:300]}
        except Exception as e:
            results[label] = {"error": str(e), "type": type(e).__name__}
    yf_url = f"https://finance.yahoo.com/rss/headline?s={ticker}"
    try:
        r = _req.get(yf_url, timeout=10, verify=False, headers={"User-Agent": "Mozilla/5.0"})
        results["yahoo_rss"] = {"status": r.status_code, "body_len": len(r.text),
                                 "body_preview": r.text[:200]}
    except Exception as e:
        results["yahoo_rss"] = {"error": str(e)}
    return jsonify(results)

@bp.delete("/api/recommendations")
@require_auth
def recommendations_bust():
    state._reco_cache["ts"] = 0.0
    return jsonify({"ok": True})
