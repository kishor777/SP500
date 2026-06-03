"""Backtest the recommendation engine against 2024-2025 S&P 500 price history."""
import math
import threading
import itertools
from bisect import bisect_left, bisect_right
from datetime import date, timedelta

import numpy as np
from flask import Blueprint, jsonify, render_template, request
from sqlalchemy import text as _t

import state
from extensions import require_auth
from db_config import get_engine
from helpers import get_setting
from config import (
    RECO_SIGNAL_WEIGHTS, RECO_LABEL_THRESHOLDS,
    RECO_MOMENTUM_WEIGHTS, RECO_FUNDAMENTAL_FIELDS,
    RECO_VALUATION_FIELDS, RECO_GURU_TAG_WEIGHT, RECO_ANALYST_SCORE_MAP,
)

bp = Blueprint("backtest", __name__)

_bt_lock = threading.Lock()
_bt_thread = None
_bt_run_id = None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _price_on_or_before(hist_rows, times, target_str):
    idx = bisect_right(times, target_str) - 1
    if idx < 0:
        return None
    v = hist_rows[idx]["close"]
    return float(v) if v and v > 0 else None


def _price_on_or_after(hist_rows, times, target_str):
    idx = bisect_left(times, target_str)
    if idx >= len(hist_rows):
        return None
    v = hist_rows[idx]["close"]
    return float(v) if v and v > 0 else None


def _get_snap_dates():
    """First trading day of each month, Jan 2024 – Dec 2025."""
    snap = []
    for year in [2024, 2025]:
        for month in range(1, 13):
            snap.append(date(year, month, 1).isoformat())
    return snap


def _pct_rank(values: dict) -> dict:
    """Return {key: 0–100 percentile rank} from a {key: numeric} dict."""
    if not values:
        return {}
    items = sorted(values.items(), key=lambda x: x[1])
    n = len(items)
    return {k: round(i / (n - 1) * 100, 1) if n > 1 else 50.0
            for i, (k, _) in enumerate(items)}


def _compute_label(score, thresholds):
    return next(lbl for thr, lbl in thresholds if score >= thr)


# ── Core computation (runs in background thread) ───────────────────────────────

def _run_backtest(run_id: int):
    global _bt_run_id
    engine = get_engine()

    def _upd(status, done, total, err=None):
        with engine.connect() as c:
            c.execute(_t("""UPDATE backtest_runs
                SET status=:s, snap_done=:d, snap_total=:t, error_msg=:e,
                    done_at=IF(:s!='running', NOW(), NULL)
                WHERE id=:id"""),
                {"s": status, "d": done, "t": total, "e": err, "id": run_id})
            c.commit()

    try:
        snap_dates = _get_snap_dates()
        _upd("running", 0, len(snap_dates))

        tickers = [str(t) for t in state.df.index
                   if state.hist_by_ticker.get(str(t))]

        # ── Check if point-in-time fundamentals are available ───────────────────
        with engine.connect() as conn:
            fund_count = conn.execute(_t(
                "SELECT COUNT(DISTINCT ticker) FROM sp500_fundamentals"
            )).scalar() or 0
            analyst_count = conn.execute(_t(
                "SELECT COUNT(DISTINCT ticker) FROM sp500_analyst_recs"
            )).scalar() or 0

        has_fundamentals = fund_count > 100
        has_analyst_hist = analyst_count > 100
        print(f"[backtest] Fundamentals: {fund_count} tickers, "
              f"Analyst history: {analyst_count} tickers")

        # ── Pre-load all fundamentals and analyst recs into memory ────────────
        all_fund_data: dict = {}   # {ticker: [(period_date, row_dict), ...] sorted asc}
        all_analyst_data: dict = {}  # {ticker: [(rec_date, score), ...] sorted asc}

        if has_fundamentals:
            with engine.connect() as conn:
                fund_rows_db = conn.execute(_t("""
                    SELECT ticker, period_date, gross_margin, operating_margin,
                           net_margin, revenue_growth, earnings_growth, roe,
                           net_income, book_value, total_revenue
                    FROM sp500_fundamentals ORDER BY ticker, period_date
                """)).fetchall()
            for r in fund_rows_db:
                all_fund_data.setdefault(r.ticker, []).append((str(r.period_date), r))

        if has_analyst_hist:
            with engine.connect() as conn:
                rec_rows_db = conn.execute(_t("""
                    SELECT ticker, rec_date, rec_to FROM sp500_analyst_recs
                    ORDER BY ticker, rec_date
                """)).fetchall()
            score_map = {"strong buy": 100, "buy": 80, "overweight": 80,
                         "outperform": 75, "hold": 50, "neutral": 50,
                         "equal-weight": 50, "underperform": 25, "underweight": 25,
                         "sell": 5}
            for r in rec_rows_db:
                sc = score_map.get((r.rec_to or "").lower().strip(), 50)
                all_analyst_data.setdefault(r.ticker, []).append((str(r.rec_date), sc))

        def _fund_at_date(ticker, snap_date_str):
            """Get the most recent fundamentals row for ticker at or before snap_date."""
            entries = all_fund_data.get(ticker)
            if not entries:
                return None
            # Binary search for latest entry <= snap_date_str
            lo, hi = 0, len(entries) - 1
            result = None
            while lo <= hi:
                mid = (lo + hi) // 2
                if entries[mid][0] <= snap_date_str:
                    result = entries[mid][1]
                    lo = mid + 1
                else:
                    hi = mid - 1
            return result

        def _analyst_at_date(ticker, snap_date_str):
            """Get consensus analyst score for ticker at snap_date."""
            entries = all_analyst_data.get(ticker)
            if not entries:
                return None
            # Find all entries <= snap_date (most recent per firm is impractical
            # without firm tracking; use the latest single entry as proxy)
            result = None
            for d, sc in entries:
                if d <= snap_date_str:
                    result = sc
                else:
                    break
            return result

        # ── Proxy fundamental / valuation / analyst scores (fallback only) ────
        def _col_rank_proxy(col_name):
            vals = {}
            for tk in tickers:
                if tk not in state.df.index:
                    continue
                try:
                    v = state.df.at[tk, col_name]
                    if v is not None and not (isinstance(v, float) and math.isnan(v)):
                        vals[tk] = float(v)
                except Exception:
                    pass
            if not vals:
                return {tk: 50.0 for tk in tickers}
            ranked = _pct_rank(vals)
            return {tk: ranked.get(tk, 50.0) for tk in tickers}

        qual_raw_proxy = {}
        for tk in tickers:
            s = 0.0
            tot = 0.0
            for col, w in RECO_FUNDAMENTAL_FIELDS.items():
                try:
                    v = state.df.at[tk, col] if tk in state.df.index else None
                    if v is not None and not (isinstance(v, float) and math.isnan(v)):
                        s += float(v) * w; tot += w
                except Exception:
                    pass
            if tot > 0:
                qual_raw_proxy[tk] = s / tot
        f_scores_proxy = {tk: _pct_rank(qual_raw_proxy).get(tk, 50.0) for tk in tickers}

        val_raw_proxy = {}
        for tk in tickers:
            s = 0.0; tot = 0.0
            for col, w in RECO_VALUATION_FIELDS.items():
                try:
                    v = state.df.at[tk, col] if tk in state.df.index else None
                    if v is not None and not (isinstance(v, float) and math.isnan(v)) and float(v) > 0:
                        s += float(v) * w; tot += w
                except Exception:
                    pass
            if tot > 0:
                val_raw_proxy[tk] = s / tot
        tmp = _pct_rank(val_raw_proxy)
        v_scores_proxy = {tk: 100 - tmp.get(tk, 50.0) for tk in tickers}  # invert

        a_scores_proxy = {}
        for tk in tickers:
            try:
                key = str(state.df.at[tk, "recommendationKey"] if tk in state.df.index else "")
                a_scores_proxy[tk] = float(RECO_ANALYST_SCORE_MAP.get(key.lower(), 50))
            except Exception:
                a_scores_proxy[tk] = 50.0

        # ── Pre-load guru holdings ─────────────────────────────────────────────
        with engine.connect() as conn:
            g_rows = conn.execute(_t("""
                SELECT gh.ticker, gh.slug, gh.change_tag, gm.filing_date
                FROM guru_holdings gh
                JOIN guru_filing_meta gm ON gh.slug=gm.slug AND gh.filing_date=gm.filing_date
                WHERE gh.ticker IS NOT NULL
                ORDER BY gm.filing_date
            """)).fetchall()
        guru_data = [(r.ticker, r.slug, r.change_tag, str(r.filing_date)) for r in g_rows]

        # ── Pre-load sentiment scores ──────────────────────────────────────────
        with engine.connect() as conn:
            sent_rows = conn.execute(_t(
                "SELECT ticker, score_date, score FROM sentiment_scores ORDER BY score_date"
            )).fetchall()
        # {ticker: [(date_str, score)]}
        sent_by_ticker: dict = {}
        for r in sent_rows:
            sent_by_ticker.setdefault(r.ticker, []).append((str(r.score_date), float(r.score)))

        # Thresholds (respects admin overrides)
        thresholds = [
            (get_setting("reco_thr_strong_buy", 68), "Strong Buy"),
            (get_setting("reco_thr_buy",        57), "Buy"),
            (get_setting("reco_thr_hold",       44), "Hold"),
            (get_setting("reco_thr_under",      32), "Underperform"),
            (0,                                       "Sell"),
        ]
        W_curr = {
            "momentum":    get_setting("reco_w_momentum",    RECO_SIGNAL_WEIGHTS["momentum"]),
            "fundamental": get_setting("reco_w_fundamental", RECO_SIGNAL_WEIGHTS["fundamental"]),
            "valuation":   get_setting("reco_w_valuation",   RECO_SIGNAL_WEIGHTS["valuation"]),
            "guru":        get_setting("reco_w_guru",        RECO_SIGNAL_WEIGHTS["guru"]),
            "analyst":     get_setting("reco_w_analyst",     RECO_SIGNAL_WEIGHTS["analyst"]),
            "sentiment":   get_setting("reco_w_sentiment",   RECO_SIGNAL_WEIGHTS["sentiment"]),
        }
        w_sum = sum(W_curr.values()) or 1
        W_curr = {k: v / w_sum for k, v in W_curr.items()}

        all_rows = []

        for snap_i, snap_date_str in enumerate(snap_dates):
            snap_d = date.fromisoformat(snap_date_str)

            # ── Momentum scores at snap_date ─────────────────────────────────
            mom_raw = {}
            for tk in tickers:
                hist = state.hist_by_ticker.get(tk) or []
                if len(hist) < 10:
                    continue
                times = [r["time"] for r in hist]
                price_d = _price_on_or_before(hist, times, snap_date_str)
                if not price_d:
                    continue
                blend = 0.0
                w_total = 0.0
                for key, wm, days in [("m3", 0.30, 91), ("m6", 0.40, 182), ("y1", 0.30, 365)]:
                    past_str = (snap_d - timedelta(days=days)).isoformat()
                    price_p = _price_on_or_before(hist, times, past_str)
                    if price_p and price_p > 0:
                        blend += ((price_d - price_p) / price_p) * wm
                        w_total += wm
                if w_total >= 0.29:  # at least m3 present
                    mom_raw[tk] = blend / w_total
            m_scores = {tk: _pct_rank(mom_raw).get(tk, 50.0) for tk in tickers}

            # ── Guru scores at snap_date ──────────────────────────────────────
            # Use most recent filing per slug where filing_date <= snap_date
            latest_per_slug: dict = {}  # {slug: {ticker: change_tag}}
            for tk2, slug, tag, fd in guru_data:
                if fd > snap_date_str:
                    break  # sorted ascending, past threshold
                latest_per_slug.setdefault(slug, {})[tk2] = tag
            guru_raw: dict = {}
            for slug_holdings in latest_per_slug.values():
                for tk2, tag in slug_holdings.items():
                    guru_raw[tk2] = guru_raw.get(tk2, 0) + RECO_GURU_TAG_WEIGHT.get(tag or "", 1.0)
            max_guru = max(guru_raw.values(), default=1)
            g_scores = {tk: round(guru_raw.get(tk, 0) / max_guru * 100, 1) if max_guru else 50.0
                        for tk in tickers}

            # ── Sentiment at snap_date ────────────────────────────────────────
            s_scores: dict = {}
            for tk in tickers:
                entries = sent_by_ticker.get(tk) or []
                # Find entry on or before snap_date
                if entries:
                    e_times = [e[0] for e in entries]
                    idx = bisect_right(e_times, snap_date_str) - 1
                    s_scores[tk] = entries[idx][1] if idx >= 0 else 50.0
                else:
                    s_scores[tk] = 50.0

            # ── Forward returns ───────────────────────────────────────────────
            fwd: dict = {}
            for tk in tickers:
                hist = state.hist_by_ticker.get(tk) or []
                if not hist:
                    continue
                times = [r["time"] for r in hist]
                price_d = _price_on_or_before(hist, times, snap_date_str)
                if not price_d:
                    continue
                row_ret = {}
                for horizon, days in [("ret_30", 30), ("ret_90", 90), ("ret_365", 365)]:
                    tgt = (snap_d + timedelta(days=days)).isoformat()
                    pf = _price_on_or_after(hist, times, tgt)
                    if pf:
                        row_ret[horizon] = round((pf - price_d) / price_d, 6)
                if row_ret:
                    fwd[tk] = row_ret

            # ── Point-in-time Quality & Valuation from quarterly fundamentals ──
            if has_fundamentals:
                fund_pit: dict = {}   # {tk: row}
                for tk in tickers:
                    r = _fund_at_date(tk, snap_date_str)
                    if r:
                        fund_pit[tk] = r

                # Build quality composite score at snap_date
                q_raw_snap: dict = {}
                for tk, r in fund_pit.items():
                    vals_q = [
                        (getattr(r, 'gross_margin',    None), 0.20),
                        (getattr(r, 'operating_margin', None), 0.20),
                        (getattr(r, 'net_margin',      None), 0.30),
                        (getattr(r, 'revenue_growth',  None), 0.15),
                        (getattr(r, 'earnings_growth', None), 0.15),
                    ]
                    s, tot = 0.0, 0.0
                    for v, w in vals_q:
                        if v is not None and not (isinstance(v, float) and math.isnan(v)):
                            s += float(v) * w; tot += w
                    if tot > 0:
                        q_raw_snap[tk] = s / tot
                f_scores_snap = {tk: _pct_rank(q_raw_snap).get(tk, f_scores_proxy.get(tk, 50.0))
                                 for tk in tickers}

                # Historical P/E for valuation (price_at_D / annualised_EPS_at_D)
                pe_raw_snap: dict = {}
                for tk in tickers:
                    r = _fund_at_date(tk, snap_date_str)
                    if not r:
                        continue
                    ni  = getattr(r, 'net_income', None)
                    rev = getattr(r, 'total_revenue', None)
                    bv  = getattr(r, 'book_value', None)
                    hist = state.hist_by_ticker.get(tk) or []
                    if not hist:
                        continue
                    times_h = [h["time"] for h in hist]
                    price_d = _price_on_or_before(hist, times_h, snap_date_str)
                    if price_d and ni and ni > 0:
                        # shares_outstanding from sp500_info (proxy — current value)
                        try:
                            sh = float(state.df.at[tk, "sharesOutstanding"]) if tk in state.df.index else None
                            if sh and sh > 0:
                                eps_annualised = (ni * 4) / sh
                                pe = price_d / eps_annualised if eps_annualised > 0 else None
                                if pe:
                                    pe_raw_snap[tk] = pe
                        except Exception:
                            pass

                if pe_raw_snap:
                    tmp_v = _pct_rank(pe_raw_snap)
                    v_scores_snap = {tk: 100 - tmp_v.get(tk, 50.0) for tk in tickers}  # invert
                else:
                    v_scores_snap = v_scores_proxy
            else:
                f_scores_snap = f_scores_proxy
                v_scores_snap = v_scores_proxy

            # ── Point-in-time analyst consensus ──────────────────────────────
            if has_analyst_hist:
                a_raw_snap: dict = {}
                for tk in tickers:
                    sc = _analyst_at_date(tk, snap_date_str)
                    if sc is not None:
                        a_raw_snap[tk] = sc
                a_scores_snap = {tk: float(a_raw_snap.get(tk, a_scores_proxy.get(tk, 50.0)))
                                 for tk in tickers}
            else:
                a_scores_snap = a_scores_proxy

            # ── Assemble rows ─────────────────────────────────────────────────
            batch = []
            for tk in tickers:
                if tk not in fwd:
                    continue
                ms  = m_scores.get(tk, 50.0)
                fs  = f_scores_snap.get(tk, 50.0)
                vs  = v_scores_snap.get(tk, 50.0)
                gs  = g_scores.get(tk, 50.0)
                a_s = a_scores_snap.get(tk, 50.0)
                ss  = s_scores.get(tk, 50.0)
                score = (ms * W_curr["momentum"]    +
                         fs * W_curr["fundamental"] +
                         vs * W_curr["valuation"]   +
                         gs * W_curr["guru"]        +
                         a_s * W_curr["analyst"]    +
                         ss * W_curr["sentiment"])
                label = _compute_label(round(score, 1), thresholds)
                r = fwd[tk]
                batch.append({
                    "run_id": run_id, "snap_date": snap_date_str, "ticker": tk,
                    "score": round(score, 2), "label": label,
                    "m": ms, "f": fs, "v": vs, "g": gs, "a": a_s, "s": ss,
                    "ret_30":  r.get("ret_30"),
                    "ret_90":  r.get("ret_90"),
                    "ret_365": r.get("ret_365"),
                })
                all_rows.append(batch[-1])

            # Save this snapshot's rows
            if batch:
                with engine.connect() as conn:
                    conn.execute(_t("""
                        INSERT INTO backtest_scores
                        (run_id,snap_date,ticker,score,label,
                         m_score,f_score,v_score,g_score,a_score,s_score,
                         ret_30,ret_90,ret_365)
                        VALUES (:rid,:sd,:tk,:sc,:lb,:m,:f,:v,:g,:a,:s,:r30,:r90,:r365)
                    """), [{"rid": b["run_id"], "sd": b["snap_date"], "tk": b["ticker"],
                            "sc": b["score"],  "lb": b["label"],
                            "m": b["m"], "f": b["f"], "v": b["v"],
                            "g": b["g"], "a": b["a"], "s": b["s"],
                            "r30": b["ret_30"], "r90": b["ret_90"], "r365": b["ret_365"]}
                           for b in batch])
                    conn.commit()

            _upd("running", snap_i + 1, len(snap_dates))

        # ── Weight grid search ────────────────────────────────────────────────
        _upd("running", len(snap_dates), len(snap_dates))  # keep at 100% while grid runs

        valid_rows = [r for r in all_rows if r["ret_90"] is not None]
        if len(valid_rows) > 50:
            X = np.array([[r["m"], r["f"], r["v"], r["g"], r["a"], r["s"]]
                          for r in valid_rows], dtype=float)
            y30  = np.array([r["ret_30"] if r["ret_30"] is not None else np.nan for r in valid_rows])
            y90  = np.array([r["ret_90"] for r in valid_rows])
            y365 = np.array([r["ret_365"] if r["ret_365"] is not None else np.nan for r in valid_rows])

            # Generate grid: 5 weight levels × 6 signals (unnormalized → normalize)
            levels = [0, 1, 2, 3, 4]
            raw_grid = np.array([c for c in itertools.product(levels, repeat=6)
                                 if sum(c) > 0], dtype=float)
            W_grid = raw_grid / raw_grid.sum(axis=1, keepdims=True)   # shape (n, 6)

            # Composites: (n_grid, n_samples)
            composites = W_grid @ X.T   # fast matrix mul

            def _ic(composites_mat, y):
                valid = ~np.isnan(y)
                comp = composites_mat[:, valid]
                yv   = y[valid]
                if len(yv) < 10:
                    return np.zeros(len(composites_mat))
                comp_c = comp - comp.mean(axis=1, keepdims=True)
                yv_c   = yv  - yv.mean()
                num    = comp_c @ yv_c
                denom  = np.sqrt((comp_c**2).sum(axis=1)) * (np.sqrt((yv_c**2).sum()) or 1)
                return np.where(denom > 0, num / denom, 0.0)

            ic30  = _ic(composites, y30)
            ic90  = _ic(composites, y90)
            ic365 = _ic(composites, y365)

            # Sharpe for 90d: simulate monthly rebalancing — take top-decile returns
            def _sharpe(composites_mat, y):
                n_samples = composites_mat.shape[1]
                n_snap = len(snap_dates)
                per_snap = n_samples // n_snap if n_snap else n_samples
                sharpes = []
                for i in range(len(composites_mat)):
                    monthly_rets = []
                    for s in range(n_snap):
                        sl = slice(s * per_snap, (s+1) * per_snap)
                        comp_s = composites_mat[i, sl]
                        y_s    = y[sl]
                        valid  = ~np.isnan(y_s)
                        if valid.sum() < 5:
                            continue
                        top_mask = comp_s >= np.percentile(comp_s[valid], 80)
                        top_rets = y_s[top_mask & valid]
                        if len(top_rets):
                            monthly_rets.append(top_rets.mean())
                    if len(monthly_rets) >= 3:
                        arr = np.array(monthly_rets)
                        sharpes.append(arr.mean() / (arr.std() + 1e-9) * math.sqrt(12))
                    else:
                        sharpes.append(0.0)
                return np.array(sharpes)

            sharpe90 = _sharpe(composites, y90)

            # Sort by ic90, take top 20
            top_idx = np.argsort(ic90)[::-1][:20]
            weight_rows = []
            for idx in top_idx:
                w = W_grid[idx]
                weight_rows.append({
                    "run_id": run_id,
                    "w_momentum": round(float(w[0]), 4),
                    "w_fundamental": round(float(w[1]), 4),
                    "w_valuation": round(float(w[2]), 4),
                    "w_guru": round(float(w[3]), 4),
                    "w_analyst": round(float(w[4]), 4),
                    "w_sentiment": round(float(w[5]), 4),
                    "ic_30":  round(float(ic30[idx]),  4),
                    "ic_90":  round(float(ic90[idx]),  4),
                    "sharpe_90": round(float(sharpe90[idx]), 3),
                })
            if weight_rows:
                with engine.connect() as conn:
                    conn.execute(_t("""
                        INSERT INTO backtest_weights
                        (run_id,w_momentum,w_fundamental,w_valuation,w_guru,w_analyst,w_sentiment,
                         ic_30,ic_90,sharpe_90)
                        VALUES (:run_id,:w_momentum,:w_fundamental,:w_valuation,:w_guru,:w_analyst,
                                :w_sentiment,:ic_30,:ic_90,:sharpe_90)
                    """), weight_rows)
                    conn.commit()

        _upd("done", len(snap_dates), len(snap_dates))

    except Exception as e:
        import traceback
        err = traceback.format_exc()[-400:]
        print(f"[backtest] ERROR: {e}\n{err}")
        with engine.connect() as conn:
            conn.execute(_t("UPDATE backtest_runs SET status='error', error_msg=:e WHERE id=:id"),
                         {"e": str(e)[:400], "id": run_id})
            conn.commit()
    finally:
        _bt_run_id = None


# ── Routes ────────────────────────────────────────────────────────────────────

@bp.get("/api/backtest/data-status")
@require_auth
def backtest_data_status():
    engine = get_engine()
    with engine.connect() as conn:
        fund_count = conn.execute(_t(
            "SELECT COUNT(DISTINCT ticker) FROM sp500_fundamentals"
        )).scalar() or 0
        analyst_count = conn.execute(_t(
            "SELECT COUNT(DISTINCT ticker) FROM sp500_analyst_recs"
        )).scalar() or 0
        latest_fund = conn.execute(_t(
            "SELECT MAX(period_date) FROM sp500_fundamentals"
        )).scalar()
    return jsonify({
        "fund_tickers":   int(fund_count),
        "analyst_tickers": int(analyst_count),
        "latest_period":  str(latest_fund) if latest_fund else None,
        "backfill_state": state._fund_backfill_status.get("state", "idle"),
        "backfill_msg":   state._fund_backfill_status.get("msg", ""),
        "backfill_pct":   round(state._fund_backfill_status.get("done", 0) /
                                max(state._fund_backfill_status.get("total", 1), 1) * 100),
    })


@bp.get("/backtest")
@require_auth
def backtest_page():
    return render_template("backtest.html")


@bp.post("/api/backtest/refetch-analyst")
@require_auth
def backtest_refetch_analyst():
    """Re-fetch analyst upgrade/downgrade history for all tickers (background)."""
    import threading
    from workers import _fetch_ticker_fundamentals
    import state

    def _run():
        from sqlalchemy import text as _t2
        from db_config import get_engine as _ge
        eng = _ge()
        tickers = [str(t) for t in state.df.index]
        state._fund_backfill_status.update({"state": "running", "done": 0,
                                            "total": len(tickers), "msg": "analyst refetch"})
        INSERT = _t2("""
            INSERT INTO sp500_analyst_recs (ticker, rec_date, firm, rec_to, rec_from, action)
            VALUES (:ticker, :rec_date, :firm, :rec_to, :rec_from, :action)
        """)
        for i, ticker in enumerate(tickers, 1):
            try:
                _, analyst_rows = _fetch_ticker_fundamentals(ticker, years=2)
                if analyst_rows:
                    with eng.connect() as conn:
                        conn.execute(INSERT, analyst_rows)
                        conn.commit()
            except Exception as e:
                print(f"[analyst-refetch] {ticker}: {e}")
            state._fund_backfill_status.update({"done": i, "msg": f"analyst {i}/{len(tickers)}"})
            import time; time.sleep(0.5)
        state._fund_backfill_status.update({"state": "done", "msg": "analyst refetch complete"})
        print("[analyst-refetch] Done")

    t = threading.Thread(target=_run, daemon=True, name="analyst-refetch")
    t.start()
    return jsonify({"ok": True})


@bp.post("/api/backtest/run")
@require_auth
def backtest_run():
    global _bt_thread, _bt_run_id
    with _bt_lock:
        if _bt_thread and _bt_thread.is_alive():
            return jsonify({"ok": False, "error": "A backtest is already running"}), 409

        engine = get_engine()
        with engine.connect() as conn:
            res = conn.execute(_t(
                "INSERT INTO backtest_runs (status) VALUES ('running')"
            ))
            run_id = res.lastrowid
            conn.commit()

        _bt_run_id = run_id
        _bt_thread = threading.Thread(target=_run_backtest, args=(run_id,), daemon=True, name="backtest")
        _bt_thread.start()

    return jsonify({"ok": True, "run_id": run_id})


@bp.get("/api/backtest/status")
@require_auth
def backtest_status():
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(_t("""
            SELECT id, status, snap_done, snap_total, error_msg, started_at, done_at
            FROM backtest_runs ORDER BY id DESC LIMIT 1
        """)).fetchone()
    if not row:
        return jsonify({"has_run": False})
    running = _bt_thread is not None and _bt_thread.is_alive()
    return jsonify({
        "has_run":    True,
        "run_id":     row.id,
        "status":     row.status,
        "running":    running,
        "snap_done":  row.snap_done,
        "snap_total": row.snap_total,
        "error_msg":  row.error_msg,
        "started_at": str(row.started_at),
        "done_at":    str(row.done_at) if row.done_at else None,
    })


@bp.get("/api/backtest/results/<int:run_id>")
@require_auth
def backtest_results(run_id):
    engine = get_engine()
    with engine.connect() as conn:
        # Label performance
        label_rows = conn.execute(_t("""
            SELECT label,
                COUNT(*)                                   AS cnt,
                AVG(ret_30)                                AS avg_30,
                AVG(ret_90)                                AS avg_90,
                AVG(ret_365)                               AS avg_365,
                SUM(ret_30  > 0) / COUNT(ret_30)          AS win_30,
                SUM(ret_90  > 0) / COUNT(ret_90)          AS win_90
            FROM backtest_scores WHERE run_id=:rid
            GROUP BY label
        """), {"rid": run_id}).fetchall()

        # Score decile performance (10 buckets by score)
        decile_rows = conn.execute(_t("""
            SELECT
                FLOOR(score / 10) * 10  AS bucket,
                COUNT(*)                AS cnt,
                AVG(ret_30)             AS avg_30,
                AVG(ret_90)             AS avg_90
            FROM backtest_scores WHERE run_id=:rid AND ret_30 IS NOT NULL
            GROUP BY bucket ORDER BY bucket DESC
        """), {"rid": run_id}).fetchall()

        # Signal IC (correlation of each signal with 90d return, independently)
        ic_rows = conn.execute(_t("""
            SELECT
              (COUNT(*) * SUM(m_score*ret_90) - SUM(m_score)*SUM(ret_90)) /
                NULLIF(SQRT((COUNT(*)*SUM(m_score*m_score)-SUM(m_score)*SUM(m_score))
                      *(COUNT(*)*SUM(ret_90*ret_90)-SUM(ret_90)*SUM(ret_90))),0) AS ic_mom,
              (COUNT(*) * SUM(f_score*ret_90) - SUM(f_score)*SUM(ret_90)) /
                NULLIF(SQRT((COUNT(*)*SUM(f_score*f_score)-SUM(f_score)*SUM(f_score))
                      *(COUNT(*)*SUM(ret_90*ret_90)-SUM(ret_90)*SUM(ret_90))),0) AS ic_fund,
              (COUNT(*) * SUM(v_score*ret_90) - SUM(v_score)*SUM(ret_90)) /
                NULLIF(SQRT((COUNT(*)*SUM(v_score*v_score)-SUM(v_score)*SUM(v_score))
                      *(COUNT(*)*SUM(ret_90*ret_90)-SUM(ret_90)*SUM(ret_90))),0) AS ic_val,
              (COUNT(*) * SUM(g_score*ret_90) - SUM(g_score)*SUM(ret_90)) /
                NULLIF(SQRT((COUNT(*)*SUM(g_score*g_score)-SUM(g_score)*SUM(g_score))
                      *(COUNT(*)*SUM(ret_90*ret_90)-SUM(ret_90)*SUM(ret_90))),0) AS ic_guru,
              (COUNT(*) * SUM(a_score*ret_90) - SUM(a_score)*SUM(ret_90)) /
                NULLIF(SQRT((COUNT(*)*SUM(a_score*a_score)-SUM(a_score)*SUM(a_score))
                      *(COUNT(*)*SUM(ret_90*ret_90)-SUM(ret_90)*SUM(ret_90))),0) AS ic_analyst,
              (COUNT(*) * SUM(s_score*ret_90) - SUM(s_score)*SUM(ret_90)) /
                NULLIF(SQRT((COUNT(*)*SUM(s_score*s_score)-SUM(s_score)*SUM(s_score))
                      *(COUNT(*)*SUM(ret_90*ret_90)-SUM(ret_90)*SUM(ret_90))),0) AS ic_sent
            FROM backtest_scores WHERE run_id=:rid AND ret_90 IS NOT NULL
        """), {"rid": run_id}).fetchone()

        # Top weight combinations
        weight_rows = conn.execute(_t("""
            SELECT w_momentum,w_fundamental,w_valuation,w_guru,w_analyst,w_sentiment,
                   ic_30,ic_90,sharpe_90
            FROM backtest_weights WHERE run_id=:rid
            ORDER BY ic_90 DESC LIMIT 15
        """), {"rid": run_id}).fetchall()

    def _r(v):
        return round(float(v), 4) if v is not None else None

    label_order = ["Strong Buy", "Buy", "Hold", "Underperform", "Sell"]
    labels_data = {r.label: {
        "cnt":    int(r.cnt),
        "avg_30": _r(r.avg_30),
        "avg_90": _r(r.avg_90),
        "avg_365":_r(r.avg_365),
        "win_30": _r(r.win_30),
        "win_90": _r(r.win_90),
    } for r in label_rows}

    return jsonify({
        "labels": [{"label": lb, **labels_data.get(lb, {})} for lb in label_order if lb in labels_data],
        "deciles": [{"bucket": int(r.bucket),
                     "cnt": int(r.cnt),
                     "avg_30": _r(r.avg_30),
                     "avg_90": _r(r.avg_90)} for r in decile_rows],
        "signal_ic": {
            "Momentum":    _r(ic_rows.ic_mom)     if ic_rows else None,
            "Quality":     _r(ic_rows.ic_fund)    if ic_rows else None,
            "Valuation":   _r(ic_rows.ic_val)     if ic_rows else None,
            "Guru":        _r(ic_rows.ic_guru)    if ic_rows else None,
            "Analyst":     _r(ic_rows.ic_analyst) if ic_rows else None,
            "Sentiment":   _r(ic_rows.ic_sent)    if ic_rows else None,
        },
        "top_weights": [{
            "w_momentum":    round(float(r.w_momentum),    3),
            "w_fundamental": round(float(r.w_fundamental), 3),
            "w_valuation":   round(float(r.w_valuation),  3),
            "w_guru":        round(float(r.w_guru),        3),
            "w_analyst":     round(float(r.w_analyst),     3),
            "w_sentiment":   round(float(r.w_sentiment),   3),
            "ic_30":    _r(r.ic_30),
            "ic_90":    _r(r.ic_90),
            "sharpe_90":_r(r.sharpe_90),
        } for r in weight_rows],
    })
