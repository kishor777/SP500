import time
import pandas as pd
from flask import Blueprint, jsonify, render_template

import state
from helpers import clean, _price_changes
from edgar import _fetch_13f, _fetch_nport
from extensions import limiter
from db_config import get_engine
from config import (
    GURUS, _OPT_SKIP, _OPT_PCT_FRAC, _OPTIMIZED_TTL, _RECO_TTL,
    RECO_OPT_PERIOD_WEIGHTS,
)

bp = Blueprint("guru", __name__)

# ── Guru rule helpers ──────────────────────────────────────────────────────────
def _apply_guru_rules(slug: str) -> list:
    rules = GURUS[slug]['rules']
    mask = pd.Series(True, index=state.df.index)
    for fid, bounds in rules.get('numeric', {}).items():
        if fid not in state.df.columns:
            continue
        fmt_ = state._s_num_meta.get(fid, {}).get('fmt', 'num')
        div = 100 if fmt_ == 'pct_frac' else 1
        lo, hi = bounds.get('min'), bounds.get('max')
        if lo is not None:
            mask &= state.df[fid] >= lo / div
        if hi is not None:
            mask &= state.df[fid] <= hi / div
    for fid, vals in rules.get('categorical', {}).items():
        if fid not in state.df.columns or not vals:
            continue
        mask &= state.df[fid].isin(vals)
    out_cols = ['longName', 'sector', 'currentPrice', 'marketCap', 'trailingPE',
                'returnOnEquity', 'profitMargins', 'revenueGrowth', 'dividendYield',
                '52WeekChange', 'recommendationKey']
    sub = state.df[mask][[c for c in out_cols if c in state.df.columns]]
    results = [{'ticker': str(t), **{k: clean(v) for k, v in row.items()}}
               for t, row in sub.iterrows()]
    results.sort(key=lambda x: x.get('marketCap') or 0, reverse=True)
    return results


def _build_optimized_guru_rules() -> dict:
    cached = state._optimized_rules_cache
    if cached and (time.time() - cached.get("ts", 0)) < _OPTIMIZED_TTL:
        return cached["rules"]

    WEIGHTS = RECO_OPT_PERIOD_WEIGHTS
    all_changes = {t: _price_changes(t) for t in state.df.index}
    valid_tickers = {t for t, pc in all_changes.items() if pc}

    def _score(tickers):
        if not tickers:
            return -999.0
        pv: dict = {k: [] for k in WEIGHTS}
        for t in tickers:
            for k, v in all_changes.get(t, {}).items():
                if k in pv:
                    pv[k].append(v)
        avail = {k: v for k, v in pv.items() if v}
        if not avail:
            return -999.0
        wt = sum(WEIGHTS[k] for k in avail)
        return sum(WEIGHTS[k] * (sum(v) / len(v)) for k, v in avail.items()) / wt

    baseline = _score(valid_tickers)
    candidate_fields = {fid for fid in state._s_num_meta if fid not in _OPT_SKIP}

    field_best: list = []
    for fid in candidate_fields:
        if fid not in state.df.columns:
            continue
        col = state.df[fid].dropna()
        if len(col) < 200 or not pd.api.types.is_numeric_dtype(col):
            continue
        div = 100 if fid in _OPT_PCT_FRAC else 1
        best_dir, best_val, best_sc = None, None, baseline - 0.5
        for q in (0.20, 0.30, 0.40, 0.50, 0.60, 0.70):
            thresh = float(col.quantile(q))
            passing = {t for t in (set(col[col >= thresh].index) & valid_tickers)}
            if len(passing) >= 20:
                s = _score(passing)
                if s > best_sc:
                    best_sc, best_dir, best_val = s, "min", round(thresh * div, 4)
        for q in (0.80, 0.70, 0.60, 0.50, 0.40, 0.30):
            thresh = float(col.quantile(q))
            passing = {t for t in (set(col[col <= thresh].index) & valid_tickers)}
            if len(passing) >= 20:
                s = _score(passing)
                if s > best_sc:
                    best_sc, best_dir, best_val = s, "max", round(thresh * div, 4)
        if best_dir:
            field_best.append((fid, best_dir, best_val, best_sc, best_sc - baseline))

    field_best.sort(key=lambda x: -x[4])
    current_mask = pd.Series(True, index=state.df.index)
    final_rules: dict = {}
    for fid, direction, val, _sc, _gain in field_best:
        if fid not in state.df.columns or fid in final_rules:
            continue
        col = state.df[fid]
        div = 100 if fid in _OPT_PCT_FRAC else 1
        test_mask = current_mask.copy()
        if direction == "min":
            test_mask &= col.fillna(-1e18) >= val / div
        else:
            test_mask &= col.fillna(1e18) <= val / div
        passing = set(state.df[test_mask].index) & valid_tickers
        if len(passing) < 20:
            continue
        if _score(passing) >= _score(set(state.df[current_mask].index) & valid_tickers):
            current_mask = test_mask
            final_rules[fid] = {direction: val}

    state._optimized_rules_cache = {"rules": final_rules, "ts": time.time()}
    return final_rules


def _build_master_guru_rules() -> dict:
    from collections import defaultdict

    _EXCLUDE = {
        "enterpriseToEbitda", "priceToSalesTrailing12Months",
        "priceToBook", "earningsQuarterlyGrowth",
    }

    def _percentile(sorted_vals, pct):
        if not sorted_vals:
            return None
        idx = pct / 100 * (len(sorted_vals) - 1)
        lo, hi = int(idx), min(int(idx) + 1, len(sorted_vals) - 1)
        return sorted_vals[lo] + (idx - lo) * (sorted_vals[hi] - sorted_vals[lo])

    mins: dict = defaultdict(list)
    maxs: dict = defaultdict(list)
    for g in GURUS.values():
        for fid, bounds in g.get("rules", {}).get("numeric", {}).items():
            if fid in _EXCLUDE:
                continue
            if "min" in bounds:
                mins[fid].append(bounds["min"])
            if "max" in bounds:
                maxs[fid].append(bounds["max"])

    numeric = {}
    for fid, vals in mins.items():
        if len(vals) >= 3:
            numeric.setdefault(fid, {})["min"] = round(_percentile(sorted(vals), 25), 2)
    for fid, vals in maxs.items():
        if len(vals) >= 3:
            numeric.setdefault(fid, {})["max"] = round(_percentile(sorted(vals), 75), 2)

    if "beta" in numeric and "min" in numeric["beta"]:
        del numeric["beta"]["min"]
        if not numeric["beta"]:
            del numeric["beta"]

    return numeric

# ── Routes ─────────────────────────────────────────────────────────────────────
@bp.get("/leaderboard")
def leaderboard_page():
    return render_template("leaderboard.html")

@bp.get("/gurus")
def gurus_page():
    return render_template("gurus.html")

@bp.get("/api/guru/list")
def guru_list():
    return jsonify([
        {"slug": s, "name": g["name"], "fund": g["fund"],
         "style": g["style"], "color": g["color"]}
        for s, g in GURUS.items()
    ])

@bp.get("/api/guru/<slug>/info")
def guru_info_route(slug):
    if slug not in GURUS:
        return jsonify({"error": "Unknown guru"}), 404
    g = GURUS[slug]
    rule_meta = {
        fid: {"label": state._s_num_meta.get(fid, {}).get("label", fid),
              "fmt":   state._s_num_meta.get(fid, {}).get("fmt", "num")}
        for fid in g["rules"].get("numeric", {})
    }
    return jsonify({**g, "rule_meta": rule_meta})

@bp.get("/api/guru/<slug>/screener-rules")
def guru_screener_rules(slug):
    if slug == "master":
        numeric = _build_master_guru_rules()
        return jsonify({"name": "Master Guru", "fund": f"Consensus of {len(GURUS)} gurus",
                        "color": "#f59e0b", "numeric": numeric, "categorical": {}})
    if slug == "optimized":
        numeric = _build_optimized_guru_rules()
        return jsonify({"name": "Optimized Guru", "fund": "Data-driven — maximizes portfolio returns",
                        "color": "#10b981", "numeric": numeric, "categorical": {}})
    if slug not in GURUS:
        return jsonify({"error": "Unknown guru"}), 404
    g = GURUS[slug]
    return jsonify({"name": g["name"], "fund": g["fund"], "color": g["color"],
                    "numeric": g["rules"].get("numeric", {}),
                    "categorical": g["rules"].get("categorical", {})})

@bp.get("/api/guru/<slug>/holdings")
@limiter.limit("20 per minute")
def guru_holdings_route(slug):
    if slug not in GURUS:
        return jsonify({"error": "Unknown guru"}), 404
    if GURUS[slug].get('source') == 'nport':
        holdings, filing_date = _fetch_nport(slug)
    else:
        holdings, filing_date = _fetch_13f(slug)
    filing_val = sum((h.get('value') or 0) for h in holdings if h.get('ticker'))
    curr_val   = sum(
        (h.get('shares') or 0) * (h.get('currentPrice') or 0)
        for h in holdings if h.get('ticker') and h.get('shares') and h.get('currentPrice')
    )
    return jsonify({"filing_date": filing_date, "count": len(holdings),
                    "holdings": holdings, "filing_total_value": filing_val,
                    "current_total_value": curr_val})

@bp.get("/api/guru/<slug>/screen")
@limiter.limit("30 per minute")
def guru_screen_route(slug):
    if slug not in GURUS:
        return jsonify({"error": "Unknown guru"}), 404
    return jsonify({"count": len(_apply_guru_rules(slug)), "results": _apply_guru_rules(slug)})

@bp.get("/api/guru/stock-leaderboard")
def stock_leaderboard_api():
    from sqlalchemy import text as sql_text

    def _enrich_agg(ticker, guru_slugs, total_shares, total_value):
        gurus = [{"slug": s, "short": state._GURU_SHORT.get(s, s),
                  "color": state._GURU_COLOR.get(s, "#818cf8")} for s in guru_slugs]
        extra = {}
        if ticker and ticker in state.df.index:
            row = state.df.loc[ticker]
            extra = {
                "longName":          clean(row.get("longName")),
                "sector":            clean(row.get("sector")),
                "currentPrice":      clean(row.get("currentPrice")),
                "marketCap":         clean(row.get("marketCap")),
                "trailingPE":        clean(row.get("trailingPE")),
                "recommendationKey": clean(row.get("recommendationKey")),
            }
        return {"ticker": ticker, "guru_count": len(gurus), "gurus": gurus,
                "total_shares": int(total_shares or 0), "total_value": int(total_value or 0), **extra}

    try:
        engine = get_engine()
        with engine.connect() as conn:
            db_rows = conn.execute(sql_text("""
                SELECT ticker,
                       COUNT(DISTINCT slug) AS guru_count,
                       GROUP_CONCAT(DISTINCT slug ORDER BY value DESC SEPARATOR ',') AS guru_slugs,
                       SUM(shares) AS total_shares, SUM(value) AS total_value
                FROM guru_holdings
                WHERE ticker IS NOT NULL AND ticker != '' AND put_call = ''
                GROUP BY ticker ORDER BY guru_count DESC, total_value DESC
            """)).fetchall()
        if db_rows:
            items = [_enrich_agg(r.ticker, r.guru_slugs.split(",") if r.guru_slugs else [],
                                 r.total_shares, r.total_value) for r in db_rows]
            loaded = len({s for r in db_rows if r.guru_slugs for s in r.guru_slugs.split(",")})
            return jsonify({"source": "db", "loaded_gurus": loaded,
                            "total_gurus": len(GURUS), "items": items})
    except Exception as e:
        print(f"Stock leaderboard DB failed: {e}")

    agg: dict = {}
    for slug, (holdings, _, _) in state._holdings_cache.items():
        for h in holdings:
            tk = h.get("ticker")
            if not tk or h.get("put_call"):
                continue
            if tk not in agg:
                agg[tk] = {"slugs": [], "total_shares": 0, "total_value": 0}
            agg[tk]["slugs"].append(slug)
            agg[tk]["total_shares"] += int(h.get("shares") or 0)
            agg[tk]["total_value"]  += int(h.get("value")  or 0)
    items = sorted(
        [_enrich_agg(tk, v["slugs"], v["total_shares"], v["total_value"]) for tk, v in agg.items()],
        key=lambda x: (-x["guru_count"], -x["total_value"])
    )
    return jsonify({"source": "cache", "loaded_gurus": len(state._holdings_cache),
                    "total_gurus": len(GURUS), "items": items})

@bp.get("/stock-picks")
def stock_picks_page():
    return render_template("stock_picks.html")
