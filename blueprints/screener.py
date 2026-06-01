from flask import Blueprint, jsonify, render_template, request

import state
from helpers import clean, _price_changes
from extensions import limiter, require_auth
from db_config import get_engine
from config import _NYSE_TZ

bp = Blueprint("screener", __name__)

# ── Screener pages ─────────────────────────────────────────────────────────────
@bp.get("/screener")
def screener_page():
    return render_template("screener.html")

# ── Volume analysis ────────────────────────────────────────────────────────────
@bp.get("/api/volume-analysis")
def volume_analysis_api():
    from datetime import date as _d, datetime, timezone
    from sqlalchemy import text as _t

    engine = get_engine()
    today = _d.today()
    today_str = today.isoformat()

    _now_et = datetime.now(_NYSE_TZ)
    _open  = _now_et.replace(hour=9,  minute=30, second=0, microsecond=0)
    _close = _now_et.replace(hour=16, minute=0,  second=0, microsecond=0)
    _elapsed = (_now_et - _open).total_seconds()
    _total   = (_close - _open).total_seconds()
    _prorate = max(0.01, min(1.0, _elapsed / _total)) if _elapsed > 0 else 1.0

    with engine.connect() as conn:
        vol_rows = conn.execute(_t("""
            SELECT ticker,
                AVG(CASE WHEN date >= DATE_SUB(CURDATE(), INTERVAL 7  DAY) THEN volume END) AS vol_1w,
                AVG(CASE WHEN date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) THEN volume END) AS vol_1m,
                AVG(CASE WHEN date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY) THEN volume END) AS vol_3m
            FROM sp500_history GROUP BY ticker
        """)).fetchall()
        vol_map = {r.ticker: {"vol_1w": float(r.vol_1w) if r.vol_1w else None,
                              "vol_1m": float(r.vol_1m) if r.vol_1m else None,
                              "vol_3m": float(r.vol_3m) if r.vol_3m else None}
                   for r in vol_rows}

        intra_rows = conn.execute(_t("""
            SELECT i.ticker, SUM(i.volume) AS vol_today, sub.close AS last_close
            FROM sp500_intraday i
            JOIN (
                SELECT ticker, close FROM sp500_intraday i2
                JOIN (SELECT ticker AS t2, MAX(dt) AS max_dt FROM sp500_intraday
                      WHERE DATE(dt) = CURDATE() GROUP BY ticker) latest
                ON i2.ticker = latest.t2 AND i2.dt = latest.max_dt
            ) sub ON sub.ticker = i.ticker
            WHERE DATE(i.dt) = CURDATE()
            GROUP BY i.ticker, sub.close
        """)).fetchall()
        intra_map = {r.ticker: {"vol_today": int(r.vol_today), "last_close": float(r.last_close)}
                     for r in intra_rows}

        prev_close_rows = conn.execute(_t("""
            SELECT i.ticker, i.close AS prev_close
            FROM sp500_intraday i
            INNER JOIN (
                SELECT ticker, MAX(dt) AS max_dt
                FROM sp500_intraday WHERE DATE(dt) < CURDATE()
                GROUP BY ticker
            ) prev ON i.ticker = prev.ticker AND i.dt = prev.max_dt
        """)).fetchall()
        prev_close_map = {r.ticker: float(r.prev_close) for r in prev_close_rows}

        info_rows = conn.execute(_t("""
            SELECT ticker, longName, sector, earningsTimestampStart FROM sp500_info
        """)).fetchall()
        info_map = {r.ticker: {
            "longName": r.longName, "sector": r.sector,
            "earnings_date": (datetime.fromtimestamp(int(r.earningsTimestampStart), tz=timezone.utc)
                              .strftime("%Y-%m-%d") if r.earningsTimestampStart else None)
        } for r in info_rows}

    result = []
    for ticker in (state.df.index if hasattr(state.df, 'index') else info_map.keys()):
        ticker = str(ticker)
        vm  = vol_map.get(ticker, {})
        im  = intra_map.get(ticker, {})
        inf = info_map.get(ticker, {})
        vol_1m    = vm.get("vol_1m")
        vol_today = im.get("vol_today")
        _expected = vol_1m * _prorate if vol_1m else None
        abnormal  = bool(vol_today and _expected and vol_today > 2 * _expected)
        vol_ratio = round(vol_today / _expected, 1) if (vol_today and _expected) else None
        live = im.get("last_close")
        prev_close = prev_close_map.get(ticker)
        d1 = round((live - prev_close) / prev_close * 100, 2) if live and prev_close else None
        result.append({
            "ticker":       ticker,
            "longName":     inf.get("longName", ""),
            "sector":       inf.get("sector", ""),
            "vol_today":    vol_today,
            "vol_1w":       round(vm.get("vol_1w")) if vm.get("vol_1w") else None,
            "vol_1m":       round(vol_1m) if vol_1m else None,
            "vol_3m":       round(vm.get("vol_3m")) if vm.get("vol_3m") else None,
            "d1":           d1,
            "earnings_date": inf.get("earnings_date"),
            "abnormal":     abnormal,
            "vol_ratio":    vol_ratio,
            "current_price": live,
        })
    return jsonify([r for r in result if r["abnormal"]])

# ── Saved filters ──────────────────────────────────────────────────────────────
@bp.get("/api/screener/saved-filters")
def saved_filters_list():
    from sqlalchemy import text as _t
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(_t(
            "SELECT id, name, updated_at FROM screener_saved_filters ORDER BY name"
        )).fetchall()
    return jsonify([{"id": r.id, "name": r.name, "updated_at": str(r.updated_at)} for r in rows])

@bp.get("/api/screener/saved-filters/<int:filter_id>")
def saved_filters_get(filter_id):
    import json as _json
    from sqlalchemy import text as _t
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(_t(
            "SELECT id, name, filter_json FROM screener_saved_filters WHERE id = :id"
        ), {"id": filter_id}).fetchone()
    if not row:
        return jsonify({"error": "Not found"}), 404
    return jsonify({"id": row.id, "name": row.name, "filters": _json.loads(row.filter_json)})

@bp.post("/api/screener/saved-filters")
@require_auth
def saved_filters_save():
    import json as _json
    from sqlalchemy import text as _t
    body = request.get_json(force=True)
    name = (body.get("name") or "").strip()
    filters = body.get("filters", {})
    if not name:
        return jsonify({"ok": False, "error": "Name required"}), 400
    engine = get_engine()
    with engine.connect() as conn:
        existing = conn.execute(_t(
            "SELECT id FROM screener_saved_filters WHERE name = :n"
        ), {"n": name}).fetchone()
        if existing:
            conn.execute(_t(
                "UPDATE screener_saved_filters SET filter_json = :j WHERE name = :n"
            ), {"j": _json.dumps(filters), "n": name})
        else:
            conn.execute(_t(
                "INSERT INTO screener_saved_filters (name, filter_json) VALUES (:n, :j)"
            ), {"n": name, "j": _json.dumps(filters)})
        conn.commit()
        row = conn.execute(_t(
            "SELECT id FROM screener_saved_filters WHERE name = :n"
        ), {"n": name}).fetchone()
    return jsonify({"ok": True, "id": row.id, "overwritten": bool(existing)})

@bp.delete("/api/screener/saved-filters/<int:filter_id>")
@require_auth
def saved_filters_delete(filter_id):
    from sqlalchemy import text as _t
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(_t("DELETE FROM screener_saved_filters WHERE id = :id"), {"id": filter_id})
        conn.commit()
    return jsonify({"ok": True})

# ── Paper trades ───────────────────────────────────────────────────────────────
@bp.get("/api/vol-trades")
def vol_trades_api():
    from datetime import date as _d
    from sqlalchemy import text as _t
    today = _d.today().isoformat()
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(_t("""
            SELECT id, trade_date, ticker, buy_time, buy_price, amount_usd,
                   sell_time, sell_price, pnl_dollar, pnl_pct, status
            FROM vol_trades
            ORDER BY trade_date DESC, id DESC
        """)).fetchall()
        s = conn.execute(_t("""
            SELECT
              SUM(pnl_dollar) AS realized_pnl, SUM(amount_usd) AS realized_inv,
              SUM(CASE WHEN trade_date = CURDATE() THEN pnl_dollar ELSE 0 END) AS day_pnl,
              SUM(CASE WHEN trade_date = CURDATE() THEN amount_usd ELSE 0 END) AS day_inv,
              SUM(CASE WHEN trade_date >= DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) DAY)
                       THEN pnl_dollar ELSE 0 END) AS week_pnl,
              SUM(CASE WHEN trade_date >= DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) DAY)
                       THEN amount_usd ELSE 0 END) AS week_inv,
              SUM(CASE WHEN trade_date >= DATE_FORMAT(CURDATE(), '%Y-%m-01')
                       THEN pnl_dollar ELSE 0 END) AS month_pnl,
              SUM(CASE WHEN trade_date >= DATE_FORMAT(CURDATE(), '%Y-%m-01')
                       THEN amount_usd ELSE 0 END) AS month_inv,
              COUNT(CASE WHEN pnl_dollar > 0 THEN 1 END) AS winning,
              COUNT(*) AS total
            FROM vol_trades WHERE status = 'sold'
        """)).fetchone()

    def _pct(pnl, inv):
        return round(float(pnl) / float(inv) * 100, 2) if inv else None

    stats = {
        "realized_pnl_dollar": round(float(s.realized_pnl or 0), 2),
        "realized_pnl_pct":    _pct(s.realized_pnl, s.realized_inv),
        "day_pnl_dollar":  round(float(s.day_pnl  or 0), 2),
        "day_pnl_pct":     _pct(s.day_pnl,   s.day_inv),
        "week_pnl_pct":    _pct(s.week_pnl,  s.week_inv),
        "month_pnl_pct":   _pct(s.month_pnl, s.month_inv),
        "success_rate":    round(float(s.winning) / float(s.total) * 100, 1) if s.total else None,
    }
    trades = [{
        "id":         r.id,
        "trade_date": str(r.trade_date),
        "ticker":     r.ticker,
        "buy_time":   r.buy_time.strftime("%H:%M") if r.buy_time else None,
        "buy_price":  round(float(r.buy_price), 2) if r.buy_price else None,
        "amount_usd": float(r.amount_usd),
        "sell_time":  r.sell_time.strftime("%H:%M") if r.sell_time else None,
        "sell_price": round(float(r.sell_price), 2) if r.sell_price else None,
        "pnl_dollar": round(float(r.pnl_dollar), 2) if r.pnl_dollar is not None else None,
        "pnl_pct":    round(float(r.pnl_pct), 2) if r.pnl_pct is not None else None,
        "status":     r.status,
    } for r in rows]
    return jsonify({
        "trades":    trades,
        "stats":     stats,
        "buy_done":  state._vol_trade_buy_date  == today,
        "sell_done": state._vol_trade_sell_date == today,
    })

# ── Screener meta + run ────────────────────────────────────────────────────────
@bp.get("/api/screener/meta")
def screener_meta_route():
    return jsonify({"numeric": state._s_num_meta, "categorical": state._s_cat_meta,
                    "groups": state._s_group_order})

@bp.post("/api/screener/run")
@limiter.limit("60 per minute")
@require_auth
def screener_run():
    import pandas as pd
    from datetime import date as _d
    from sqlalchemy import text as _t

    body = request.get_json(force=True)
    mask = pd.Series(True, index=state.df.index)

    for fid, bounds in body.get("numeric", {}).items():
        if fid not in state.df.columns:
            continue
        fmt_ = state._s_num_meta.get(fid, {}).get("fmt", "num")
        div = 100 if fmt_ == "pct_frac" else 1
        col = state.df[fid]
        lo, hi = bounds.get("min"), bounds.get("max")
        if lo is not None:
            mask &= col >= (lo / div)
        if hi is not None:
            mask &= col <= (hi / div)

    for fid, vals in body.get("categorical", {}).items():
        if fid not in state.df.columns or not vals:
            continue
        mask &= state.df[fid].isin(vals)

    out_cols = ["longName", "sector", "currentPrice", "marketCap",
                "trailingPE", "dividendYield", "beta",
                "returnOnEquity", "revenueGrowth", "recommendationKey"]
    sub = state.df[mask][[c for c in out_cols if c in state.df.columns]]
    results = [{"ticker": str(t), **{k: clean(v) for k, v in row.items()}}
               for t, row in sub.iterrows()]

    for r in results:
        r["pc"] = _price_changes(r["ticker"])

    _today_str = _d.today().isoformat()
    try:
        _engine = get_engine()
        with _engine.connect() as _conn:
            _intra_rows = _conn.execute(_t("""
                SELECT i.ticker, i.close
                FROM sp500_intraday i
                INNER JOIN (
                    SELECT ticker, MAX(dt) AS max_dt
                    FROM sp500_intraday WHERE DATE(dt) = CURDATE() GROUP BY ticker
                ) latest ON i.ticker = latest.ticker AND i.dt = latest.max_dt
            """)).fetchall()
        _intraday_map = {row.ticker: float(row.close) for row in _intra_rows}
    except Exception:
        _intraday_map = {}

    for r in results:
        _live = _intraday_map.get(r["ticker"])
        _hist = state.hist_by_ticker.get(r["ticker"]) or []
        _prev = next((h["close"] for h in reversed(_hist)
                      if h["time"] < _today_str and h["close"] > 0), None)
        r["d1"] = round((_live - _prev) / _prev * 100, 2) if _live and _prev else None

    portfolio = {}
    for key in ("w1", "m1", "m3", "m6", "y1"):
        vals = [r["pc"][key] for r in results if key in r.get("pc", {})]
        portfolio[key] = round(sum(vals) / len(vals), 2) if vals else None

    return jsonify({"count": len(results), "results": results, "portfolio": portfolio})
