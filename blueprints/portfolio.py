import math
from flask import Blueprint, jsonify, render_template, request
from sqlalchemy import text as _t

import state
from extensions import require_auth
from db_config import get_engine

bp = Blueprint("portfolio", __name__)


def _v(val, default=None):
    """Return default if val is None or NaN."""
    try:
        return default if (val is None or math.isnan(float(val))) else val
    except Exception:
        return default


def _prev_close_map(conn):
    rows = conn.execute(_t("""
        SELECT i.ticker, i.close AS prev_close
        FROM sp500_intraday i
        INNER JOIN (
            SELECT ticker, MAX(dt) AS max_dt
            FROM sp500_intraday WHERE DATE(dt) < CURDATE()
            GROUP BY ticker
        ) prev ON i.ticker = prev.ticker AND i.dt = prev.max_dt
    """)).fetchall()
    return {r.ticker: float(r.prev_close) for r in rows}


@bp.get("/portfolio")
@require_auth
def portfolio_page():
    return render_template("portfolio.html")


@bp.get("/api/portfolio")
@require_auth
def portfolio_list():
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(_t("""
            SELECT id, ticker, shares, avg_cost, notes, added_at
            FROM portfolio_holdings ORDER BY added_at
        """)).fetchall()
        prev_map = _prev_close_map(conn)

    holdings = []
    total_value = 0.0
    total_cost  = 0.0
    total_day_pnl = 0.0

    sector_map = {}  # sector -> market_value

    for r in rows:
        ticker   = r.ticker
        shares   = float(r.shares)
        avg_cost = float(r.avg_cost)
        cost_basis = round(shares * avg_cost, 2)

        current_price = None
        long_name = ticker
        sector = None
        if ticker in state.df.index:
            current_price = _v(state.df.at[ticker, "currentPrice"])
            long_name     = _v(state.df.at[ticker, "longName"], ticker)
            sector        = _v(state.df.at[ticker, "sector"])

        market_value    = round(shares * current_price, 2) if current_price else None
        unreal_pnl      = round(market_value - cost_basis, 2) if market_value is not None else None
        unreal_pnl_pct  = round((current_price - avg_cost) / avg_cost * 100, 2) if current_price else None
        prev_close      = prev_map.get(ticker)
        day_chg_pct     = round((current_price - prev_close) / prev_close * 100, 2) if (current_price and prev_close) else None
        day_pnl         = round((current_price - prev_close) * shares, 2) if (current_price and prev_close) else None

        total_cost  += cost_basis
        if market_value is not None:
            total_value += market_value
            if sector:
                sector_map[sector] = sector_map.get(sector, 0) + market_value
        if day_pnl is not None:
            total_day_pnl += day_pnl

        holdings.append({
            "id":            r.id,
            "ticker":        ticker,
            "longName":      long_name,
            "sector":        sector,
            "shares":        shares,
            "avg_cost":      avg_cost,
            "cost_basis":    cost_basis,
            "current_price": current_price,
            "market_value":  market_value,
            "unreal_pnl":    unreal_pnl,
            "unreal_pnl_pct": unreal_pnl_pct,
            "day_chg_pct":   day_chg_pct,
            "day_pnl":       day_pnl,
            "notes":         r.notes or "",
            "added_at":      str(r.added_at)[:10],
        })

    total_pnl     = round(total_value - total_cost, 2)
    total_pnl_pct = round(total_pnl / total_cost * 100, 2) if total_cost else None
    sector_alloc  = [{"sector": s, "value": round(v, 2)} for s, v in
                     sorted(sector_map.items(), key=lambda x: -x[1])]

    return jsonify({
        "holdings": holdings,
        "summary": {
            "total_value":   round(total_value, 2),
            "total_cost":    round(total_cost, 2),
            "total_pnl":     total_pnl,
            "total_pnl_pct": total_pnl_pct,
            "day_pnl":       round(total_day_pnl, 2),
        },
        "sector_alloc": sector_alloc,
    })


@bp.post("/api/portfolio")
@require_auth
def portfolio_add():
    body     = request.get_json(force=True)
    ticker   = (body.get("ticker") or "").strip().upper()
    shares   = body.get("shares")
    avg_cost = body.get("avg_cost")
    notes    = (body.get("notes") or "").strip()
    if not ticker or shares is None or avg_cost is None:
        return jsonify({"ok": False, "error": "ticker, shares and avg_cost are required"}), 400
    engine = get_engine()
    with engine.connect() as conn:
        res = conn.execute(_t("""
            INSERT INTO portfolio_holdings (ticker, shares, avg_cost, notes)
            VALUES (:tk, :sh, :ac, :nt)
        """), {"tk": ticker, "sh": float(shares), "ac": float(avg_cost), "nt": notes})
        conn.commit()
        row_id = res.lastrowid
    return jsonify({"ok": True, "id": row_id})


@bp.put("/api/portfolio/<int:holding_id>")
@require_auth
def portfolio_update(holding_id):
    body     = request.get_json(force=True)
    shares   = body.get("shares")
    avg_cost = body.get("avg_cost")
    notes    = (body.get("notes") or "").strip()
    if shares is None or avg_cost is None:
        return jsonify({"ok": False, "error": "shares and avg_cost required"}), 400
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(_t("""
            UPDATE portfolio_holdings SET shares=:sh, avg_cost=:ac, notes=:nt WHERE id=:id
        """), {"sh": float(shares), "ac": float(avg_cost), "nt": notes, "id": holding_id})
        conn.commit()
    return jsonify({"ok": True})


@bp.delete("/api/portfolio/<int:holding_id>")
@require_auth
def portfolio_delete(holding_id):
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(_t("DELETE FROM portfolio_holdings WHERE id = :id"), {"id": holding_id})
        conn.commit()
    return jsonify({"ok": True})
