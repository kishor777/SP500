import math
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date as _date
from flask import Blueprint, jsonify, render_template, request
from sqlalchemy import text as _t

import state
from extensions import require_auth
from db_config import get_engine

bp = Blueprint("portfolio", __name__)


def _v(val, default=None):
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


def _live_price_map(conn):
    """Latest intraday close for today — most current price available."""
    rows = conn.execute(_t("""
        SELECT i.ticker, i.close AS live_price
        FROM sp500_intraday i
        INNER JOIN (
            SELECT ticker, MAX(dt) AS max_dt
            FROM sp500_intraday WHERE DATE(dt) = CURDATE()
            GROUP BY ticker
        ) today ON i.ticker = today.ticker AND i.dt = today.max_dt
    """)).fetchall()
    return {r.ticker: float(r.live_price) for r in rows}


def _get_claude_rec(ticker: str) -> dict:
    """Call Claude Sonnet acting as Charlie Munger: Munger 40% / fundamentals 40% / momentum 20%."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        return {"rec": None, "reason": "ANTHROPIC_API_KEY not configured"}

    try:
        import anthropic
    except ImportError:
        return {"rec": None, "reason": "anthropic package not installed — run: pip install anthropic"}

    from helpers import _price_changes

    def _fn(col, mult=1):
        if ticker not in state.df.index:
            return "N/A"
        try:
            v = state.df.at[ticker, col]
            if v is None or (isinstance(v, float) and math.isnan(v)):
                return "N/A"
            return f"{float(v) * mult:.1f}"
        except Exception:
            return "N/A"

    def _fs(col):
        if ticker not in state.df.index:
            return "N/A"
        try:
            v = state.df.at[ticker, col]
            if v is None or (isinstance(v, float) and math.isnan(v)):
                return "N/A"
            return str(v)
        except Exception:
            return "N/A"

    long_name = _fs("longName") if _fs("longName") != "N/A" else ticker
    sector    = _fs("sector")

    fcf = "N/A"
    if ticker in state.df.index:
        try:
            v = state.df.at[ticker, "freeCashflow"]
            if v and not math.isnan(float(v)):
                fcf = f"${float(v)/1e9:.1f}B"
        except Exception:
            pass

    pc = _price_changes(ticker)
    m3 = f"{pc['m3']:+.1f}%" if "m3" in pc else "N/A"
    m6 = f"{pc['m6']:+.1f}%" if "m6" in pc else "N/A"
    y1 = f"{pc['y1']:+.1f}%" if "y1" in pc else "N/A"

    prompt = f"""You are evaluating {ticker} ({long_name}, {sector}) using a weighted framework to issue a portfolio recommendation.

CHARLIE MUNGER PRINCIPLES — 40% weight:
Assess: durable competitive moat (brand/network/switching costs), simple predictable business model, high returns on capital with minimal reinvestment, honest capable management, strong free cash flow generation, low debt and financial complexity, consistent long-term earnings predictability.

FUNDAMENTALS — 40% weight:
ROE: {_fn("returnOnEquity", 100)}%  |  Profit Margin: {_fn("profitMargins", 100)}%  |  Gross Margin: {_fn("grossMargins", 100)}%
Operating Margin: {_fn("operatingMargins", 100)}%  |  Revenue Growth: {_fn("revenueGrowth", 100)}%  |  Earnings Growth: {_fn("earningsGrowth", 100)}%
Trailing P/E: {_fn("trailingPE")}  |  Forward P/E: {_fn("forwardPE")}  |  Price/Book: {_fn("priceToBook")}
EV/EBITDA: {_fn("enterpriseToEbitda")}  |  Debt/Equity: {_fn("debtToEquity")}  |  Free Cash Flow: {fcf}
Analyst Consensus: {_fs("recommendationKey")}

MOMENTUM — 20% weight:
3-Month Return: {m3}  |  6-Month Return: {m6}  |  1-Year Return: {y1}

Output EXACTLY two lines — no preamble, no explanation outside these two lines:
Line 1: One word only — Buy, Hold, or Sell
Line 2: Rationale, max 12 words, written as Munger himself would say it"""

    try:
        client = anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=80,
            messages=[{"role": "user", "content": prompt}],
        )
        text = msg.content[0].text.strip()
        lines = [l.strip() for l in text.split("\n") if l.strip()]
        rec = "Hold"
        for word in ("Buy", "Hold", "Sell"):
            if lines and word.lower() in lines[0].lower():
                rec = word
                break
        reason = lines[1] if len(lines) > 1 else ""
        return {"rec": rec, "reason": reason}
    except Exception as e:
        err = str(e)[:120]
        print(f"[claude-rec] {ticker}: {err}")
        return {"rec": None, "reason": err}


@bp.get("/api/portfolio/recs/status")
@require_auth
def portfolio_recs_status():
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    has_key = bool(api_key)
    try:
        import anthropic
        has_pkg = True
    except ImportError:
        has_pkg = False
    return jsonify({"has_key": has_key, "has_pkg": has_pkg,
                    "ready": has_key and has_pkg})


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
            SELECT id, ticker, shares, avg_cost, notes, added_at,
                   claude_rec, claude_rec_date, claude_rec_reason, market_currency
            FROM portfolio_holdings ORDER BY added_at
        """)).fetchall()
        prev_map = _prev_close_map(conn)
        live_map = _live_price_map(conn)

    holdings = []
    total_value   = 0.0
    total_cost    = 0.0
    total_day_pnl = 0.0

    for r in rows:
        ticker          = r.ticker
        shares          = float(r.shares)
        avg_cost        = float(r.avg_cost)
        cost_basis      = round(shares * avg_cost, 2)
        market_currency = r.market_currency or "USD"

        # Live price: today's intraday → state.df → yesterday's close
        current_price = live_map.get(ticker)
        if current_price is None and ticker in state.df.index:
            current_price = _v(state.df.at[ticker, "currentPrice"])
        if current_price is None:
            current_price = prev_map.get(ticker)

        long_name = ticker
        if ticker in state.df.index:
            long_name = _v(state.df.at[ticker, "longName"], ticker)

        market_value   = round(shares * current_price, 2) if current_price else None
        unreal_pnl     = round(market_value - cost_basis, 2) if market_value is not None else None
        unreal_pnl_pct = round((current_price - avg_cost) / avg_cost * 100, 2) if current_price else None
        prev_close     = prev_map.get(ticker)
        day_chg_pct    = round((current_price - prev_close) / prev_close * 100, 2) if (current_price and prev_close) else None
        day_pnl        = round((current_price - prev_close) * shares, 2) if (current_price and prev_close) else None

        total_cost += cost_basis
        if market_value is not None:
            total_value += market_value
        if day_pnl is not None:
            total_day_pnl += day_pnl

        holdings.append({
            "id":               r.id,
            "ticker":           ticker,
            "longName":         long_name,
            "market_currency":  market_currency,
            "shares":           shares,
            "avg_cost":         avg_cost,
            "current_price":    current_price,
            "market_value":     market_value,
            "unreal_pnl":       unreal_pnl,
            "unreal_pnl_pct":   unreal_pnl_pct,
            "day_chg_pct":      day_chg_pct,
            "day_pnl":          day_pnl,
            "claude_rec":       r.claude_rec,
            "claude_rec_date":  str(r.claude_rec_date) if r.claude_rec_date else None,
            "claude_rec_reason": r.claude_rec_reason or "",
            "notes":            r.notes or "",
            "added_at":         str(r.added_at)[:10],
        })

    total_pnl     = round(total_value - total_cost, 2)
    total_pnl_pct = round(total_pnl / total_cost * 100, 2) if total_cost else None

    return jsonify({
        "holdings": holdings,
        "summary": {
            "total_value":   round(total_value, 2),
            "total_cost":    round(total_cost, 2),
            "total_pnl":     total_pnl,
            "total_pnl_pct": total_pnl_pct,
            "day_pnl":       round(total_day_pnl, 2),
        },
    })


@bp.post("/api/portfolio/recs")
@require_auth
def portfolio_recs():
    force = (request.get_json(force=True, silent=True) or {}).get("force", False)
    today  = _date.today().isoformat()
    engine = get_engine()

    with engine.connect() as conn:
        rows = conn.execute(_t(
            "SELECT id, ticker, claude_rec_date FROM portfolio_holdings ORDER BY added_at"
        )).fetchall()

    to_compute = [r for r in rows if force or not r.claude_rec_date or str(r.claude_rec_date) != today]
    if not to_compute:
        with engine.connect() as conn:
            all_rows = conn.execute(_t(
                "SELECT ticker, claude_rec, claude_rec_date, claude_rec_reason FROM portfolio_holdings"
            )).fetchall()
        return jsonify({"ok": True, "recs": {
            r.ticker: {"rec": r.claude_rec,
                       "date": str(r.claude_rec_date) if r.claude_rec_date else None,
                       "reason": r.claude_rec_reason}
            for r in all_rows
        }})

    with ThreadPoolExecutor(max_workers=min(5, len(to_compute))) as pool:
        futures = {pool.submit(_get_claude_rec, r.ticker): r for r in to_compute}
        for future in as_completed(futures):
            r    = futures[future]
            data = future.result()
            with engine.connect() as conn:
                conn.execute(_t("""
                    UPDATE portfolio_holdings
                    SET claude_rec=:rec, claude_rec_date=:dt, claude_rec_reason=:reason
                    WHERE id=:id
                """), {"rec": data["rec"], "dt": today if data["rec"] else None,
                       "reason": data["reason"], "id": r.id})
                conn.commit()

    with engine.connect() as conn:
        all_rows = conn.execute(_t(
            "SELECT ticker, claude_rec, claude_rec_date, claude_rec_reason FROM portfolio_holdings"
        )).fetchall()
    return jsonify({"ok": True, "recs": {
        r.ticker: {"rec": r.claude_rec,
                   "date": str(r.claude_rec_date) if r.claude_rec_date else None,
                   "reason": r.claude_rec_reason}
        for r in all_rows
    }})


@bp.patch("/api/portfolio/<int:holding_id>/notes")
@require_auth
def portfolio_update_notes(holding_id):
    notes = (request.get_json(force=True).get("notes") or "").strip()
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(_t("UPDATE portfolio_holdings SET notes=:nt WHERE id=:id"),
                     {"nt": notes, "id": holding_id})
        conn.commit()
    return jsonify({"ok": True})


@bp.post("/api/portfolio/import")
@require_auth
def portfolio_import():
    body     = request.get_json(force=True)
    holdings = body.get("holdings", [])
    mode     = body.get("mode", "merge")
    if not holdings:
        return jsonify({"ok": False, "error": "No holdings provided"}), 400

    engine = get_engine()
    imported = 0
    with engine.connect() as conn:
        if mode == "replace":
            conn.execute(_t("DELETE FROM portfolio_holdings"))
            conn.commit()

        for h in holdings:
            ticker          = str(h.get("ticker", "")).strip().upper()
            shares          = float(h.get("shares", 0))
            avg_cost        = float(h.get("avg_cost", 0))
            notes           = str(h.get("notes", "")).strip()
            market_currency = str(h.get("market_currency", "USD"))[:3].upper()
            if not ticker or shares <= 0 or avg_cost <= 0:
                continue

            existing = conn.execute(_t(
                "SELECT id FROM portfolio_holdings WHERE ticker = :tk"
            ), {"tk": ticker}).fetchone()

            if existing and mode == "merge":
                conn.execute(_t("""
                    UPDATE portfolio_holdings
                    SET shares=:sh, avg_cost=:ac, notes=:nt, market_currency=:cur WHERE ticker=:tk
                """), {"sh": shares, "ac": avg_cost, "nt": notes,
                       "cur": market_currency, "tk": ticker})
            else:
                conn.execute(_t("""
                    INSERT INTO portfolio_holdings (ticker, shares, avg_cost, notes, market_currency)
                    VALUES (:tk, :sh, :ac, :nt, :cur)
                """), {"tk": ticker, "sh": shares, "ac": avg_cost,
                       "nt": notes, "cur": market_currency})
            imported += 1
        conn.commit()
    return jsonify({"ok": True, "count": imported})


@bp.post("/api/portfolio")
@require_auth
def portfolio_add():
    body            = request.get_json(force=True)
    ticker          = (body.get("ticker") or "").strip().upper()
    shares          = body.get("shares")
    avg_cost        = body.get("avg_cost")
    notes           = (body.get("notes") or "").strip()
    market_currency = (body.get("market_currency") or "USD").strip().upper()[:3]
    if not ticker or shares is None or avg_cost is None:
        return jsonify({"ok": False, "error": "ticker, shares and avg_cost are required"}), 400
    engine = get_engine()
    with engine.connect() as conn:
        res = conn.execute(_t("""
            INSERT INTO portfolio_holdings (ticker, shares, avg_cost, notes, market_currency)
            VALUES (:tk, :sh, :ac, :nt, :cur)
        """), {"tk": ticker, "sh": float(shares), "ac": float(avg_cost),
               "nt": notes, "cur": market_currency})
        conn.commit()
    return jsonify({"ok": True, "id": res.lastrowid})


@bp.put("/api/portfolio/<int:holding_id>")
@require_auth
def portfolio_update(holding_id):
    body            = request.get_json(force=True)
    shares          = body.get("shares")
    avg_cost        = body.get("avg_cost")
    notes           = (body.get("notes") or "").strip()
    market_currency = (body.get("market_currency") or "USD").strip().upper()[:3]
    if shares is None or avg_cost is None:
        return jsonify({"ok": False, "error": "shares and avg_cost required"}), 400
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(_t("""
            UPDATE portfolio_holdings
            SET shares=:sh, avg_cost=:ac, notes=:nt, market_currency=:cur WHERE id=:id
        """), {"sh": float(shares), "ac": float(avg_cost),
               "nt": notes, "cur": market_currency, "id": holding_id})
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
