import re
import time
from flask import Blueprint, jsonify, render_template, request

import state
from helpers import clean, row_to_dict, _market_is_open, _get_intraday
from config import TABLE_COLS, DETAIL_SECTIONS, SCHEDULER_CONFIG

bp = Blueprint("market_data", __name__)

_VALID_TICKER = re.compile(r'^[A-Z0-9.]{1,10}$')

@bp.get("/")
def index():
    return render_template("main.html")

@bp.get("/api/table")
def table_data():
    cols = [c for c in TABLE_COLS if c in state.df.columns]
    sub = state.df[cols].copy()
    sub.index = sub.index.astype(str)
    records = []
    for ticker, row in sub.iterrows():
        rec = {"ticker": ticker}
        rec.update(row_to_dict(row))
        records.append(rec)
    return jsonify(records)

@bp.get("/api/company/<ticker>")
def company_detail(ticker):
    if ticker not in state.df.index:
        return jsonify({"error": "Not found"}), 404
    row = state.df.loc[ticker]
    result = {"ticker": ticker, "sections": {}}
    for section, fields in DETAIL_SECTIONS.items():
        result["sections"][section] = {
            f: clean(row[f]) if f in row.index else None for f in fields
        }
    return jsonify(result)

@bp.get("/api/history/<ticker>")
def history_data(ticker):
    ticker = ticker.upper()
    if not _VALID_TICKER.match(ticker):
        return jsonify({"error": "Invalid ticker"}), 400
    interval = request.args.get("interval", "1d")
    if interval == "5m":
        return jsonify(_get_intraday(ticker))
    if ticker not in state.hist_by_ticker:
        return jsonify({"error": "Not found"}), 404
    return jsonify(state.hist_by_ticker[ticker])

@bp.get("/api/last-refresh")
def last_refresh_route():
    return jsonify(state._last_refresh)

@bp.get("/api/history-backfill-status")
def history_backfill_status():
    return jsonify(state._hist_backfill_status)

@bp.get("/health")
def health():
    market_open   = _market_is_open()
    rt = state._refresh_thread
    bt = state._backfill_thread
    refresh_alive = rt.is_alive() if rt else False
    epoch         = state._last_refresh.get("epoch", 0.0)
    age_sec       = int(time.time() - epoch) if epoch else None
    stale_limit   = SCHEDULER_CONFIG["refresh_interval_sec"] * 2

    degraded = not refresh_alive or (market_open and epoch and age_sec > stale_limit)

    return jsonify({
        "status":                   "degraded" if degraded else "ok",
        "market_open":              market_open,
        "refresh_thread":           "alive" if refresh_alive else "dead",
        "backfill_thread":          "alive" if (bt and bt.is_alive()) else "done",
        "last_refresh_status":      state._last_refresh.get("status"),
        "last_refresh_age_seconds": age_sec,
        "last_refresh_count":       state._last_refresh.get("count"),
    }), (503 if degraded else 200)
