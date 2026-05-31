"""S&P 500 info viewer — Flask backend."""
import logging
import math
import re
import time
import requests
import urllib3
import xml.etree.ElementTree as ET
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)  # SEC EDGAR, local dev only

# Suppress verbose library logging (prevents Railway 500-logs/sec rate limit)
for _lib in ("yfinance", "urllib3", "requests",
             "peewee", "apscheduler", "PIL", "transformers", "torch",
             "werkzeug"):
    logging.getLogger(_lib).setLevel(logging.WARNING)
logging.getLogger("sqlalchemy").setLevel(logging.ERROR)
import numpy as np
import pandas as pd
import os
from functools import wraps
from flask import Flask, jsonify, render_template, request, session, redirect
from flask_limiter import Limiter
from db_config import (get_engine, create_guru_tables, create_guru_rules_table,
                       migrate_guru_tables, create_intraday_table,
                       create_screener_filters_table, create_vol_trades_table,
                       create_sentiment_table, create_sp500_history_table)
from sentiment_engine import load_all_scores, run_sentiment_pass, should_run_sentiment, sentiment_status
from config import (
    _NYSE_TZ, _NYSE_HOLIDAYS, SCHEDULER_CONFIG,
    TABLE_COLS, DETAIL_SECTIONS,
    SCREENER_NUM_GROUPS, SCREENER_CAT_FIELDS,
    GURUS,
    _strip_words_re, _strip_punct_re, _13F_ABBREV,
    _TICKER_OVERRIDES, _norm, _TICKER_OVERRIDES_NORM,
    _EDGAR_UA, _DB_GURU_TTL,
    _OPTIMIZED_TTL, _OPT_SKIP, _OPT_PCT_FRAC,
    _RECO_TTL,
    RECO_MOMENTUM_WEIGHTS, RECO_FUNDAMENTAL_FIELDS, RECO_VALUATION_FIELDS,
    RECO_GURU_TAG_WEIGHT, RECO_ANALYST_SCORE_MAP,
    RECO_SIGNAL_WEIGHTS, RECO_LABEL_THRESHOLDS,
    RECO_OPT_PERIOD_WEIGHTS,
    SENTIMENT_STOCKTWITS_WEIGHT, SENTIMENT_REDDIT_WEIGHT,
    SENTIMENT_VOLUME_WEIGHT, SENTIMENT_MOMENTUM_WEIGHT,
)

app = Flask(__name__)

def _client_ip():
    return request.headers.get("X-Forwarded-For", request.remote_addr or "127.0.0.1").split(",")[0].strip()

limiter = Limiter(key_func=_client_ip, app=app, default_limits=["200 per minute"], storage_uri="memory://")

# ── Auth ──────────────────────────────────────────────────────────────────────
app.config["SECRET_KEY"] = os.environ.get("FLASK_SECRET_KEY") or os.urandom(24)
_ADMIN_SECRET = os.environ.get("ADMIN_SECRET", "").strip()
print(f"[auth] ADMIN_SECRET length={len(_ADMIN_SECRET)} set={bool(_ADMIN_SECRET)}")

def require_auth(f):
    @wraps(f)
    def _inner(*args, **kwargs):
        if _ADMIN_SECRET and not session.get("authed"):
            return jsonify({"error": "Unauthorized", "login": "/admin/login"}), 401
        return f(*args, **kwargs)
    return _inner

_LOGIN_ENDPOINTS = {"admin_login_page", "admin_login_redirect", "admin_login", "admin_logout", "health"}

@app.before_request
def _global_auth():
    if not _ADMIN_SECRET:
        return
    if request.endpoint in _LOGIN_ENDPOINTS:
        return
    if not session.get("authed"):
        if request.path.startswith("/api/"):
            return jsonify({"error": "Unauthorized", "login": "/admin/login"}), 401
        return redirect(f"/admin/login?next={request.path}")

# ── info — load from MySQL ────────────────────────────────────────────────────
def _load_info() -> pd.DataFrame:
    try:
        engine = get_engine()
        frame = pd.read_sql("SELECT * FROM sp500_info", engine)
        if "ticker" not in frame.columns:
            print("Warning: sp500_info has no ticker column — starting empty")
            return pd.DataFrame()
        frame = frame.set_index("ticker")
        frame.index.name = "ticker"
        print(f"Loaded {len(frame)} tickers from MySQL")
        return frame
    except Exception as e:
        print(f"Warning: could not load sp500_info ({e}) — starting with empty data. "
              "Restore from backup to populate.")
        return pd.DataFrame()

df = _load_info()

# ── history (grouped by ticker for O(1) lookup) ───────────────────────────────
def _load_history() -> dict:
    try:
        engine = get_engine()
        frame = pd.read_sql(
            "SELECT ticker, date, open, high, low, close, volume FROM sp500_history", engine
        )
        frame["date"] = pd.to_datetime(frame["date"]).dt.strftime("%Y-%m-%d")
        result = {
            t: g[["date", "open", "high", "low", "close", "volume"]]
                .rename(columns={"date": "time"})
                .sort_values("time")
                .to_dict("records")
            for t, g in frame.groupby("ticker")
        }
        print(f"Loaded history for {len(result)} tickers from MySQL")
        return result
    except Exception as e:
        print(f"Warning: could not load sp500_history ({e}) — charts will be empty until restore.")
        return {}

hist_by_ticker = _load_history()

# ── background price refresh (every 5 min, market hours only) ────────────────
import threading
from datetime import datetime as _dt, date as _date, time as _time, timedelta as _timedelta
from bisect import bisect_right as _bisect_right
from zoneinfo import ZoneInfo

def _yf_session() -> requests.Session:
    """Return a requests Session with SSL verification disabled for yfinance."""
    s = requests.Session()
    s.verify = False
    s.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})
    return s

def _market_is_open() -> bool:
    """Return True if NYSE regular session is currently in progress."""
    now = _dt.now(_NYSE_TZ)
    if now.weekday() >= 5:                    # Saturday / Sunday
        return False
    if now.date() in _NYSE_HOLIDAYS:
        return False
    return _time(9, 30) <= now.time() < _time(16, 0)

_last_refresh: dict = {"ts": None, "count": 0, "status": "pending", "epoch": 0.0}
_vol_trade_buy_date:  str | None = None   # date of last completed auto-buy
_vol_trade_sell_date: str | None = None   # date of last completed auto-sell

# ── Intraday helpers ──────────────────────────────────────────────────────────
def _save_intraday_bars(bars_df: "pd.DataFrame", tickers: list) -> int:
    """Upsert 5-minute bars into sp500_intraday. Commits per-ticker to avoid long locks."""
    from sqlalchemy import text as _t
    from datetime import datetime, timezone, timedelta

    if bars_df.empty:
        return 0

    # Normalise to MultiIndex regardless of single vs multi ticker download
    if not isinstance(bars_df.columns, pd.MultiIndex):
        bars_df.columns = pd.MultiIndex.from_tuples(
            [(c, tickers[0]) for c in bars_df.columns]
        )

    if "Close" not in bars_df.columns.get_level_values(0):
        return 0

    engine = get_engine()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=60)).replace(tzinfo=None)
    saved  = 0

    # Prune old rows once (quick, separate transaction)
    with engine.connect() as conn:
        conn.execute(_t("DELETE FROM sp500_intraday WHERE dt < :cutoff"), {"cutoff": cutoff})
        conn.commit()

    close_df = bars_df["Close"]

    INSERT_SQL = _t("""
        INSERT INTO sp500_intraday (ticker, dt, open, high, low, close, volume)
        VALUES (:tk, :dt, :o, :h, :l, :c, :v)
        ON DUPLICATE KEY UPDATE
            open=VALUES(open), high=VALUES(high), low=VALUES(low),
            close=VALUES(close), volume=VALUES(volume)
    """)

    for ticker in tickers:
        if ticker not in close_df.columns:
            continue
        try:
            t_open  = bars_df["Open"][ticker]
            t_high  = bars_df["High"][ticker]
            t_low   = bars_df["Low"][ticker]
            t_close = bars_df["Close"][ticker]
            t_vol   = bars_df["Volume"][ticker]
        except KeyError:
            continue

        rows = []
        for dt_idx in t_close.index:
            c = t_close[dt_idx]
            if pd.isna(c):
                continue
            dt_naive = pd.Timestamp(dt_idx).tz_convert("UTC").tz_localize(None)
            rows.append({
                "tk": ticker, "dt": dt_naive,
                "o":  float(t_open[dt_idx])  if not pd.isna(t_open[dt_idx])  else None,
                "h":  float(t_high[dt_idx])  if not pd.isna(t_high[dt_idx])  else None,
                "l":  float(t_low[dt_idx])   if not pd.isna(t_low[dt_idx])   else None,
                "c":  float(c),
                "v":  int(t_vol[dt_idx])     if not pd.isna(t_vol[dt_idx])   else 0,
            })

        if not rows:
            continue

        # Commit per-ticker — keeps transactions small, shows progress immediately
        with engine.connect() as conn:
            conn.execute(INSERT_SQL, rows)
            conn.commit()
        saved += len(rows)

    return saved


def _get_intraday(ticker: str) -> list:
    """Load 5-minute bars for a ticker from DB, ordered ascending."""
    from sqlalchemy import text as _t
    try:
        engine = get_engine()
        with engine.connect() as conn:
            rows = conn.execute(_t("""
                SELECT dt, open, high, low, close, volume
                FROM sp500_intraday WHERE ticker=:tk ORDER BY dt
            """), {"tk": ticker}).fetchall()
        # Return Unix timestamps (seconds) — required by lightweight-charts for intraday
        return [{"time": int(r.dt.timestamp()),
                 "open": r.open, "high": r.high,
                 "low":  r.low,  "close": r.close,
                 "volume": r.volume} for r in rows]
    except Exception as e:
        print(f"[intraday] load error [{ticker}]: {e}")
        return []


def _intraday_backfill():
    """Fetch 60 days of 5-min data in batches of 50 tickers (runs once at startup)."""
    import yfinance as yf
    from sqlalchemy import text as _t

    try:
        engine = get_engine()
        with engine.connect() as conn:
            count = conn.execute(_t("SELECT COUNT(*) FROM sp500_intraday")).scalar()
        if count and count > SCHEDULER_CONFIG["intraday_skip_threshold"]:
            return  # already populated
    except Exception:
        return

    tickers = list(df.index)
    batch_size = SCHEDULER_CONFIG["intraday_batch_size"]
    total_saved = 0
    batches = [tickers[i:i+batch_size] for i in range(0, len(tickers), batch_size)]
    _days = SCHEDULER_CONFIG["intraday_backfill_days"]
    print(f"[intraday] Starting {_days}-day backfill — {len(tickers)} tickers in {len(batches)} batches…")

    for i, batch in enumerate(batches, 1):
        try:
            data = yf.download(batch, period=f"{_days}d", interval="5m",
                               auto_adjust=True, progress=False, threads=True,
                               session=_yf_session())
            saved = _save_intraday_bars(data, batch)
            total_saved += saved
            print(f"[intraday] Batch {i}/{len(batches)} — {saved} bars saved (total {total_saved:,})")
        except Exception as e:
            print(f"[intraday] Batch {i} error: {e}")
        time.sleep(1)  # brief pause between batches

    print(f"[intraday] Backfill complete — {total_saved:,} bars total")


_hist_backfill_status: dict = {"state": "pending", "done": 0, "total": 0, "rows_added": 0, "msg": ""}


def _history_backfill_2y():
    """Backfill 2 years of daily OHLCV into sp500_history. Skips if already complete."""
    import yfinance as yf
    from sqlalchemy import text as _t
    from datetime import date as _date, timedelta

    # Check whether backfill is already done (earliest date within 5 days of 2Y ago)
    target_start = (_date.today() - timedelta(days=SCHEDULER_CONFIG["hist_backfill_days"])).isoformat()
    try:
        engine = get_engine()
        with engine.connect() as conn:
            earliest = conn.execute(_t(
                "SELECT MIN(date) FROM sp500_history"
            )).scalar()
        if earliest and str(earliest) <= target_start:
            _hist_backfill_status.update({"state": "done", "msg": "Already complete"})
            print(f"[hist-backfill] Already have data from {earliest} — skipping")
            return
    except Exception as e:
        # Table missing or empty — proceed with backfill
        print(f"[hist-backfill] Check failed ({e}), proceeding with backfill")

    tickers = list(df.index)
    batch_size = SCHEDULER_CONFIG["hist_batch_size"]
    batches = [tickers[i:i+batch_size] for i in range(0, len(tickers), batch_size)]
    _hist_backfill_status.update({"state": "running", "done": 0,
                                  "total": len(batches), "rows_added": 0,
                                  "msg": f"0/{len(batches)} batches"})
    print(f"[hist-backfill] Starting {SCHEDULER_CONFIG['hist_backfill_days']}d daily backfill — "
          f"{len(tickers)} tickers, {len(batches)} batches of {batch_size}")

    INSERT_SQL = _t("""
        INSERT IGNORE INTO sp500_history (ticker, date, open, high, low, close, volume)
        VALUES (:ticker, :date, :open, :high, :low, :close, :volume)
    """)

    total_rows = 0
    engine = get_engine()

    for i, batch in enumerate(batches, 1):
        try:
            data = yf.download(batch, period="2y", interval="1d",
                               auto_adjust=True, progress=False, threads=True,
                               session=_yf_session())
            if data.empty:
                continue

            # Normalise to MultiIndex
            if not isinstance(data.columns, pd.MultiIndex):
                data.columns = pd.MultiIndex.from_tuples([(c, batch[0]) for c in data.columns])

            batch_rows = 0
            for ticker in batch:
                try:
                    t_o = data["Open"][ticker]
                    t_h = data["High"][ticker]
                    t_l = data["Low"][ticker]
                    t_c = data["Close"][ticker]
                    t_v = data["Volume"][ticker]
                except KeyError:
                    continue

                rows = []
                for dt_idx in t_c.index:
                    c = t_c[dt_idx]
                    if pd.isna(c):
                        continue
                    rows.append({
                        "ticker": ticker,
                        "date":   pd.Timestamp(dt_idx).date().isoformat(),
                        "open":   float(t_o[dt_idx]) if not pd.isna(t_o[dt_idx]) else None,
                        "high":   float(t_h[dt_idx]) if not pd.isna(t_h[dt_idx]) else None,
                        "low":    float(t_l[dt_idx]) if not pd.isna(t_l[dt_idx]) else None,
                        "close":  float(c),
                        "volume": int(t_v[dt_idx])   if not pd.isna(t_v[dt_idx]) else 0,
                    })
                if not rows:
                    continue
                with engine.connect() as conn:
                    conn.execute(INSERT_SQL, rows)
                    conn.commit()
                batch_rows += len(rows)

            total_rows += batch_rows
            _hist_backfill_status.update({
                "done": i, "rows_added": total_rows,
                "msg": f"{i}/{len(batches)} batches — {total_rows:,} rows added"
            })
            print(f"[hist-backfill] Batch {i}/{len(batches)} — {batch_rows} rows (total {total_rows:,})")
        except Exception as e:
            print(f"[hist-backfill] Batch {i} error: {e}")

        time.sleep(1)

    # Hot-reload hist_by_ticker so charts and momentum immediately reflect 2Y data
    global hist_by_ticker
    hist_by_ticker = _load_history()
    _hist_backfill_status.update({"state": "done",
                                  "msg": f"Complete — {total_rows:,} rows added, history reloaded"})
    print(f"[hist-backfill] Done — {total_rows:,} rows added. hist_by_ticker reloaded.")


def _vol_trade_buy(today_str: str):
    """Paper-trade top-3 bullish volume-abnormal stocks (called once per day after 10:00 ET)."""
    from sqlalchemy import text as _t

    engine = get_engine()
    with engine.connect() as conn:
        existing = conn.execute(_t(
            "SELECT COUNT(*) FROM vol_trades WHERE trade_date = :d"
        ), {"d": today_str}).scalar()
    if existing > 0:
        print(f"[vol-trades] buy already recorded for {today_str} — skipping")
        return

    buy_time = _dt.now(_NYSE_TZ).replace(tzinfo=None)
    try:
        with engine.connect() as conn:
            vol_rows = conn.execute(_t("""
                SELECT ticker,
                    AVG(CASE WHEN date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                        THEN volume END) AS vol_1m
                FROM sp500_history GROUP BY ticker
            """)).fetchall()
            vol_1m_map = {r.ticker: float(r.vol_1m) if r.vol_1m else None for r in vol_rows}

            intra_rows = conn.execute(_t("""
                SELECT i.ticker, SUM(i.volume) AS vol_today, sub.close AS last_close
                FROM sp500_intraday i
                JOIN (
                    SELECT ticker, close FROM sp500_intraday i2
                    JOIN (SELECT ticker AS t2, MAX(dt) AS max_dt
                          FROM sp500_intraday WHERE DATE(dt) = CURDATE()
                          GROUP BY ticker) latest
                    ON i2.ticker = latest.t2 AND i2.dt = latest.max_dt
                ) sub ON sub.ticker = i.ticker
                WHERE DATE(i.dt) = CURDATE()
                GROUP BY i.ticker, sub.close
            """)).fetchall()
            intra_map = {r.ticker: {"vol_today": int(r.vol_today),
                                    "last_close": float(r.last_close)}
                         for r in intra_rows}
    except Exception as e:
        print(f"[vol-trades] buy data query error: {e}")
        return

    now_et  = _dt.now(_NYSE_TZ)
    open_t  = now_et.replace(hour=9,  minute=30, second=0, microsecond=0)
    close_t = now_et.replace(hour=16, minute=0,  second=0, microsecond=0)
    elapsed = (now_et - open_t).total_seconds()
    prorate = max(0.01, min(1.0, elapsed / (close_t - open_t).total_seconds()))

    candidates = []
    for ticker in list(df.index):
        ticker    = str(ticker)
        vol_1m    = vol_1m_map.get(ticker)
        im        = intra_map.get(ticker, {})
        vol_today = im.get("vol_today")
        live      = im.get("last_close")
        expected  = vol_1m * prorate if vol_1m else None
        if not (vol_today and expected and vol_today > 2 * expected and live):
            continue
        hist_rows  = hist_by_ticker.get(ticker) or []
        prev_close = next((r["close"] for r in reversed(hist_rows)
                           if r["time"] < today_str and r["close"] > 0), None)
        d1 = (live - prev_close) / prev_close * 100 if live and prev_close else None
        if d1 is None or d1 <= 0:
            continue
        candidates.append({"ticker": ticker, "buy_price": live,
                            "vol_ratio": vol_today / expected})

    candidates.sort(key=lambda x: x["vol_ratio"], reverse=True)
    top3 = candidates[:3]

    if not top3:
        print(f"[vol-trades] no bullish candidates at {buy_time.strftime('%H:%M')} — no trades placed")
        return

    with engine.connect() as conn:
        for c in top3:
            conn.execute(_t("""
                INSERT INTO vol_trades (trade_date, ticker, buy_time, buy_price, amount_usd, status)
                VALUES (:d, :tk, :bt, :bp, 1000, 'open')
            """), {"d": today_str, "tk": c["ticker"], "bt": buy_time, "bp": c["buy_price"]})
        conn.commit()

    print(f"[vol-trades] bought {[c['ticker'] for c in top3]} @ {buy_time.strftime('%H:%M ET')}")


def _vol_trade_sell(today_str: str):
    """Close all open paper trades from today at current intraday price (called once at 14:00 ET)."""
    from sqlalchemy import text as _t

    engine = get_engine()
    with engine.connect() as conn:
        open_trades = conn.execute(_t("""
            SELECT id, ticker, buy_price, amount_usd
            FROM vol_trades WHERE trade_date = :d AND status = 'open'
        """), {"d": today_str}).fetchall()

    if not open_trades:
        print(f"[vol-trades] no open trades to sell on {today_str}")
        return

    sell_time = _dt.now(_NYSE_TZ).replace(tzinfo=None)
    try:
        with engine.connect() as conn:
            price_rows = conn.execute(_t("""
                SELECT i.ticker, i.close
                FROM sp500_intraday i
                INNER JOIN (
                    SELECT ticker, MAX(dt) AS max_dt
                    FROM sp500_intraday WHERE DATE(dt) = CURDATE() GROUP BY ticker
                ) latest ON i.ticker = latest.ticker AND i.dt = latest.max_dt
            """)).fetchall()
        price_map = {r.ticker: float(r.close) for r in price_rows}
    except Exception as e:
        print(f"[vol-trades] sell price query error: {e}")
        return

    with engine.connect() as conn:
        for t in open_trades:
            sell_price = price_map.get(t.ticker)
            if sell_price is None:
                continue
            shares     = t.amount_usd / t.buy_price
            pnl_dollar = round((sell_price - t.buy_price) * shares, 2)
            pnl_pct    = round((sell_price - t.buy_price) / t.buy_price * 100, 2)
            conn.execute(_t("""
                UPDATE vol_trades
                SET sell_time = :st, sell_price = :sp,
                    pnl_dollar = :pd, pnl_pct = :pp, status = 'sold'
                WHERE id = :id
            """), {"st": sell_time, "sp": sell_price, "pd": pnl_dollar,
                   "pp": pnl_pct, "id": t.id})
        conn.commit()

    print(f"[vol-trades] sold {[t.ticker for t in open_trades]} @ {sell_time.strftime('%H:%M ET')}")


def _price_refresh_loop():
    import yfinance as yf

    while True:
        time.sleep(SCHEDULER_CONFIG["refresh_interval_sec"])
        if not _market_is_open():
            _last_refresh["status"] = "closed"
            print(f"[refresh] market closed — skipping ({_dt.now(_NYSE_TZ).strftime('%H:%M ET')})")
            continue
        try:
            tickers = list(df.index)

            sess = _yf_session()

            # ── daily close (currentPrice + hist_by_ticker) ───────────────
            daily = yf.download(tickers, period="2d", interval="1d",
                                auto_adjust=True, progress=False, threads=True,
                                session=sess)
            if not daily.empty:
                today   = _date.today().isoformat()
                updated = 0
                for ticker, price in daily["Close"].iloc[-1].items():
                    if pd.isna(price) or ticker not in df.index:
                        continue
                    price = round(float(price), 4)
                    df.at[ticker, "currentPrice"] = price
                    rows = hist_by_ticker.get(ticker)
                    if rows:
                        if rows[-1]["time"] == today:
                            rows[-1]["close"] = price
                        else:
                            rows.append({"time": today, "open": price, "high": price,
                                         "low": price, "close": price, "volume": 0})
                    updated += 1
                _last_refresh.update({"ts": _dt.now(_NYSE_TZ).strftime("%H:%M:%S ET"),
                                      "count": updated, "status": "open",
                                      "epoch": time.time()})
                print(f"[refresh] {updated} prices updated at {_last_refresh['ts']}")

            # ── intraday 5-min bars ───────────────────────────────────────
            intra = yf.download(tickers, period="1d", interval="5m",
                                auto_adjust=True, progress=False, threads=True,
                                session=sess)
            if not intra.empty:
                saved = _save_intraday_bars(intra, tickers)
                print(f"[intraday] {saved} bars upserted")

            # ── vol trade auto-scheduler ──────────────────────────────
            global _vol_trade_buy_date, _vol_trade_sell_date
            _now_et = _dt.now(_NYSE_TZ)
            _td = _date.today().isoformat()
            if _now_et.hour >= SCHEDULER_CONFIG["vol_trade_buy_hour"] and _vol_trade_buy_date != _td:
                _vol_trade_buy(_td)
                _vol_trade_buy_date = _td
            if _now_et.hour >= SCHEDULER_CONFIG["vol_trade_sell_hour"] and _vol_trade_sell_date != _td:
                _vol_trade_sell(_td)
                _vol_trade_sell_date = _td

        except Exception as _ex:
            _last_refresh["status"] = "error"
            print(f"[refresh] error: {_ex}")

# ── Create all tables FIRST, then start background threads ───────────────────
try:
    create_sp500_history_table()
    create_guru_tables()
    migrate_guru_tables()
    create_intraday_table()
    create_screener_filters_table()
    create_vol_trades_table()
    create_sentiment_table()
except Exception as _e:
    print(f"Could not create/migrate tables: {_e}")

# Price refresh starts immediately; backfills are staggered so they don't
# hammer yfinance and memory simultaneously on a cold start.
_refresh_thread = threading.Thread(target=_price_refresh_loop, daemon=True, name="price-refresh")
_refresh_thread.start()

def _delayed_backfill():
    time.sleep(10)                  # let gunicorn fully boot first
    _intraday_backfill()
    _history_backfill_2y()          # runs after intraday finishes

_backfill_thread = threading.Thread(target=_delayed_backfill, daemon=True, name="backfill")
_backfill_thread.start()


def clean(val):
    if isinstance(val, np.generic):
        val = val.item()          # convert any numpy scalar → native Python type
    if isinstance(val, float) and math.isnan(val):
        return None
    return val


def row_to_dict(row):
    return {k: clean(v) for k, v in row.items()}


@app.get("/")
def index():
    return render_template('main.html')


@app.get("/api/table")
def table_data():
    cols = [c for c in TABLE_COLS if c in df.columns]
    sub = df[cols].copy()
    sub.index = sub.index.astype(str)
    records = []
    for ticker, row in sub.iterrows():
        rec = {"ticker": ticker}
        rec.update(row_to_dict(row))
        records.append(rec)
    return jsonify(records)


@app.get("/api/company/<ticker>")
def company_detail(ticker):
    if ticker not in df.index:
        return jsonify({"error": "Not found"}), 404
    row = df.loc[ticker]
    result = {"ticker": ticker, "sections": {}}
    for section, fields in DETAIL_SECTIONS.items():
        result["sections"][section] = {
            f: clean(row[f]) if f in row.index else None for f in fields
        }
    return jsonify(result)


_VALID_TICKER = re.compile(r'^[A-Z0-9.]{1,10}$')

@app.get("/api/history/<ticker>")
def history_data(ticker):
    ticker = ticker.upper()
    if not _VALID_TICKER.match(ticker):
        return jsonify({"error": "Invalid ticker"}), 400
    interval = request.args.get("interval", "1d")
    if interval == "5m":
        return jsonify(_get_intraday(ticker))
    if ticker not in hist_by_ticker:
        return jsonify({"error": "Not found"}), 404
    return jsonify(hist_by_ticker[ticker])


# ── Guru / Coattail Investing ──────────────────────────────────────────────

# Build normalised-name → ticker lookup (both shortName and longName)
_name_to_ticker: dict[str, str] = {}
for _tk, _row in df.iterrows():
    for _f in ('shortName', 'longName'):
        _v = _row.get(_f)
        if pd.notna(_v):
            _n = _norm(str(_v))
            if _n:
                _name_to_ticker[_n] = _tk


def _match_ticker(issuer: str) -> str | None:
    """Try to resolve a 13F issuer name to an S&P 500 ticker."""
    n = _norm(issuer)
    # 0. Hard overrides — names that resist normalization
    for ok, ticker in _TICKER_OVERRIDES_NORM.items():
        if n == ok or n.startswith(ok):
            return ticker

    # 1. Exact match after normalisation
    if n in _name_to_ticker:
        return _name_to_ticker[n]

    # 2. Prefix match — handles trailing class/series tokens
    for k, v in _name_to_ticker.items():
        plen = min(len(n), len(k))
        if plen >= 6 and n[:plen] == k[:plen]:
            return v

    # 3. Word-overlap (Jaccard) — best score wins if ≥ 0.6
    n_words = set(n.split())
    if len(n_words) >= 1:
        best_score, best_tk = 0.0, None
        for k, v in _name_to_ticker.items():
            k_words = set(k.split())
            if not k_words:
                continue
            inter = len(n_words & k_words)
            if inter == 0:
                continue
            score = inter / max(len(n_words), len(k_words))
            if score > best_score:
                best_score, best_tk = score, v
        if best_score >= 0.60:
            return best_tk

    return None

# ── EDGAR 13F fetcher ──────────────────────────────────────────────────────

_holdings_cache: dict = {}   # slug → (list, filing_date, fetch_time)

# ── Guru rules DB helpers ──────────────────────────────────────────────────

def _save_guru_rules_to_db():
    """Seed guru_rules table from the in-memory GURUS dict."""
    import json as _json
    from sqlalchemy import text as sql_text
    try:
        engine = get_engine()
        with engine.connect() as conn:
            for slug, g in GURUS.items():
                rules = g.get("rules", {})
                for field_id, bounds in rules.get("numeric", {}).items():
                    for rule_type, val in bounds.items():  # bounds is {min: x} or {max: x}
                        conn.execute(sql_text("""
                            INSERT INTO guru_rules (slug, field_id, rule_type, num_value)
                            VALUES (:slug, :fid, :rt, :val)
                            ON DUPLICATE KEY UPDATE num_value=VALUES(num_value), is_active=1,
                                                    updated_at=CURRENT_TIMESTAMP
                        """), {"slug": slug, "fid": field_id, "rt": rule_type, "val": float(val)})
                for field_id, vals in rules.get("categorical", {}).items():
                    conn.execute(sql_text("""
                        INSERT INTO guru_rules (slug, field_id, rule_type, cat_values)
                        VALUES (:slug, :fid, 'categorical', :cv)
                        ON DUPLICATE KEY UPDATE cat_values=VALUES(cat_values), is_active=1,
                                                updated_at=CURRENT_TIMESTAMP
                    """), {"slug": slug, "fid": field_id, "cv": _json.dumps(vals)})
            conn.commit()
        print("Guru rules seeded to DB.")
    except Exception as e:
        print(f"Could not seed guru rules to DB: {e}")


def _load_guru_rules_from_db() -> dict:
    """Load active guru rules from DB. Returns {slug: {numeric: {...}, categorical: {...}}}."""
    import json as _json
    from sqlalchemy import text as sql_text
    result: dict = {}
    try:
        engine = get_engine()
        with engine.connect() as conn:
            rows = conn.execute(sql_text(
                "SELECT slug, field_id, rule_type, num_value, cat_values "
                "FROM guru_rules WHERE is_active=1"
            )).fetchall()
        for r in rows:
            slug = r.slug
            if slug not in result:
                result[slug] = {"numeric": {}, "categorical": {}}
            if r.rule_type in ("min", "max"):
                existing = result[slug]["numeric"].get(r.field_id, {})
                existing[r.rule_type] = r.num_value
                result[slug]["numeric"][r.field_id] = existing
            elif r.rule_type == "categorical":
                try:
                    result[slug]["categorical"][r.field_id] = _json.loads(r.cat_values or "[]")
                except Exception:
                    pass
    except Exception as e:
        print(f"Could not load guru rules from DB: {e}")
    return result


# ── Guru DB helpers ────────────────────────────────────────────────────────

def _db_save_holdings(slug: str, filing_date: str, holdings: list, source: str = '13f'):
    """Persist holdings for (slug, filing_date); keep at most 2 filings per guru."""
    from sqlalchemy import text as sql_text
    from datetime import datetime
    try:
        engine = get_engine()
        with engine.connect() as conn:
            # Upsert meta for this specific (slug, filing_date)
            conn.execute(sql_text("""
                INSERT INTO guru_filing_meta (slug, filing_date, fetched_at, source)
                VALUES (:slug, :fd, :now, :src)
                ON DUPLICATE KEY UPDATE fetched_at=VALUES(fetched_at), source=VALUES(source)
            """), {"slug": slug, "fd": filing_date, "now": datetime.utcnow(), "src": source})

            # Replace holdings for this filing date only (safe re-fetch)
            conn.execute(sql_text(
                "DELETE FROM guru_holdings WHERE slug=:slug AND filing_date=:fd"
            ), {"slug": slug, "fd": filing_date})

            for h in holdings:
                conn.execute(sql_text("""
                    INSERT INTO guru_holdings
                        (slug, filing_date, name, cusip, ticker, put_call, value, shares, weight, change_tag)
                    VALUES (:slug, :fd, :name, :cusip, :ticker, :pc, :val, :shr, :wgt, :chg)
                """), {
                    "slug": slug, "fd": filing_date,
                    "name":   h.get("name", ""),
                    "cusip":  h.get("cusip") or None,
                    "ticker": h.get("ticker") or None,
                    "pc":     h.get("put_call", ""),
                    "val":    int(h.get("value", 0)),
                    "shr":    int(h.get("shares", 0)),
                    "wgt":    h.get("weight"),
                    "chg":    h.get("change") or None,
                })

            # Prune: keep only the 2 most recent filing_dates per guru
            all_dates = conn.execute(sql_text("""
                SELECT filing_date FROM guru_filing_meta
                WHERE slug=:slug ORDER BY filing_date DESC
            """), {"slug": slug}).fetchall()
            for row in all_dates[2:]:
                old_fd = str(row[0])
                conn.execute(sql_text(
                    "DELETE FROM guru_holdings WHERE slug=:slug AND filing_date=:fd"
                ), {"slug": slug, "fd": old_fd})
                conn.execute(sql_text(
                    "DELETE FROM guru_filing_meta WHERE slug=:slug AND filing_date=:fd"
                ), {"slug": slug, "fd": old_fd})

            conn.commit()
    except Exception as e:
        print(f"DB save holdings failed [{slug}]: {e}")


def _db_load_holdings(slug: str) -> tuple[list | None, str | None]:
    """Load latest filing from DB. Returns (None, None) if missing or older than _DB_GURU_TTL."""
    from sqlalchemy import text as sql_text
    from datetime import datetime
    try:
        engine = get_engine()
        with engine.connect() as conn:
            meta = conn.execute(sql_text("""
                SELECT filing_date, fetched_at FROM guru_filing_meta
                WHERE slug=:slug ORDER BY filing_date DESC LIMIT 1
            """), {"slug": slug}).fetchone()
            if not meta:
                return None, None
            filing_date, fetched_at = meta
            if (datetime.utcnow() - fetched_at).total_seconds() > _DB_GURU_TTL:
                return None, None
            rows = conn.execute(sql_text("""
                SELECT name, cusip, ticker, put_call, value, shares, weight, change_tag
                FROM guru_holdings WHERE slug=:slug AND filing_date=:fd ORDER BY value DESC
            """), {"slug": slug, "fd": str(filing_date)}).fetchall()
        holdings = [
            {"name": r.name, "cusip": r.cusip or "", "ticker": r.ticker or "",
             "put_call": r.put_call or "", "value": r.value, "shares": r.shares,
             "weight": r.weight, "change": r.change_tag or ""}
            for r in rows
        ]
        return holdings, str(filing_date)
    except Exception as e:
        print(f"DB load holdings failed [{slug}]: {e}")
        return None, None


def _db_load_prev_for_diff(slug: str, current_date: str) -> dict:
    """Return previous filing's holdings as {ticker|put_call: value} for change computation."""
    from sqlalchemy import text as sql_text
    try:
        engine = get_engine()
        with engine.connect() as conn:
            prev = conn.execute(sql_text("""
                SELECT filing_date FROM guru_filing_meta
                WHERE slug=:slug AND filing_date < :cd ORDER BY filing_date DESC LIMIT 1
            """), {"slug": slug, "cd": current_date}).fetchone()
            if not prev:
                return {}
            rows = conn.execute(sql_text("""
                SELECT ticker, put_call, value FROM guru_holdings
                WHERE slug=:slug AND filing_date=:fd
            """), {"slug": slug, "fd": str(prev[0])}).fetchall()
        return {f"{r.ticker or ''}|{r.put_call or ''}": r.value for r in rows}
    except Exception as e:
        print(f"DB load prev holdings failed [{slug}]: {e}")
        return {}


def _enrich_holdings(raw: list, filing_date: str | None) -> list:
    """Join raw 13F rows with sp500_info (prices, sector, etc.)."""
    total_val = sum(h["value"] for h in raw) or 1
    enriched = []
    for h in raw:
        tk = h.get("ticker") or None
        extra: dict = {}
        if tk and tk in df.index:
            row = df.loc[tk]
            extra = {
                "longName":          clean(row.get("longName")),
                "sector":            clean(row.get("sector")),
                "currentPrice":      clean(row.get("currentPrice")),
                "marketCap":         clean(row.get("marketCap")),
                "trailingPE":        clean(row.get("trailingPE")),
                "returnOnEquity":    clean(row.get("returnOnEquity")),
                "profitMargins":     clean(row.get("profitMargins")),
                "revenueGrowth":     clean(row.get("revenueGrowth")),
                "52WeekChange":      clean(row.get("52WeekChange")),
                "recommendationKey": clean(row.get("recommendationKey")),
            }
        weight = h.get("weight") if h.get("weight") is not None else round(h["value"] / total_val * 100, 2)
        enriched.append({**h, "weight": weight, **extra})
    return enriched

def _edgar_get(url: str, timeout: int = 90) -> requests.Response:
    """GET with 3 retries and exponential back-off for EDGAR endpoints."""
    for attempt in range(3):
        try:
            return requests.get(url, headers=_EDGAR_UA, timeout=timeout, verify=False)
        except Exception:
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)

def _dedup_rows(rows: list) -> list:
    """Merge rows with same issuer name + put/call type by summing value+shares."""
    merged: dict = {}
    for r in rows:
        key = (_norm(r['name']), r.get('put_call', ''))
        if key in merged:
            merged[key]['value']  += r['value']
            merged[key]['shares'] += r['shares']
        else:
            merged[key] = dict(r)
    return sorted(merged.values(), key=lambda x: x['value'], reverse=True)


def _parse_info_table(text: str) -> list:
    """Parse 13F information table from XML (with or without namespace) or regex fallback.

    The SEC 13F standard reports <value> in thousands of USD, so we normally multiply
    by 1000.  Some filers (including Berkshire recent filings) instead report full dollar
    amounts.  We auto-detect: if any raw value >= 1e8 the file is already in dollars.
    """
    def _apply_multiplier(rows: list) -> list:
        if not rows:
            return rows
        max_raw = max(r['value'] for r in rows)
        mult = 1 if max_raw >= 100_000_000 else 1000
        for r in rows:
            r['value'] = r['value'] * mult
        return rows

    # Try ElementTree
    try:
        clean_xml = re.sub(r'<\?xml[^?]*\?>', '', text).strip()
        root = ET.fromstring(clean_xml)
        ns = (root.tag.split('}')[0] + '}') if root.tag.startswith('{') else ''
        items = list(root.iter(f'{ns}infoTable')) or list(root.iter('infoTable'))
        rows = []
        for info in items:
            def _gt(tag):
                return (info.findtext(f'{ns}{tag}') or info.findtext(tag) or '').strip()
            val_str = _gt('value').replace(',', '')
            try:
                raw_value = int(float(val_str))
            except ValueError:
                raw_value = 0
            shrs_el = info.find(f'{ns}shrsOrPrnAmt') or info.find('shrsOrPrnAmt')
            shares = 0
            if shrs_el is not None:
                try:
                    shares = int(float((shrs_el.findtext(f'{ns}sshPrnamt') or
                                        shrs_el.findtext('sshPrnamt') or '0').replace(',', '')))
                except ValueError:
                    shares = 0
            name = _gt('nameOfIssuer')
            put_call = _gt('putCall').upper()   # 'PUT', 'CALL', or '' for equity
            if name and raw_value > 0:
                rows.append({'name': name, 'cusip': _gt('cusip'),
                             'value': raw_value, 'shares': shares, 'put_call': put_call})
        if rows:
            return _dedup_rows(_apply_multiplier(rows))
    except Exception:
        pass

    # Regex fallback for HTM/XML variants
    pat = re.compile(
        r'<nameOfIssuer[^>]*>([^<]+)</nameOfIssuer>.*?'
        r'<cusip[^>]*>([^<]+)</cusip>.*?<value[^>]*>([^<]+)</value>.*?'
        r'<sshPrnamt[^>]*>([^<]+)</sshPrnamt>', re.DOTALL | re.IGNORECASE)
    rows = []
    for m in pat.finditer(text):
        try:
            raw_value = int(float(m.group(3).replace(',', '')))
            shares = int(float(m.group(4).replace(',', '')))
        except ValueError:
            continue
        if raw_value > 0:
            ctx = text[max(0, m.start() - 300): m.end() + 300]
            pc_m = re.search(r'<putCall[^>]*>([^<]*)</putCall>', ctx, re.IGNORECASE)
            put_call = pc_m.group(1).strip().upper() if pc_m else ''
            rows.append({'name': m.group(1).strip(), 'cusip': m.group(2).strip(),
                         'value': raw_value, 'shares': shares, 'put_call': put_call})
    return _dedup_rows(_apply_multiplier(rows))


def _fetch_13f(slug: str) -> tuple[list, str | None]:
    """Fetch latest 13F-HR, using DB cache (24h) then in-memory cache (1h)."""
    # 1. In-memory cache
    cached = _holdings_cache.get(slug)
    if cached and (time.time() - cached[2]) < 3600:
        return cached[0], cached[1]

    # 2. DB cache
    raw_db, filing_date_db = _db_load_holdings(slug)
    if raw_db is not None:
        enriched = _enrich_holdings(raw_db, filing_date_db)
        _holdings_cache[slug] = (enriched, filing_date_db, time.time())
        return enriched, filing_date_db

    # 3. Fetch from EDGAR
    guru = GURUS[slug]
    cik = guru['cik']
    cik_padded = cik.zfill(10)

    try:
        sub = _edgar_get(f"https://data.sec.gov/submissions/CIK{cik_padded}.json").json()

        recent = sub['filings']['recent']
        accession = filing_date = None
        for i, form in enumerate(recent['form']):
            if form in ('13F-HR', '13F-HR/A'):
                accession = recent['accessionNumber'][i]
                filing_date = recent['filingDate'][i]
                break
        if not accession:
            return [], None

        acc_nd = accession.replace('-', '')
        base_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nd}"

        dir_html = _edgar_get(f"{base_url}/").text
        all_links = re.findall(r'href="(/Archives/edgar/data/[^"]+\.(xml|htm|html))"',
                               dir_html, re.I)
        doc_name = None
        xml_links = [l[0].split('/')[-1] for l in all_links if l[0].endswith('.xml')]
        for nm in xml_links:
            if nm.lower() != 'primary_doc.xml':
                doc_name = nm
                break
        if not doc_name and xml_links:
            doc_name = xml_links[0]
        if not doc_name:
            return [], filing_date

        doc_text = _edgar_get(f"{base_url}/{doc_name}").text
        raw = _dedup_rows(_parse_info_table(doc_text))

        max_h = guru.get('max_holdings', 500)
        raw = raw[:max_h]

        total_val = sum(h['value'] for h in raw) or 1

        # Change indicators vs previous filing (loaded from DB — survives restarts)
        prev_vals = _db_load_prev_for_diff(slug, filing_date)

        raw_with_meta = []
        for h in raw:
            tk = _match_ticker(h['name'])
            put_call = h.get('put_call', '')
            prev_key = f"{tk or ''}|{put_call}"
            if not prev_vals:
                change = ''
            elif prev_key not in prev_vals:
                change = 'new'
            elif h['value'] > prev_vals[prev_key] * 1.005:
                change = 'added'
            elif h['value'] < prev_vals[prev_key] * 0.995:
                change = 'reduced'
            else:
                change = 'held'
            raw_with_meta.append({**h, 'ticker': tk, 'put_call': put_call, 'change': change,
                                   'weight': round(h['value'] / total_val * 100, 2)})

        # Save to DB (keeps 2 most recent filings)
        _db_save_holdings(slug, filing_date, raw_with_meta)

        enriched = _enrich_holdings(raw_with_meta, filing_date)
        _holdings_cache[slug] = (enriched, filing_date, time.time())
        return enriched, filing_date

    except Exception as ex:
        print(f"13F fetch error [{slug}]: {ex}")
        return [], None


def _fetch_nport(slug: str) -> tuple[list, str | None]:
    """Fetch latest NPORT-P for an ETF series from EDGAR, using DB cache (24h)."""
    # 1. In-memory cache
    cached = _holdings_cache.get(slug)
    if cached and (time.time() - cached[2]) < 3600:
        return cached[0], cached[1]

    # 2. DB cache
    raw_db, filing_date_db = _db_load_holdings(slug)
    if raw_db is not None:
        enriched = _enrich_holdings(raw_db, filing_date_db)
        _holdings_cache[slug] = (enriched, filing_date_db, time.time())
        return enriched, filing_date_db

    # 3. Fetch from EDGAR
    guru = GURUS[slug]
    cik = guru['cik']
    series_id = guru['series_id']
    cik_padded = cik.zfill(10)

    try:
        sub = _edgar_get(f"https://data.sec.gov/submissions/CIK{cik_padded}.json").json()
        recent = sub['filings']['recent']

        accession = filing_date = None
        for i, form in enumerate(recent['form']):
            if form != 'NPORT-P':
                continue
            acc = recent['accessionNumber'][i]
            acc_nd = acc.replace('-', '')
            r = _edgar_get(f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nd}/primary_doc.xml")
            if series_id in r.text:
                accession = acc
                filing_date = recent['filingDate'][i]
                doc_text = r.text
                break

        if not accession:
            return [], None

        ns = {'n': 'http://www.sec.gov/edgar/nport'}
        root = ET.fromstring(doc_text)

        rep_date = None
        rd = root.find('.//n:repPdDate', ns)
        if rd is not None:
            rep_date = rd.text

        raw_holdings = []
        total_val = 0.0
        for sec in root.findall('.//n:invstOrSec', ns):
            name_el   = sec.find('n:name', ns)
            val_el    = sec.find('n:valUSD', ns)
            pct_el    = sec.find('n:pctVal', ns)
            shares_el = sec.find('n:balance', ns)
            ticker_el = sec.find('.//n:ticker', ns)

            name   = name_el.text              if name_el   is not None else ''
            val    = float(val_el.text)        if val_el    is not None else 0.0
            pct    = float(pct_el.text)        if pct_el    is not None else 0.0
            shares = float(shares_el.text)     if shares_el is not None else 0.0
            tk     = ticker_el.attrib.get('value') if ticker_el is not None else None

            total_val += val
            raw_holdings.append({'name': name, 'value': int(val), 'shares': int(shares),
                                  'weight': round(pct, 2), 'ticker': tk or '', 'put_call': ''})

        final_date = rep_date or filing_date
        raw_sorted = sorted(raw_holdings, key=lambda x: x['value'], reverse=True)

        _db_save_holdings(slug, final_date, raw_sorted, source='nport')
        enriched = _enrich_holdings(raw_sorted, final_date)
        _holdings_cache[slug] = (enriched, final_date, time.time())
        return enriched, final_date

    except Exception as ex:
        print(f"NPORT fetch error [{slug}]: {ex}")
        return [], None


def _price_changes(ticker: str) -> dict:
    """Return 1W/1M/3M/1Y % price changes from in-memory history."""
    rows = hist_by_ticker.get(ticker)
    if not rows or len(rows) < 5:
        return {}
    last_close = rows[-1]["close"]
    today = _date.fromisoformat(rows[-1]["time"])
    times = [r["time"] for r in rows]
    changes = {}
    for key, days in (("w1", 7), ("m1", 30), ("m3", 91), ("m6", 182), ("y1", 365)):
        target = (today - _timedelta(days=days)).isoformat()
        idx = _bisect_right(times, target) - 1
        if idx >= 0 and rows[idx]["close"] > 0:
            changes[key] = round((last_close - rows[idx]["close"]) / rows[idx]["close"] * 100, 2)
    return changes


def _apply_guru_rules(slug: str) -> list:
    """Filter S&P 500 universe by guru's investment rules."""
    rules = GURUS[slug]['rules']
    mask = pd.Series(True, index=df.index)
    for fid, bounds in rules.get('numeric', {}).items():
        if fid not in df.columns:
            continue
        fmt_ = _s_num_meta.get(fid, {}).get('fmt', 'num')
        div = 100 if fmt_ == 'pct_frac' else 1
        lo, hi = bounds.get('min'), bounds.get('max')
        if lo is not None:
            mask &= df[fid] >= lo / div
        if hi is not None:
            mask &= df[fid] <= hi / div
    for fid, vals in rules.get('categorical', {}).items():
        if fid not in df.columns or not vals:
            continue
        mask &= df[fid].isin(vals)

    out_cols = ['longName', 'sector', 'currentPrice', 'marketCap', 'trailingPE',
                'returnOnEquity', 'profitMargins', 'revenueGrowth', 'dividendYield',
                '52WeekChange', 'recommendationKey']
    sub = df[mask][[c for c in out_cols if c in df.columns]]
    results = [{'ticker': str(t), **{k: clean(v) for k, v in row.items()}}
               for t, row in sub.iterrows()]
    results.sort(key=lambda x: x.get('marketCap') or 0, reverse=True)
    return results


def _build_screener_meta():
    num_meta = {}
    for group, fields in SCREENER_NUM_GROUPS.items():
        for f in fields:
            fid = f["id"]
            if fid not in df.columns:
                continue
            s = df[fid].dropna()
            if len(s) == 0 or not pd.api.types.is_numeric_dtype(s):
                continue
            fmt_ = f.get("fmt", "num")
            mult = 100 if fmt_ == "pct_frac" else 1
            num_meta[fid] = {
                "label": f["label"], "group": group, "fmt": fmt_,
                "min":    round(float(s.min())             * mult, 4),
                "max":    round(float(s.max())             * mult, 4),
                "p5":     round(float(s.quantile(0.05))   * mult, 4),
                "p95":    round(float(s.quantile(0.95))   * mult, 4),
                "median": round(float(s.median())         * mult, 4),
            }
    cat_meta = {}
    for f in SCREENER_CAT_FIELDS:
        fid = f["id"]
        if fid not in df.columns:
            continue
        vals = sorted(df[fid].dropna().unique().tolist())
        cat_meta[fid] = {"label": f["label"], "values": vals}
    return num_meta, cat_meta


_s_num_meta, _s_cat_meta = _build_screener_meta()
_s_group_order = list(SCREENER_NUM_GROUPS.keys())


@app.get("/screener")
def screener_page():
    return render_template('screener.html')


@app.get("/api/volume-analysis")
def volume_analysis_api():
    from datetime import date as _d, timedelta, datetime, timezone
    from sqlalchemy import text as _t
    engine = get_engine()
    today = _d.today()
    today_str = today.isoformat()

    # Pro-rata factor: how much of the trading day (9:30–16:00 ET) has elapsed
    _now_et = datetime.now(_NYSE_TZ)
    _open  = _now_et.replace(hour=9,  minute=30, second=0, microsecond=0)
    _close = _now_et.replace(hour=16, minute=0,  second=0, microsecond=0)
    _elapsed = (_now_et - _open).total_seconds()
    _total   = (_close - _open).total_seconds()   # 23400 s = 390 min
    _prorate = max(0.01, min(1.0, _elapsed / _total)) if _elapsed > 0 else 1.0

    with engine.connect() as conn:
        # avg daily volumes from history
        vol_rows = conn.execute(_t("""
            SELECT ticker,
                AVG(CASE WHEN date >= DATE_SUB(CURDATE(), INTERVAL 7  DAY) THEN volume END) AS vol_1w,
                AVG(CASE WHEN date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) THEN volume END) AS vol_1m,
                AVG(CASE WHEN date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY) THEN volume END) AS vol_3m
            FROM sp500_history
            GROUP BY ticker
        """)).fetchall()
        vol_map = {r.ticker: {"vol_1w": float(r.vol_1w) if r.vol_1w else None,
                              "vol_1m": float(r.vol_1m) if r.vol_1m else None,
                              "vol_3m": float(r.vol_3m) if r.vol_3m else None}
                   for r in vol_rows}

        # today's intraday volume and latest close
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

        # company info + earnings
        info_rows = conn.execute(_t("""
            SELECT ticker, longName, sector, earningsTimestampStart
            FROM sp500_info
        """)).fetchall()
        info_map = {r.ticker: {
            "longName": r.longName,
            "sector": r.sector,
            "earnings_date": (datetime.fromtimestamp(int(r.earningsTimestampStart), tz=timezone.utc).strftime("%Y-%m-%d")
                              if r.earningsTimestampStart else None)
        } for r in info_rows}

    result = []
    for ticker in (df.index if hasattr(df, 'index') else info_map.keys()):
        ticker = str(ticker)
        vm  = vol_map.get(ticker, {})
        im  = intra_map.get(ticker, {})
        inf = info_map.get(ticker, {})

        vol_1m = vm.get("vol_1m")
        vol_today = im.get("vol_today")
        # Compare today's vol to the prorated expected volume (1M avg × elapsed fraction)
        _expected = vol_1m * _prorate if vol_1m else None
        abnormal  = bool(vol_today and _expected and vol_today > 2 * _expected)
        vol_ratio = round(vol_today / _expected, 1) if (vol_today and _expected) else None

        # today price change
        hist_rows = hist_by_ticker.get(ticker) or []
        prev_close = next((r["close"] for r in reversed(hist_rows)
                           if r["time"] < today_str and r["close"] > 0), None)
        live = im.get("last_close")
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


@app.get("/api/last-refresh")
def last_refresh_route():
    return jsonify(_last_refresh)


@app.get("/api/history-backfill-status")
def history_backfill_status():
    return jsonify(_hist_backfill_status)


@app.get("/health")
def health():
    market_open   = _market_is_open()
    refresh_alive = _refresh_thread.is_alive()
    epoch         = _last_refresh.get("epoch", 0.0)
    age_sec       = int(time.time() - epoch) if epoch else None
    stale_limit   = SCHEDULER_CONFIG["refresh_interval_sec"] * 2  # 2 missed cycles = problem

    degraded = not refresh_alive or (market_open and epoch and age_sec > stale_limit)

    return jsonify({
        "status":                 "degraded" if degraded else "ok",
        "market_open":            market_open,
        "refresh_thread":         "alive" if refresh_alive else "dead",
        "backfill_thread":        "alive" if _backfill_thread.is_alive() else "done",
        "last_refresh_status":    _last_refresh.get("status"),
        "last_refresh_age_seconds": age_sec,
        "last_refresh_count":     _last_refresh.get("count"),
    }), (503 if degraded else 200)


# ── Admin login ───────────────────────────────────────────────────────────────

@app.get("/admin/login")
def admin_login_page():
    logged_in = bool(session.get("authed"))
    info = "Already logged in." if logged_in else None
    return render_template('login.html', error=None, info=info,
                           next=request.args.get("next", "/"), logged_in=logged_in)

@app.get("/api/admin/login")
def admin_login_redirect():
    return redirect("/admin/login")

@app.post("/api/admin/login")
def admin_login():
    password = request.form.get("password", "")
    next_url  = request.form.get("next", "/")
    if not next_url.startswith("/") or next_url.startswith("//"):
        next_url = "/"
    if not _ADMIN_SECRET:
        session["authed"] = True
        return redirect(next_url)
    if password == _ADMIN_SECRET:
        session["authed"] = True
        return redirect(next_url)
    return render_template('login.html', error="Incorrect password.",
                           info=None, next=next_url, logged_in=False), 401

@app.get("/api/admin/logout")
def admin_logout():
    session.clear()
    return redirect("/admin/login")


@app.get("/api/screener/saved-filters")
def saved_filters_list():
    from sqlalchemy import text as _t
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(_t(
            "SELECT id, name, updated_at FROM screener_saved_filters ORDER BY name"
        )).fetchall()
    return jsonify([{"id": r.id, "name": r.name, "updated_at": str(r.updated_at)} for r in rows])


@app.get("/api/screener/saved-filters/<int:filter_id>")
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


@app.post("/api/screener/saved-filters")
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


@app.delete("/api/screener/saved-filters/<int:filter_id>")
@require_auth
def saved_filters_delete(filter_id):
    from sqlalchemy import text as _t
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(_t("DELETE FROM screener_saved_filters WHERE id = :id"), {"id": filter_id})
        conn.commit()
    return jsonify({"ok": True})


@app.get("/api/vol-trades")
def vol_trades_api():
    from sqlalchemy import text as _t
    today = _date.today().isoformat()
    engine = get_engine()
    with engine.connect() as conn:
        # Last 30 days of trades for the table
        rows = conn.execute(_t("""
            SELECT id, trade_date, ticker, buy_time, buy_price, amount_usd,
                   sell_time, sell_price, pnl_dollar, pnl_pct, status
            FROM vol_trades
            WHERE trade_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
            ORDER BY trade_date DESC, id DESC
        """)).fetchall()
        # Aggregate stats across periods (sold trades only)
        s = conn.execute(_t("""
            SELECT
              SUM(pnl_dollar)                                                                   AS realized_pnl,
              SUM(amount_usd)                                                                   AS realized_inv,
              SUM(CASE WHEN trade_date = CURDATE()                               THEN pnl_dollar ELSE 0 END) AS day_pnl,
              SUM(CASE WHEN trade_date = CURDATE()                               THEN amount_usd ELSE 0 END) AS day_inv,
              SUM(CASE WHEN trade_date >= DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) DAY)  THEN pnl_dollar ELSE 0 END) AS week_pnl,
              SUM(CASE WHEN trade_date >= DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) DAY)  THEN amount_usd ELSE 0 END) AS week_inv,
              SUM(CASE WHEN trade_date >= DATE_FORMAT(CURDATE(), '%Y-%m-01')                    THEN pnl_dollar ELSE 0 END) AS month_pnl,
              SUM(CASE WHEN trade_date >= DATE_FORMAT(CURDATE(), '%Y-%m-01')                    THEN amount_usd ELSE 0 END) AS month_inv,
              COUNT(CASE WHEN pnl_dollar > 0 THEN 1 END) AS winning,
              COUNT(*)                                   AS total
            FROM vol_trades WHERE status = 'sold'
        """)).fetchone()

    def _pct(pnl, inv):
        return round(float(pnl) / float(inv) * 100, 2) if inv else None

    stats = {
        "realized_pnl_dollar": round(float(s.realized_pnl or 0), 2),
        "realized_pnl_pct":    _pct(s.realized_pnl, s.realized_inv),
        "day_pnl_dollar": round(float(s.day_pnl  or 0), 2),
        "day_pnl_pct":    _pct(s.day_pnl,   s.day_inv),
        "week_pnl_pct":   _pct(s.week_pnl,  s.week_inv),
        "month_pnl_pct":  _pct(s.month_pnl, s.month_inv),
        "success_rate":   round(float(s.winning) / float(s.total) * 100, 1) if s.total else None,
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
        "buy_done":  _vol_trade_buy_date  == today,
        "sell_done": _vol_trade_sell_date == today,
    })


@app.get("/api/screener/meta")
def screener_meta_route():
    return jsonify({"numeric": _s_num_meta, "categorical": _s_cat_meta,
                    "groups": _s_group_order})


@app.post("/api/screener/run")
@limiter.limit("60 per minute")
@require_auth
def screener_run():
    body = request.get_json(force=True)
    mask = pd.Series(True, index=df.index)

    for fid, bounds in body.get("numeric", {}).items():
        if fid not in df.columns:
            continue
        fmt_ = _s_num_meta.get(fid, {}).get("fmt", "num")
        div = 100 if fmt_ == "pct_frac" else 1
        col = df[fid]
        lo, hi = bounds.get("min"), bounds.get("max")
        if lo is not None:
            mask &= col >= (lo / div)
        if hi is not None:
            mask &= col <= (hi / div)

    for fid, vals in body.get("categorical", {}).items():
        if fid not in df.columns or not vals:
            continue
        mask &= df[fid].isin(vals)

    out_cols = ["longName", "sector", "currentPrice", "marketCap",
                "trailingPE", "dividendYield", "beta",
                "returnOnEquity", "revenueGrowth", "recommendationKey"]
    sub = df[mask][[c for c in out_cols if c in df.columns]]
    results = [{"ticker": str(t), **{k: clean(v) for k, v in row.items()}}
               for t, row in sub.iterrows()]

    # Attach price changes from history
    for r in results:
        r["pc"] = _price_changes(r["ticker"])

    # Compute today's change (d1) via latest intraday bar vs previous daily close
    from datetime import date as _date
    from sqlalchemy import text as _t
    _today_str = _date.today().isoformat()
    try:
        _engine = get_engine()
        with _engine.connect() as _conn:
            _intra_rows = _conn.execute(_t("""
                SELECT i.ticker, i.close
                FROM sp500_intraday i
                INNER JOIN (
                    SELECT ticker, MAX(dt) AS max_dt
                    FROM sp500_intraday
                    WHERE DATE(dt) = CURDATE()
                    GROUP BY ticker
                ) latest ON i.ticker = latest.ticker AND i.dt = latest.max_dt
            """)).fetchall()
        _intraday_map = {row.ticker: float(row.close) for row in _intra_rows}
    except Exception:
        _intraday_map = {}
    for r in results:
        _live = _intraday_map.get(r["ticker"])
        _hist = hist_by_ticker.get(r["ticker"]) or []
        _prev = next((h["close"] for h in reversed(_hist)
                      if h["time"] < _today_str and h["close"] > 0), None)
        r["d1"] = round((_live - _prev) / _prev * 100, 2) if _live and _prev else None

    # Portfolio summary: equal-weighted avg per period
    portfolio = {}
    for key in ("w1", "m1", "m3", "m6", "y1"):
        vals = [r["pc"][key] for r in results if key in r.get("pc", {})]
        portfolio[key] = round(sum(vals) / len(vals), 2) if vals else None

    return jsonify({"count": len(results), "results": results, "portfolio": portfolio})


# ── Guru routes ────────────────────────────────────────────────────────────

@app.get("/leaderboard")
def leaderboard_page():
    return render_template('leaderboard.html')


@app.get("/gurus")
def gurus_page():
    return render_template('gurus.html')


@app.get("/api/guru/list")
def guru_list():
    return jsonify([
        {"slug": s, "name": g["name"], "fund": g["fund"],
         "style": g["style"], "color": g["color"]}
        for s, g in GURUS.items()
    ])


@app.get("/api/guru/<slug>/info")
def guru_info_route(slug):
    if slug not in GURUS:
        return jsonify({"error": "Unknown guru"}), 404
    g = GURUS[slug]
    rule_meta = {
        fid: {"label": _s_num_meta.get(fid, {}).get("label", fid),
              "fmt":   _s_num_meta.get(fid, {}).get("fmt", "num")}
        for fid in g["rules"].get("numeric", {})
    }
    return jsonify({**g, "rule_meta": rule_meta})


_optimized_rules_cache: dict = {}   # {"rules": {...}, "ts": float}

def _build_optimized_guru_rules() -> dict:
    """Data-driven screener rules that maximize equal-weighted portfolio returns.

    For each field in the screener meta, tests quantile thresholds and picks the
    direction/value that maximises a composite score weighted toward longer periods.
    Rules are combined greedily (best improvement first) until <20 tickers remain.
    Result is cached for _OPTIMIZED_TTL seconds.
    """
    global _optimized_rules_cache
    cached = _optimized_rules_cache
    if cached and (time.time() - cached.get("ts", 0)) < _OPTIMIZED_TTL:
        return cached["rules"]

    WEIGHTS = RECO_OPT_PERIOD_WEIGHTS

    all_changes = {t: _price_changes(t) for t in df.index}
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
    candidate_fields = {fid for fid in _s_num_meta if fid not in _OPT_SKIP}

    field_best: list = []
    for fid in candidate_fields:
        if fid not in df.columns:
            continue
        col = df[fid].dropna()
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

    current_mask = pd.Series(True, index=df.index)
    final_rules: dict = {}
    for fid, direction, val, _sc, _gain in field_best:
        if fid not in df.columns or fid in final_rules:
            continue
        col = df[fid]
        div = 100 if fid in _OPT_PCT_FRAC else 1
        test_mask = current_mask.copy()
        if direction == "min":
            test_mask &= col.fillna(-1e18) >= val / div
        else:
            test_mask &= col.fillna(1e18) <= val / div
        passing = set(df[test_mask].index) & valid_tickers
        if len(passing) < 20:
            continue
        if _score(passing) >= _score(set(df[current_mask].index) & valid_tickers):
            current_mask = test_mask
            final_rules[fid] = {direction: val}

    _optimized_rules_cache = {"rules": final_rules, "ts": time.time()}
    return final_rules


def _build_master_guru_rules() -> dict:
    """Aggregate all guru numeric rules into a balanced combined filter.

    Strategy:
    - Only include fields used by ≥3 gurus (more consensus = more signal).
    - Exclude fields that conflict with quality stocks in a combined context:
      EV/EBITDA, P/S, P/B create false negatives when combined with margin/ROE rules
      because high-quality companies legitimately trade at premium valuations.
      earningsQuarterlyGrowth is too volatile. beta range is too narrow combined.
    - For min rules use the p25 of gurus' thresholds (lenient floor).
    - For max rules use the p75 of gurus' thresholds (lenient ceiling).
      This keeps the combined filter achievable while still representing consensus.
    """
    from collections import defaultdict

    _EXCLUDE = {
        "enterpriseToEbitda",          # conflicts with high-quality valuations
        "priceToSalesTrailing12Months", # tech/healthcare always high
        "priceToBook",                  # intangible-heavy firms have high/negative BV
        "earningsQuarterlyGrowth",      # too volatile, not in enough gurus
    }

    def _percentile(sorted_vals, pct):
        """Return the p-th percentile of a sorted list (0–100)."""
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
            val = _percentile(sorted(vals), 25)   # lenient floor
            numeric.setdefault(fid, {})["min"] = round(val, 2)
    for fid, vals in maxs.items():
        if len(vals) >= 3:
            val = _percentile(sorted(vals), 75)   # lenient ceiling
            numeric.setdefault(fid, {})["max"] = round(val, 2)

    # beta: keep only max (don't restrict low-beta quality stocks from below)
    if "beta" in numeric and "min" in numeric["beta"]:
        del numeric["beta"]["min"]
        if not numeric["beta"]:
            del numeric["beta"]

    return numeric


@app.get("/api/guru/<slug>/screener-rules")
def guru_screener_rules(slug):
    """Return guru rules ready for the screener — numeric rules are {fid: {min|max: val}}."""
    if slug == "master":
        numeric = _build_master_guru_rules()
        n_gurus = len(GURUS)
        return jsonify({
            "name": "Master Guru", "fund": f"Consensus of {n_gurus} gurus",
            "color": "#f59e0b",
            "numeric": numeric, "categorical": {},
        })
    if slug == "optimized":
        numeric = _build_optimized_guru_rules()
        return jsonify({
            "name": "Optimized Guru", "fund": "Data-driven — maximizes portfolio returns",
            "color": "#10b981",
            "numeric": numeric, "categorical": {},
        })
    if slug not in GURUS:
        return jsonify({"error": "Unknown guru"}), 404
    g = GURUS[slug]
    return jsonify({
        "name": g["name"], "fund": g["fund"], "color": g["color"],
        "numeric":     g["rules"].get("numeric", {}),
        "categorical": g["rules"].get("categorical", {}),
    })


@app.get("/api/guru/<slug>/holdings")
@limiter.limit("20 per minute")
def guru_holdings_route(slug):
    if slug not in GURUS:
        return jsonify({"error": "Unknown guru"}), 404
    if GURUS[slug].get('source') == 'nport':
        holdings, filing_date = _fetch_nport(slug)
    else:
        holdings, filing_date = _fetch_13f(slug)
    # Performance stats — compare filing-date value vs current price × shares
    filing_val = sum((h.get('value') or 0) for h in holdings if h.get('ticker'))
    curr_val   = sum(
        (h.get('shares') or 0) * (h.get('currentPrice') or 0)
        for h in holdings
        if h.get('ticker') and h.get('shares') and h.get('currentPrice')
    )
    return jsonify({
        "filing_date": filing_date,
        "count": len(holdings),
        "holdings": holdings,
        "filing_total_value": filing_val,
        "current_total_value": curr_val,
    })


@app.get("/api/guru/<slug>/screen")
@limiter.limit("30 per minute")
def guru_screen_route(slug):
    if slug not in GURUS:
        return jsonify({"error": "Unknown guru"}), 404
    results = _apply_guru_rules(slug)
    return jsonify({"count": len(results), "results": results})


_GURU_SHORT = {slug: g["name"].split()[-1] for slug, g in GURUS.items()}
_GURU_COLOR = {slug: g["color"] for slug, g in GURUS.items()}

@app.get("/api/guru/stock-leaderboard")
def stock_leaderboard_api():
    from sqlalchemy import text as sql_text

    def _enrich_agg(ticker, guru_slugs, total_shares, total_value):
        gurus = [{"slug": s, "short": _GURU_SHORT.get(s, s), "color": _GURU_COLOR.get(s, "#818cf8")}
                 for s in guru_slugs]
        extra = {}
        if ticker and ticker in df.index:
            row = df.loc[ticker]
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

    # Try DB first
    try:
        engine = get_engine()
        with engine.connect() as conn:
            db_rows = conn.execute(sql_text("""
                SELECT   ticker,
                         COUNT(DISTINCT slug)  AS guru_count,
                         GROUP_CONCAT(DISTINCT slug ORDER BY value DESC SEPARATOR ',') AS guru_slugs,
                         SUM(shares)           AS total_shares,
                         SUM(value)            AS total_value
                FROM     guru_holdings
                WHERE    ticker IS NOT NULL AND ticker != '' AND put_call = ''
                GROUP BY ticker
                ORDER BY guru_count DESC, total_value DESC
            """)).fetchall()
        if db_rows:
            items = [_enrich_agg(r.ticker,
                                 r.guru_slugs.split(",") if r.guru_slugs else [],
                                 r.total_shares, r.total_value)
                     for r in db_rows]
            loaded = len({s for r in db_rows if r.guru_slugs for s in r.guru_slugs.split(",")})
            return jsonify({"source": "db", "loaded_gurus": loaded,
                            "total_gurus": len(GURUS), "items": items})
    except Exception as e:
        print(f"Stock leaderboard DB failed: {e}")

    # Fall back to in-memory cache
    agg: dict = {}
    for slug, (holdings, _, _) in _holdings_cache.items():
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
        [_enrich_agg(tk, v["slugs"], v["total_shares"], v["total_value"])
         for tk, v in agg.items()],
        key=lambda x: (-x["guru_count"], -x["total_value"])
    )
    return jsonify({"source": "cache", "loaded_gurus": len(_holdings_cache),
                    "total_gurus": len(GURUS), "items": items})


@app.get("/stock-picks")
def stock_picks_page():
    return render_template('stock_picks.html')


# ── Recommendation Engine ─────────────────────────────────────────────────────
_reco_cache: dict = {"data": [], "ts": 0.0}

def _pct_rank(series: "pd.Series") -> "pd.Series":
    """Return 0–100 percentile rank; higher value = higher score."""
    return series.rank(pct=True, na_option="bottom") * 100

def _inv_pct_rank(series: "pd.Series") -> "pd.Series":
    """Inverted 0–100 percentile rank; lower value = higher score."""
    return (1 - series.rank(pct=True, na_option="top")) * 100

def _compute_recommendations() -> list:
    """Score every S&P 500 stock across 5 signals and return ranked list."""
    from sqlalchemy import text as _t

    # Latest intraday close for today (used for d1 calculation)
    try:
        engine = get_engine()
        with engine.connect() as conn:
            rows = conn.execute(_t("""
                SELECT i.ticker, i.close
                FROM sp500_intraday i
                INNER JOIN (
                    SELECT ticker, MAX(dt) AS max_dt
                    FROM sp500_intraday
                    WHERE DATE(dt) = CURDATE()
                    GROUP BY ticker
                ) latest ON i.ticker = latest.ticker AND i.dt = latest.max_dt
            """)).fetchall()
        _intraday_latest = {r.ticker: float(r.close) for r in rows}
    except Exception:
        _intraday_latest = {}

    scores = pd.DataFrame(index=df.index)

    # ── 1. Momentum (25%) ────────────────────────────────────────────────────
    mom_rows = {}
    for ticker in df.index:
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
        scores["momentum"] = _pct_rank(weighted.reindex(df.index))
    else:
        scores["momentum"] = 50.0

    # ── 2. Fundamental quality (25%) ─────────────────────────────────────────
    fund_fields = RECO_FUNDAMENTAL_FIELDS
    fund_score = pd.Series(0.0, index=df.index)
    w_used = 0.0
    for fld, w in fund_fields.items():
        if fld in df.columns:
            fund_score += _pct_rank(df[fld]) * w
            w_used += w
    scores["fundamental"] = (fund_score / w_used) if w_used else 50.0

    # ── 3. Valuation (15%) — lower is better ─────────────────────────────────
    val_fields = RECO_VALUATION_FIELDS
    val_score = pd.Series(0.0, index=df.index)
    w_used = 0.0
    for fld, w in val_fields.items():
        if fld in df.columns:
            # Clamp extreme negatives (distressed companies) before ranking
            col = df[fld].clip(lower=0)
            val_score += _inv_pct_rank(col) * w
            w_used += w
    scores["valuation"] = (val_score / w_used) if w_used else 50.0

    # ── 4. Guru conviction (20%) ─────────────────────────────────────────────
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
                {tk: v["score"] / max_score * 100 for tk, v in guru_scores.items()},
                dtype=float
            ).reindex(df.index, fill_value=0.0)
        else:
            guru_series = pd.Series(0.0, index=df.index)
        scores["guru"] = guru_series
        guru_counts = pd.Series(
            {tk: v["count"] for tk, v in guru_scores.items()}, dtype=int
        ).reindex(df.index, fill_value=0)
    except Exception as e:
        print(f"[reco] guru score error: {e}")
        scores["guru"] = 50.0
        guru_counts = pd.Series(0, index=df.index)

    # ── 5. Analyst consensus (15%) ────────────────────────────────────────────
    REC_MAP = RECO_ANALYST_SCORE_MAP
    if "recommendationKey" in df.columns:
        scores["analyst"] = df["recommendationKey"].map(
            lambda x: REC_MAP.get(str(x).lower().replace(" ", ""), 50) if pd.notna(x) else 50
        ).astype(float)
    else:
        scores["analyst"] = 50.0

    # ── 6. Market Sentiment (15%) ─────────────────────────────────────────────
    engine = get_engine()
    sentiment_map = load_all_scores(engine)

    # Trigger background pass for top-20 + bottom-20 if schedule warrants it
    prelim_order = scores[["momentum", "fundamental", "valuation", "guru", "analyst"]].mean(axis=1)
    sorted_prelim = prelim_order.sort_values(ascending=False).index
    target_tickers = list(sorted_prelim[:20]) + list(sorted_prelim[-20:])
    target_tickers = list(dict.fromkeys(target_tickers))  # dedup, preserve order

    if should_run_sentiment(sentiment_map, engine):
        def _invalidate_reco_cache():
            _reco_cache["ts"] = 0.0
        run_sentiment_pass(target_tickers, engine, on_complete=_invalidate_reco_cache)

    sent_series = pd.Series(
        {tk: v["score"] for tk, v in sentiment_map.items()},
        dtype=float,
    ).reindex(df.index, fill_value=50.0)   # neutral for un-scored tickers
    scores["sentiment"] = sent_series

    # ── Composite ─────────────────────────────────────────────────────────────
    W = RECO_SIGNAL_WEIGHTS
    scores["score"] = sum(scores[k].fillna(50) * w for k, w in W.items())
    scores["score"] = scores["score"].round(1)

    def _label(s):
        return next(lbl for thr, lbl in RECO_LABEL_THRESHOLDS if s >= thr)

    # ── Build output ──────────────────────────────────────────────────────────
    keep = ["longName", "sector", "currentPrice", "marketCap",
            "returnOnEquity", "grossMargins", "trailingPE",
            "revenueGrowth", "recommendationKey"]
    sub = df[[c for c in keep if c in df.columns]].copy()

    result = []
    _today_str = _date.today().isoformat()
    sorted_tickers = scores["score"].sort_values(ascending=False).index
    for rank, ticker in enumerate(sorted_tickers, 1):
        row  = sub.loc[ticker] if ticker in sub.index else pd.Series(dtype=object)
        sc   = scores.loc[ticker]
        pc   = mom_rows.get(ticker, {})
        sent = sentiment_map.get(ticker, {})
        _live = _intraday_latest.get(ticker)
        _hist_rows = hist_by_ticker.get(ticker) or []
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
            "m1":  pc.get("m1"),
            "m3":  pc.get("m3"),
            "m6":  pc.get("m6"),
            "y1":  pc.get("y1"),
            "d1":  _d1,
        })
    return result


def _get_recommendations() -> list:
    global _reco_cache
    if time.time() - _reco_cache["ts"] > _RECO_TTL or not _reco_cache["data"]:
        print("[reco] Computing recommendation scores…")
        _reco_cache["data"] = _compute_recommendations()
        _reco_cache["ts"]   = time.time()
        print(f"[reco] Done — {len(_reco_cache['data'])} stocks scored")
    return _reco_cache["data"]


@app.get("/recommendations")
def recommendations_page():
    return render_template('recommendations.html')


@app.get("/api/recommendations")
@limiter.limit("30 per minute")
def recommendations_api():
    return jsonify(_get_recommendations())


@app.get("/api/sentiment/status")
def sentiment_status_api():
    return jsonify(sentiment_status(get_engine()))


@app.post("/api/sentiment/run")
@limiter.limit("5 per minute")
@require_auth
def sentiment_run_api():
    """Trigger an on-demand sentiment pass for current top/bottom 20 tickers."""
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
        _reco_cache["ts"] = 0.0
    run_sentiment_pass(target, get_engine(), on_complete=_invalidate)
    return jsonify({"ok": True, "message": f"Sentiment pass started for {len(target)} tickers.", "tickers": target})


@app.get("/api/sentiment/test/<ticker>")
@limiter.limit("10 per minute")
def sentiment_test_api(ticker):
    ticker = ticker.upper()
    if not _VALID_TICKER.match(ticker):
        return jsonify({"error": "Invalid ticker"}), 400
    """Diagnostic: raw connectivity probe for StockTwits and Yahoo Finance RSS."""
    import requests as _req
    results = {}

    # ── StockTwits ────────────────────────────────────────────────────────────
    st_url = f"https://api.stocktwits.com/api/2/streams/symbol/{ticker.upper()}.json"
    for label, verify in [("stocktwits_no_verify", False), ("stocktwits_verify", True)]:
        try:
            r = _req.get(st_url, params={"limit": 5}, timeout=10, verify=verify,
                         headers={"User-Agent": "Mozilla/5.0"})
            body = r.text[:300]
            results[label] = {"status": r.status_code, "body_len": len(r.text), "body_preview": body}
        except Exception as e:
            results[label] = {"error": str(e), "type": type(e).__name__}

    # ── Yahoo Finance RSS (known to work via proxy) ───────────────────────────
    yf_url = f"https://finance.yahoo.com/rss/headline?s={ticker.upper()}"
    try:
        r = _req.get(yf_url, timeout=10, verify=False,
                     headers={"User-Agent": "Mozilla/5.0"})
        results["yahoo_rss"] = {"status": r.status_code, "body_len": len(r.text),
                                 "body_preview": r.text[:200]}
    except Exception as e:
        results["yahoo_rss"] = {"error": str(e)}

    return jsonify(results)


@app.delete("/api/recommendations")
@require_auth
def recommendations_bust():
    _reco_cache["ts"] = 0.0
    return jsonify({"ok": True})










try:
    create_guru_rules_table()
    _db_rules = _load_guru_rules_from_db()
    if not _db_rules:
        print("Seeding guru rules to DB...")
        _save_guru_rules_to_db()
        _db_rules = _load_guru_rules_from_db()
    for _slug, _rules in _db_rules.items():
        if _slug in GURUS:
            GURUS[_slug]["rules"] = _rules
except Exception as _e:
    print(f"Could not initialize guru rules DB: {_e}")

if __name__ == "__main__":
    app.run(debug=True, port=5050, threaded=True, use_reloader=False)
