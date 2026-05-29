"""S&P 500 info viewer — Flask backend."""
import math
import re
import time
import requests
import urllib3
import xml.etree.ElementTree as ET
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)  # SEC EDGAR, local dev only
import numpy as np
import pandas as pd
from flask import Flask, jsonify, render_template_string, request
from db_config import get_engine, create_guru_tables, create_guru_rules_table, migrate_guru_tables, create_intraday_table, create_screener_filters_table, create_vol_trades_table, create_sentiment_table
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

# ── info — load from MySQL, fall back to CSV if DB unavailable ────────────────
def _load_info() -> pd.DataFrame:
    engine = get_engine()
    frame = pd.read_sql("SELECT * FROM sp500_info", engine)
    if "ticker" not in frame.columns:
        raise ValueError("sp500_info: ticker column missing")
    frame = frame.set_index("ticker")
    frame.index.name = "ticker"
    print(f"Loaded {len(frame)} tickers from MySQL")
    return frame

df = _load_info()

# ── history (grouped by ticker for O(1) lookup) ───────────────────────────────
def _load_history() -> dict:
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

_last_refresh: dict = {"ts": None, "count": 0, "status": "pending"}
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
        _hist_backfill_status.update({"state": "error", "msg": str(e)})
        return

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
                                      "count": updated, "status": "open"})
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

_refresh_thread = threading.Thread(target=_price_refresh_loop, daemon=True, name="price-refresh")
_refresh_thread.start()

_backfill_thread = threading.Thread(target=_intraday_backfill, daemon=True, name="intraday-backfill")
_backfill_thread.start()

_hist_backfill_thread = threading.Thread(target=_history_backfill_2y, daemon=True, name="hist-backfill-2y")
_hist_backfill_thread.start()

try:
    create_guru_tables()
    migrate_guru_tables()
    create_intraday_table()
    create_screener_filters_table()
    create_vol_trades_table()
    create_sentiment_table()
except Exception as _e:
    print(f"Could not create/migrate guru tables: {_e}")


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
    return render_template_string(HTML)


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


@app.get("/api/history/<ticker>")
def history_data(ticker):
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
    return render_template_string(SCREENER_HTML)


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
    return render_template_string(LEADERBOARD_HTML)


@app.get("/gurus")
def gurus_page():
    return render_template_string(GURUS_HTML)


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
    return render_template_string(STOCK_LB_HTML)


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
    return render_template_string(RECO_HTML)


@app.get("/api/recommendations")
def recommendations_api():
    return jsonify(_get_recommendations())


@app.get("/api/sentiment/status")
def sentiment_status_api():
    return jsonify(sentiment_status(get_engine()))


@app.post("/api/sentiment/run")
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
def sentiment_test_api(ticker):
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
def recommendations_bust():
    _reco_cache["ts"] = 0.0
    return jsonify({"ok": True})


HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>S&P 500 Viewer</title>
<link rel="stylesheet" href="https://cdn.datatables.net/1.13.8/css/jquery.dataTables.min.css">
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: system-ui, sans-serif; background: #0f1117; color: #e2e8f0; min-height: 100vh; }
  header { background: #1a1d27; border-bottom: 1px solid #2d3148; padding: 12px 24px; display: flex; align-items: center; gap: 16px; }
  header h1 { font-size: 1.25rem; font-weight: 700; color: #f8fafc; }
  header span { font-size: 0.8rem; color: #94a3b8; background: #2d3148; padding: 2px 8px; border-radius: 99px; }
  .nav-links { display: flex; gap: 4px; margin-left: 8px; }
  .nav-link { font-size: 0.82rem; padding: 5px 14px; border-radius: 6px; text-decoration: none; color: #94a3b8; transition: background .15s, color .15s; }
  .nav-link:hover { background: #2d3148; color: #e2e8f0; }
  .nav-link.active { background: #3730a3; color: #fff; font-weight: 600; }
  #filters { padding: 16px 24px; display: flex; gap: 12px; flex-wrap: wrap; background: #0f1117; border-bottom: 1px solid #1e2235; }
  #filters select, #filters input { background: #1a1d27; border: 1px solid #2d3148; color: #e2e8f0; padding: 6px 12px; border-radius: 6px; font-size: 0.85rem; }
  #filters input { flex: 1; min-width: 200px; }
  #main { padding: 0 24px 24px; overflow-x: auto; }
  table.dataTable { border-collapse: collapse; width: 100% !important; }
  table.dataTable thead th { background: #1a1d27; color: #94a3b8; font-size: 0.75rem; text-transform: uppercase; letter-spacing: .05em; border-bottom: 1px solid #2d3148; padding: 10px 12px; cursor: pointer; }
  table.dataTable tbody tr { border-bottom: 1px solid #1e2235; cursor: pointer; transition: background .1s; }
  table.dataTable tbody tr:hover { background: #1a1d27; }
  table.dataTable tbody td { padding: 9px 12px; font-size: 0.82rem; white-space: nowrap; }
  .ticker-badge { font-weight: 700; color: #818cf8; font-family: monospace; font-size: 0.9rem; text-decoration: underline dotted; text-underline-offset: 3px; }
  .ticker-badge:hover { color: #a5b4fc; }
  .sector-badge { background: #1e2235; color: #94a3b8; padding: 2px 7px; border-radius: 4px; font-size: 0.75rem; }
  .rec-strong-buy { color: #059669; font-weight: 700; }
  .rec-buy { color: #34d399; font-weight: 600; }
  .rec-hold { color: #fbbf24; font-weight: 600; }
  .rec-sell { color: #f87171; font-weight: 600; }
  .rec-none { color: #64748b; font-weight: 400; }
  .num { text-align: right; }

  /* ── shared overlay ── */
  .overlay { display: none; position: fixed; inset: 0; background: rgba(0,0,0,.55); z-index: 99; }
  .overlay.active { display: block; }

  /* ── company detail panel ── */
  #panel { position: fixed; top: 0; right: -520px; width: 520px; height: 100vh; background: #1a1d27; border-left: 1px solid #2d3148; overflow-y: auto; transition: right .25s ease; z-index: 100; display: flex; flex-direction: column; }
  #panel.open { right: 0; }
  #panel-header { padding: 20px 24px; border-bottom: 1px solid #2d3148; display: flex; justify-content: space-between; align-items: flex-start; }
  #panel-header h2 { font-size: 1.1rem; color: #f8fafc; }
  #panel-header .meta { font-size: 0.78rem; color: #64748b; margin-top: 2px; }
  #panel-close { background: none; border: none; color: #64748b; font-size: 1.4rem; cursor: pointer; padding: 0 4px; line-height: 1; }
  #panel-close:hover { color: #e2e8f0; }
  #panel-body { padding: 0 24px 24px; flex: 1; }
  .section { margin-top: 16px; }
  .section h3 {
    font-size: 0.72rem; text-transform: uppercase; letter-spacing: .08em;
    color: #818cf8; padding: 7px 0; border-bottom: 1px solid #2d3148;
    cursor: pointer; user-select: none; display: flex; align-items: center; gap: 6px;
  }
  .section h3:hover { color: #a5b4fc; }
  .section h3::before {
    content: '▶'; font-size: 0.55rem; display: inline-block;
    transition: transform 0.18s; flex-shrink: 0;
  }
  .section.open h3::before { transform: rotate(90deg); }
  .section h3 .field-count { margin-left: auto; font-size: 0.65rem; color: #4f5b8a; font-weight: 400; text-transform: none; letter-spacing: 0; }
  .field-grid { display: none; grid-template-columns: 1fr 1fr; gap: 6px 16px; margin-top: 10px; }
  .section.open .field-grid { display: grid; }
  .field { display: flex; flex-direction: column; gap: 1px; padding: 5px 0; }
  .field.wide { grid-column: 1 / -1; }
  .field label { font-size: 0.68rem; color: #64748b; text-transform: uppercase; letter-spacing: .04em; }
  .field span { font-size: 0.82rem; color: #e2e8f0; word-break: break-word; white-space: normal; }
  .summary-text { font-size: 0.78rem; color: #94a3b8; line-height: 1.5; max-height: 120px; overflow-y: auto; }

  /* ── history popup ── */
  #hist-overlay { display: none; position: fixed; inset: 0; background: rgba(0,0,0,.65); z-index: 199; }
  #hist-overlay.active { display: block; }
  #hist-modal {
    display: none; position: fixed; top: 50%; left: 50%; transform: translate(-50%, -50%);
    width: min(1280px, 96vw); max-height: 90vh; background: #1a1d27; border: 1px solid #2d3148;
    border-radius: 12px; z-index: 200; overflow: hidden; flex-direction: column;
    box-shadow: 0 24px 64px rgba(0,0,0,.6);
  }
  #hist-modal.active { display: flex; }
  #hist-header { padding: 18px 20px 14px; border-bottom: 1px solid #2d3148; display: flex; justify-content: space-between; align-items: flex-start; flex-shrink: 0; }
  #hist-title { font-size: 1.1rem; font-weight: 700; color: #f8fafc; }
  #hist-subtitle { font-size: 0.75rem; color: #64748b; margin-top: 3px; }
  .interval-btns { display:flex; gap:4px; align-items:center; }
  .interval-btn { background:#1a1d27; border:1px solid #2d3148; color:#94a3b8; padding:4px 12px; border-radius:6px; font-size:0.75rem; cursor:pointer; }
  .interval-btn.active,.interval-btn:hover { background:#6366f1; border-color:#6366f1; color:#fff; }
  #hist-close { background: none; border: none; color: #64748b; font-size: 1.4rem; cursor: pointer; padding: 0 4px; line-height: 1; flex-shrink: 0; }
  #hist-close:hover { color: #e2e8f0; }
  #hist-stats { display: flex; gap: 0; border-bottom: 1px solid #2d3148; flex-shrink: 0; }
  .hstat { flex: 1; padding: 10px 16px; border-right: 1px solid #2d3148; }
  .hstat:last-child { border-right: none; }
  .hstat label { font-size: 0.65rem; text-transform: uppercase; letter-spacing: .06em; color: #64748b; display: block; margin-bottom: 3px; }
  .hstat span { font-size: 0.95rem; font-weight: 600; color: #f8fafc; }
  .hstat span.up { color: #34d399; }
  .hstat span.down { color: #f87171; }
  /* two-column body */
  #hist-body { display: flex; flex: 1; min-height: 0; }
  #hist-charts { flex: 1; min-width: 0; display: flex; flex-direction: column; }
  #price-chart { flex: 1; min-height: 0; }
  #vol-chart   { height: 110px; flex-shrink: 0; border-top: 1px solid #2d3148; }
  #hist-loading { padding: 60px; text-align: center; color: #64748b; font-size: 0.85rem; }
  /* right company panel */
  #hist-company-panel { width: 320px; flex-shrink: 0; border-left: 1px solid #2d3148; overflow-y: auto; display: flex; flex-direction: column; }
  #hist-company-divider { padding: 10px 16px 6px; font-size: 0.65rem; text-transform: uppercase; letter-spacing: .1em; color: #4f5b8a; border-bottom: 1px solid #2d3148; flex-shrink: 0; }
  #hist-company { padding: 0 16px 16px; }

  /* DataTables overrides */
  .dataTables_wrapper { color: #e2e8f0; }
  .dataTables_filter, .dataTables_length { display: none; }
  .dataTables_info { font-size: 0.78rem; color: #64748b; padding: 10px 0; }
  table.dataTable thead .sorting::after, table.dataTable thead .sorting_asc::after, table.dataTable thead .sorting_desc::after { color: #64748b; }
</style>
</head>
<body>
<header>
  <h1>S&amp;P 500 Viewer</h1>
  <nav class="nav-links">
    <a href="/" class="nav-link active">Table</a>
    <a href="/screener" class="nav-link">Screener</a>
    <a href="/gurus" class="nav-link">Guru Investing</a>
    <a href="/recommendations" class="nav-link">Recommendations</a>
  </nav>
  <span id="count-badge">Loading…</span>
</header>
<div id="filters">
  <input id="search" type="text" placeholder="Search ticker, name, sector…">
  <select id="sector-filter"><option value="">All Sectors</option></select>
  <select id="rec-filter">
    <option value="">All Recommendations</option>
    <option value="strong_buy">Strong Buy</option>
    <option value="buy">Buy</option>
    <option value="hold">Hold</option>
    <option value="sell">Sell</option>
    <option value="none">None</option>
  </select>
  <select id="beta-filter">
    <option value="">All Beta</option>
    <option value="lt0.5">Beta &lt; 0.5 (Very Low)</option>
    <option value="0.5-1">Beta 0.5 – 1.0 (Low)</option>
    <option value="1-1.5">Beta 1.0 – 1.5 (Moderate)</option>
    <option value="gt1.5">Beta &gt; 1.5 (High)</option>
  </select>
  <select id="rev-filter">
    <option value="">All Rev Growth</option>
    <option value="ltN">Declining (&lt; 0%)</option>
    <option value="0-5">Slow (0% – 5%)</option>
    <option value="5-15">Moderate (5% – 15%)</option>
    <option value="15-30">Strong (15% – 30%)</option>
    <option value="gt30">High (&gt; 30%)</option>
  </select>
  <select id="return-filter">
    <option value="">All 1Y Return</option>
    <option value="ltN">Negative (&lt; 0%)</option>
    <option value="0-20">Modest (0% – 20%)</option>
    <option value="20-50">Strong (20% – 50%)</option>
    <option value="50-100">Very Strong (50% – 100%)</option>
    <option value="gt100">Exceptional (&gt; 100%)</option>
  </select>
</div>
<div id="main">
  <table id="tbl" class="dataTable" style="display:none">
    <thead><tr>
      <th>Ticker</th><th>Name</th><th>Sector</th>
      <th class="num">Price</th><th class="num">Mkt Cap</th>
      <th class="num">Fwd P/E</th><th class="num">Div Yield</th>
      <th class="num">Beta</th><th class="num">ROE</th>
      <th class="num">Rev Growth</th><th>Rec</th>
    </tr></thead>
    <tbody></tbody>
  </table>
</div>

<!-- company detail side panel -->
<div class="overlay" id="overlay"></div>
<div id="panel">
  <div id="panel-header">
    <div>
      <h2 id="panel-title">—</h2>
      <div class="meta" id="panel-meta"></div>
    </div>
    <button id="panel-close">&#x2715;</button>
  </div>
  <div id="panel-body"></div>
</div>

<!-- history popup -->
<div id="hist-overlay"></div>
<div id="hist-modal">
  <div id="hist-header">
    <div>
      <h2 id="hist-title"></h2>
      <div id="hist-subtitle">1-Year Price History — click ticker symbol in any row</div>
    </div>
    <div class="interval-btns">
      <button class="interval-btn active" data-interval="1d">Daily</button>
      <button class="interval-btn" data-interval="5m">5 Min</button>
    </div>
    <button id="hist-close">&#x2715;</button>
  </div>
  <div id="hist-stats">
    <div class="hstat"><label>1Y High</label><span id="hs-high">—</span></div>
    <div class="hstat"><label>1Y Low</label><span id="hs-low">—</span></div>
    <div class="hstat"><label>1Y Return</label><span id="hs-ret">—</span></div>
    <div class="hstat"><label>Avg Volume</label><span id="hs-vol">—</span></div>
    <div class="hstat"><label>Last Volume</label><span id="hs-lastvol">—</span></div>
    <div class="hstat"><label>Last Close</label><span id="hs-last">—</span></div>
  </div>
  <div id="hist-body">
    <div id="hist-charts">
      <div id="hist-loading">Loading chart…</div>
      <div id="price-chart"></div>
      <div id="vol-chart" style="display:none"></div>
    </div>
    <div id="hist-company-panel">
      <div id="hist-company-divider">Company Details</div>
      <div id="hist-company"></div>
    </div>
  </div>
</div>

<script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
<script src="https://cdn.datatables.net/1.13.8/js/jquery.dataTables.min.js"></script>
<script src="https://unpkg.com/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js"></script>
<script>
const fmt = {
  pct: v => v == null ? '—' : (v * 100).toFixed(1) + '%',
  num: (v, d=2) => v == null ? '—' : Number(v).toLocaleString(undefined, {minimumFractionDigits:d, maximumFractionDigits:d}),
  cap: v => {
    if (v == null) return '—';
    if (v >= 1e12) return '$' + (v/1e12).toFixed(2) + 'T';
    if (v >= 1e9)  return '$' + (v/1e9).toFixed(2)  + 'B';
    if (v >= 1e6)  return '$' + (v/1e6).toFixed(1)   + 'M';
    return '$' + v.toLocaleString();
  },
  price: v => v == null ? '—' : '$' + Number(v).toFixed(2),
  rec: v => {
    if (!v || v === 'none') return '<span class="rec-none">None</span>';
    const labels = { strong_buy: 'Strong Buy', buy: 'Buy', hold: 'Hold', sell: 'Sell', strong_sell: 'Strong Sell' };
    const classes = { strong_buy: 'rec-strong-buy', buy: 'rec-buy', hold: 'rec-hold', sell: 'rec-sell', strong_sell: 'rec-sell' };
    const cls = classes[v] || 'rec-hold';
    const label = labels[v] || v.replace(/_/g, ' ').replace(/\b[a-z]/g, c => c.toUpperCase());
    return `<span class="${cls}">${label}</span>`;
  },
};

let dt, allData = [], sectors = new Set();

fetch('/api/table').then(r => r.json()).then(data => {
  allData = data;
  data.forEach(r => r.sector && sectors.add(r.sector));
  const sel = document.getElementById('sector-filter');
  [...sectors].sort().forEach(s => sel.insertAdjacentHTML('beforeend', `<option value="${s}">${s}</option>`));

  const tbody = document.querySelector('#tbl tbody');
  data.forEach(r => {
    const tr = document.createElement('tr');
    tr.dataset.ticker = r.ticker;
    const n = v => v ?? -Infinity;
    tr.innerHTML = `
      <td class="ticker-badge" title="Click for price history">${r.ticker}</td>
      <td>${r.longName || '—'}</td>
      <td><span class="sector-badge">${r.sector || '—'}</span></td>
      <td class="num" data-order="${n(r.currentPrice)}">${fmt.price(r.currentPrice)}</td>
      <td class="num" data-order="${n(r.marketCap)}">${fmt.cap(r.marketCap)}</td>
      <td class="num" data-order="${n(r.forwardPE)}">${r.forwardPE != null ? fmt.num(r.forwardPE, 1) : '—'}</td>
      <td class="num" data-order="${n(r.dividendYield)}">${r.dividendYield != null ? fmt.pct(r.dividendYield) : '—'}</td>
      <td class="num" data-order="${n(r.beta)}">${r.beta != null ? fmt.num(r.beta, 2) : '—'}</td>
      <td class="num" data-order="${n(r.returnOnEquity)}">${r.returnOnEquity != null ? fmt.pct(r.returnOnEquity) : '—'}</td>
      <td class="num" data-order="${n(r.revenueGrowth)}">${r.revenueGrowth != null ? fmt.pct(r.revenueGrowth) : '—'}</td>
      <td>${fmt.rec(r.recommendationKey)}</td>`;

    // ticker cell → history + company popup
    tr.querySelector('td').addEventListener('click', e => {
      e.stopPropagation();
      openHistory(r.ticker, r.longName);
    });
    tbody.appendChild(tr);
  });

  document.getElementById('tbl').style.display = '';
  dt = $('#tbl').DataTable({ paging: false, order: [[4, 'desc']], dom: 'ti', columnDefs: [{orderable:false, targets:[2,10]}] });
  document.getElementById('count-badge').textContent = data.length + ' companies';

  document.getElementById('search').addEventListener('input', applyFilters);
  document.getElementById('sector-filter').addEventListener('change', applyFilters);
  document.getElementById('rec-filter').addEventListener('change', applyFilters);
  document.getElementById('beta-filter').addEventListener('change', applyFilters);
  document.getElementById('rev-filter').addEventListener('change', applyFilters);
  document.getElementById('return-filter').addEventListener('change', applyFilters);
});

function inRange(val, range) {
  if (!range || val == null) return true;
  if (range === 'ltN')   return val < 0;
  if (range.startsWith('lt')) return val < parseFloat(range.slice(2));
  if (range.startsWith('gt')) return val > parseFloat(range.slice(2));
  const [lo, hi] = range.split('-').map(Number);
  return val >= lo && val < hi;
}

function applyFilters() {
  const q    = document.getElementById('search').value.toLowerCase();
  const sec  = document.getElementById('sector-filter').value;
  const rec  = document.getElementById('rec-filter').value;
  const beta = document.getElementById('beta-filter').value;
  const rev  = document.getElementById('rev-filter').value;
  const ret  = document.getElementById('return-filter').value;
  dt.rows().every(function() {
    const r = allData.find(d => d.ticker === this.node().dataset.ticker);
    const match =
      (!q || r.ticker.toLowerCase().includes(q) || (r.longName||'').toLowerCase().includes(q) || (r.sector||'').toLowerCase().includes(q) || (r.industry||'').toLowerCase().includes(q)) &&
      (!sec  || r.sector === sec) &&
      (!rec  || r.recommendationKey === rec) &&
      inRange(r.beta, beta) &&
      inRange(r.revenueGrowth != null ? r.revenueGrowth * 100 : null, rev) &&
      inRange(r['52WeekChange'] != null ? r['52WeekChange'] * 100 : null, ret);
    match ? $(this.node()).show() : $(this.node()).hide();
  });
  dt.draw();
}

// ── company detail panel ───────────────────────────────────────────────────
function openPanel(ticker) {
  // Open immediately so the slide animation is visible while data loads
  const body = document.getElementById('panel-body');
  document.getElementById('panel-title').textContent = ticker;
  document.getElementById('panel-meta').textContent = 'Loading…';
  body.innerHTML = '<div style="padding:24px;color:#64748b;font-size:0.85rem">Loading…</div>';
  document.getElementById('panel').classList.add('open');
  document.getElementById('overlay').classList.add('active');

  fetch(`/api/company/${ticker}`)
    .then(r => { if (!r.ok) throw new Error('Server error ' + r.status); return r.json(); })
    .then(data => {
      const profile = data.sections['Company Profile & Address'] || {};
      document.getElementById('panel-title').textContent = profile.longName || ticker;
      document.getElementById('panel-meta').textContent = `${ticker} · ${profile.city || ''}, ${profile.country || ''}`;
      body.innerHTML = '';
      let firstOpen = true;
      for (const [section, fields] of Object.entries(data.sections)) {
        const populated = Object.entries(fields).filter(([, v]) => v != null);
        if (!populated.length) continue;
        const div = document.createElement('div');
        div.className = 'section' + (firstOpen ? ' open' : '');
        firstOpen = false;
        div.innerHTML = `<h3>${section}<span class="field-count">${populated.length} fields</span></h3><div class="field-grid"></div>`;
        div.querySelector('h3').addEventListener('click', () => div.classList.toggle('open'));
        const grid = div.querySelector('.field-grid');
        for (const [key, val] of populated) {
          const isSummary = key === 'longBusinessSummary';
          const f = document.createElement('div');
          f.className = 'field' + (isSummary ? ' wide' : '');
          const label = key.replace(/([A-Z])/g, ' $1').replace(/^./, s => s.toUpperCase());
          let displayVal;
          try { displayVal = isSummary ? `<div class="summary-text">${val}</div>` : `<span>${formatVal(key, val)}</span>`; }
          catch(e) { displayVal = `<span>${String(val)}</span>`; }
          f.innerHTML = `<label>${label}</label>${displayVal}`;
          grid.appendChild(f);
        }
        body.appendChild(div);
      }
    })
    .catch(err => {
      body.innerHTML = `<div style="padding:24px;color:#f87171;font-size:0.85rem">Error: ${err.message}</div>`;
    });
}

function closePanel() {
  document.getElementById('panel').classList.remove('open');
  document.getElementById('overlay').classList.remove('active');
}
document.getElementById('panel-close').addEventListener('click', closePanel);
document.getElementById('overlay').addEventListener('click', closePanel);

function formatVal(key, val) {
  if (typeof val === 'boolean') return val ? 'Yes' : 'No';
  if (typeof val === 'string') return val;
  const pctKeys = ['dividendYield','payoutRatio','profitMargins','grossMargins','ebitdaMargins','operatingMargins','returnOnAssets','returnOnEquity','revenueGrowth','earningsGrowth','heldPercentInsiders','heldPercentInstitutions','shortPercentOfFloat','fiveYearAvgDividendYield','trailingAnnualDividendYield'];
  const capKeys = ['marketCap','enterpriseValue','totalRevenue','grossProfits','ebitda','netIncomeToCommon','totalCash','totalDebt','freeCashflow','operatingCashflow'];
  const priceKeys = ['currentPrice','previousClose','open','dayLow','dayHigh','fiftyTwoWeekLow','fiftyTwoWeekHigh','bid','ask','fiftyDayAverage','twoHundredDayAverage','targetHighPrice','targetLowPrice','targetMeanPrice','targetMedianPrice','dividendRate','trailingAnnualDividendRate','bookValue','totalCashPerShare','revenuePerShare','trailingEps','forwardEps'];
  if (pctKeys.includes(key)) return fmt.pct(val);
  if (capKeys.includes(key)) return fmt.cap(val);
  if (priceKeys.includes(key)) return fmt.price(val);
  if (typeof val === 'number') return Number.isInteger(val) ? val.toLocaleString() : fmt.num(val, 2);
  return String(val);
}

// ── history popup ──────────────────────────────────────────────────────────
let priceChart = null, volChart = null, _histTicker = null, _histName = null;

function openHistory(ticker, name) {
  _histTicker = ticker; _histName = name || '';
  document.getElementById('hist-title').textContent = `${ticker}  —  ${_histName}`;
  document.getElementById('hist-loading').style.display = 'block';
  document.getElementById('price-chart').style.cssText = 'display:none';
  document.getElementById('vol-chart').style.display = 'none';
  ['hs-high','hs-low','hs-ret','hs-vol','hs-lastvol','hs-last'].forEach(id => document.getElementById(id).textContent = '…');
  document.getElementById('hist-company-divider').style.display = 'none';
  document.getElementById('hist-company').innerHTML = '';
  document.querySelectorAll('.interval-btn').forEach(b => b.classList.remove('active'));
  document.querySelector('.interval-btn[data-interval="1d"]').classList.add('active');
  document.getElementById('hist-modal').classList.add('active');
  document.getElementById('hist-overlay').classList.add('active');

  fetch(`/api/company/${ticker}`)
    .then(r => r.json())
    .then(data => {
      const container = document.getElementById('hist-company');
      container.innerHTML = '';
      for (const [section, fields] of Object.entries(data.sections)) {
        const populated = Object.entries(fields).filter(([, v]) => v != null);
        if (!populated.length) continue;
        const div = document.createElement('div');
        div.className = 'section';
        div.innerHTML = `<h3>${section}<span class="field-count">${populated.length} fields</span></h3><div class="field-grid"></div>`;
        div.querySelector('h3').addEventListener('click', () => div.classList.toggle('open'));
        const grid = div.querySelector('.field-grid');
        for (const [key, val] of populated) {
          const isSummary = key === 'longBusinessSummary';
          const f = document.createElement('div');
          f.className = 'field' + (isSummary ? ' wide' : '');
          const label = key.replace(/([A-Z])/g, ' $1').replace(/^./, s => s.toUpperCase());
          let displayVal;
          try { displayVal = isSummary ? `<div class="summary-text">${val}</div>` : `<span>${formatVal(key, val)}</span>`; }
          catch(e) { displayVal = `<span>${String(val)}</span>`; }
          f.innerHTML = `<label>${label}</label>${displayVal}`;
          grid.appendChild(f);
        }
        container.appendChild(div);
      }
      document.getElementById('hist-company-divider').style.display = 'block';
    })
    .catch(() => {});

  _loadHistChart(ticker, '1d');
}

function _loadHistChart(ticker, interval) {
  document.getElementById('hist-loading').style.display = 'block';
  document.getElementById('price-chart').style.cssText = 'display:none';
  document.getElementById('vol-chart').style.display = 'none';
  fetch(`/api/history/${ticker}?interval=${interval}`)
    .then(r => r.json())
    .then(rows => _renderHistChart(rows, interval))
    .catch(() => { document.getElementById('hist-loading').style.display = 'none'; });
}

function _renderHistChart(rows, interval) {
  document.getElementById('hist-loading').style.display = 'none';
  document.getElementById('price-chart').style.cssText = '';
  document.getElementById('vol-chart').style.display = 'block';

  if (interval !== '5m') {
    const closes = rows.map(r => r.close);
    const high = Math.max(...rows.map(r => r.high));
    const low  = Math.min(...rows.map(r => r.low));
    const ret  = (closes.at(-1) - closes[0]) / closes[0];
    const avgVol = rows.reduce((s, r) => s + r.volume, 0) / rows.length;
    document.getElementById('hs-high').textContent = '$' + high.toFixed(2);
    document.getElementById('hs-low').textContent  = '$' + low.toFixed(2);
    const retEl = document.getElementById('hs-ret');
    retEl.textContent = (ret >= 0 ? '+' : '') + (ret * 100).toFixed(1) + '%';
    retEl.className = ret >= 0 ? 'up' : 'down';
    document.getElementById('hs-vol').textContent     = fmt.cap(avgVol).replace('$','');
    document.getElementById('hs-lastvol').textContent = fmt.cap(rows.at(-1).volume).replace('$','');
    document.getElementById('hs-last').textContent    = '$' + closes.at(-1).toFixed(2);
  } else {
    ['hs-high','hs-low','hs-ret','hs-vol','hs-lastvol','hs-last'].forEach(id => document.getElementById(id).textContent = '—');
  }

  if (priceChart) { priceChart.remove(); priceChart = null; }
  if (volChart)   { volChart.remove();   volChart = null; }

  const chartOpts = {
    layout: { background: { color: '#1a1d27' }, textColor: '#94a3b8' },
    grid: { vertLines: { color: '#1e2235' }, horzLines: { color: '#1e2235' } },
    crosshair: { mode: 1 },
    rightPriceScale: { borderColor: '#2d3148' },
    timeScale: { borderColor: '#2d3148', timeVisible: true },
    handleScroll: true, handleScale: true,
  };

  const pEl = document.getElementById('price-chart');
  priceChart = LightweightCharts.createChart(pEl, { ...chartOpts, autoSize: true });
  const candles = priceChart.addCandlestickSeries({
    upColor: '#34d399', downColor: '#f87171',
    borderUpColor: '#34d399', borderDownColor: '#f87171',
    wickUpColor: '#34d399', wickDownColor: '#f87171',
  });
  candles.setData(rows.map(r => ({ time: r.time, open: r.open, high: r.high, low: r.low, close: r.close })));
  priceChart.timeScale().fitContent();

  const vEl = document.getElementById('vol-chart');
  volChart = LightweightCharts.createChart(vEl, { ...chartOpts, autoSize: true });
  const volSeries = volChart.addHistogramSeries({
    color: '#4f5b8a', priceFormat: { type: 'volume' },
    priceScaleId: 'vol', scaleMargins: { top: 0.1, bottom: 0 },
  });
  volSeries.setData(rows.map((r, i) => ({
    time: r.time, value: r.volume,
    color: i > 0 && r.close >= rows[i-1].close ? '#34d39966' : '#f8717166',
  })));
  volChart.timeScale().fitContent();

  priceChart.timeScale().subscribeVisibleLogicalRangeChange(range => {
    if (range) volChart.timeScale().setVisibleLogicalRange(range);
  });
  volChart.timeScale().subscribeVisibleLogicalRangeChange(range => {
    if (range) priceChart.timeScale().setVisibleLogicalRange(range);
  });
}

document.querySelectorAll('.interval-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.interval-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    if (_histTicker) _loadHistChart(_histTicker, btn.dataset.interval);
  });
});

function closeHistory() {
  document.getElementById('hist-modal').classList.remove('active');
  document.getElementById('hist-overlay').classList.remove('active');
}
document.getElementById('hist-close').addEventListener('click', closeHistory);
document.getElementById('hist-overlay').addEventListener('click', closeHistory);
</script>
</body>
</html>"""

SCREENER_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>S&P 500 Screener</title>
<link rel="stylesheet" href="https://cdn.datatables.net/1.13.8/css/jquery.dataTables.min.css">
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: system-ui, sans-serif; background: #0f1117; color: #e2e8f0; height: 100vh; display: flex; flex-direction: column; overflow: hidden; }
  header { background: #1a1d27; border-bottom: 1px solid #2d3148; padding: 12px 24px; display: flex; align-items: center; gap: 16px; flex-shrink: 0; }
  header h1 { font-size: 1.25rem; font-weight: 700; color: #f8fafc; }
  .nav-links { display: flex; gap: 4px; margin-left: 8px; }
  .nav-link { font-size: 0.82rem; padding: 5px 14px; border-radius: 6px; text-decoration: none; color: #94a3b8; transition: background .15s, color .15s; }
  .nav-link:hover { background: #2d3148; color: #e2e8f0; }
  .nav-link.active { background: #3730a3; color: #fff; font-weight: 600; }

  /* layout */
  #scr-wrap { display: flex; flex: 1; min-height: 0; }
  #scr-filters { width: 360px; flex-shrink: 0; overflow-y: auto; border-right: 1px solid #2d3148; background: #0d0f18; display: flex; flex-direction: column; }
  #scr-controls { padding: 12px 16px; border-bottom: 1px solid #2d3148; display: flex; align-items: center; gap: 8px; flex-shrink: 0; }
  #scr-apply { background: #3730a3; color: #fff; border: none; padding: 6px 16px; border-radius: 6px; font-size: 0.82rem; font-weight: 600; cursor: pointer; }
  #scr-apply:hover { background: #4338ca; }
  #scr-reset { background: #1e2235; color: #94a3b8; border: 1px solid #2d3148; padding: 6px 12px; border-radius: 6px; font-size: 0.82rem; cursor: pointer; }
  #scr-reset:hover { color: #e2e8f0; }
  #scr-match { font-size: 0.78rem; color: #64748b; margin-left: auto; }
  #scr-groups { flex: 1; padding: 8px 0 16px; }

  /* parameter search */
  #param-search-wrap { padding: 10px 16px 8px; border-bottom: 1px solid #2d3148; flex-shrink: 0; position: relative; }
  #param-search { width: 100%; background: #1a1d27; border: 1px solid #2d3148; color: #e2e8f0; padding: 7px 32px 7px 28px; border-radius: 6px; font-size: 0.82rem; outline: none; }
  #param-search:focus { border-color: #818cf8; }
  #param-search::placeholder { color: #4f5b8a; }
  #param-search-icon { position: absolute; left: 24px; top: 50%; transform: translateY(-50%); color: #4f5b8a; font-size: 0.82rem; pointer-events: none; }
  #param-search-clear { position: absolute; right: 24px; top: 50%; transform: translateY(-50%); color: #64748b; font-size: 0.9rem; cursor: pointer; display: none; background: none; border: none; padding: 0 2px; line-height: 1; }
  #param-search-clear:hover { color: #e2e8f0; }
  #param-match-count { font-size: 0.72rem; color: #4f5b8a; padding: 4px 16px 0; display: none; }
  .fg-group.param-hidden { display: none; }
  .num-field.param-hidden { display: none; }
  .cat-field-wrapper.param-hidden { display: none; }
  .param-highlight { background: #3730a355; border-radius: 3px; color: #a5b4fc; font-weight: 600; }

  /* filter groups */
  .fg-group { border-bottom: 1px solid #1e2235; }
  .fg-header { padding: 9px 16px; cursor: pointer; user-select: none; display: flex; align-items: center; gap: 8px; font-size: 0.72rem; text-transform: uppercase; letter-spacing: .08em; color: #818cf8; }
  .fg-header:hover { color: #a5b4fc; }
  .fg-header::before { content: '▶'; font-size: 0.55rem; transition: transform .18s; flex-shrink: 0; }
  .fg-group.open .fg-header::before { transform: rotate(90deg); }
  .fg-body { display: none; padding: 4px 16px 12px; }
  .fg-group.open .fg-body { display: block; }

  /* categorical */
  .cat-search { width: 100%; background: #1a1d27; border: 1px solid #2d3148; color: #e2e8f0; padding: 5px 8px; border-radius: 5px; font-size: 0.78rem; margin-bottom: 6px; }
  .cat-list { max-height: 160px; overflow-y: auto; display: flex; flex-direction: column; gap: 3px; }
  .cat-item { display: flex; align-items: center; gap: 7px; font-size: 0.78rem; color: #cbd5e1; cursor: pointer; padding: 2px 0; }
  .cat-item input[type=checkbox] { accent-color: #818cf8; width: 13px; height: 13px; flex-shrink: 0; cursor: pointer; }

  /* numeric range */
  .num-field { margin-bottom: 10px; }
  .num-label { font-size: 0.75rem; color: #94a3b8; margin-bottom: 4px; display: flex; justify-content: space-between; }
  .num-range-hint { font-size: 0.68rem; color: #4f5b8a; }
  .num-inputs { display: flex; align-items: center; gap: 6px; }
  .num-inputs input { flex: 1; background: #1a1d27; border: 1px solid #2d3148; color: #e2e8f0; padding: 5px 7px; border-radius: 5px; font-size: 0.78rem; width: 0; min-width: 0; }
  .num-inputs input:focus { outline: none; border-color: #818cf8; }
  .num-inputs input.active { border-color: #818cf8; background: #1e2040; }
  .num-sep { color: #4f5b8a; font-size: 0.8rem; flex-shrink: 0; }

  /* results */
  #scr-results { flex: 1; overflow-y: auto; display: flex; flex-direction: column; }
  #scr-summary { padding: 8px 20px; border-bottom: 1px solid #1e2235; font-size: 0.8rem; color: #64748b; flex-shrink: 0; display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }
  #scr-summary strong { color: #818cf8; font-size: 1rem; }
  #portfolio-bar { display: none; padding: 8px 20px; border-bottom: 1px solid #1e2235; background: #0d0f18; flex-shrink: 0; display: flex; align-items: center; gap: 6px; flex-wrap: wrap; }
  #portfolio-bar.visible { display: flex; }
  .pb-label { font-size: 0.7rem; color: #64748b; text-transform: uppercase; letter-spacing: .06em; font-weight: 600; margin-right: 4px; }
  .pb-chip { display: inline-flex; align-items: center; gap: 5px; background: #1a1d27; border: 1px solid #2d3148; border-radius: 6px; padding: 4px 10px; font-size: 0.8rem; }
  .pb-period { font-size: 0.68rem; color: #64748b; }
  .pb-val { font-weight: 700; }
  .pb-val.up { color: #34d399; }
  .pb-val.down { color: #f87171; }
  .chg { font-size: 0.78rem; font-weight: 600; }
  .chg.up { color: #34d399; }
  .chg.down { color: #f87171; }
  .chg.flat { color: #64748b; }
  #scr-table-wrap { flex: 1; overflow-y: auto; padding: 0 20px 20px; }
  table.dataTable { border-collapse: collapse; width: 100% !important; }
  table.dataTable thead th { background: #1a1d27; color: #94a3b8; font-size: 0.72rem; text-transform: uppercase; letter-spacing: .05em; border-bottom: 1px solid #2d3148; padding: 10px 10px; cursor: pointer; white-space: nowrap; }
  table.dataTable tbody tr { border-bottom: 1px solid #1e2235; transition: background .1s; }
  table.dataTable tbody tr:hover { background: #1a1d27; }
  table.dataTable tbody td { padding: 8px 10px; font-size: 0.8rem; white-space: nowrap; }
  .tk { font-weight: 700; color: #818cf8; font-family: monospace; cursor: pointer; text-decoration: underline dotted; text-underline-offset: 3px; }
  .tk:hover { color: #a5b4fc; }
  .sb { background: #1e2235; color: #94a3b8; padding: 2px 6px; border-radius: 4px; font-size: 0.72rem; }
  .num { text-align: right; }
  .rec-strong-buy { color: #059669; font-weight: 700; }
  .rec-buy  { color: #34d399; font-weight: 600; }
  .rec-hold { color: #fbbf24; font-weight: 600; }
  .rec-sell { color: #f87171; font-weight: 600; }
  .rec-none { color: #64748b; }
  .dataTables_wrapper { color: #e2e8f0; }
  .dataTables_filter, .dataTables_length { display: none; }
  .dataTables_info { font-size: 0.75rem; color: #64748b; padding: 8px 0; }
  table.dataTable thead .sorting::after, table.dataTable thead .sorting_asc::after, table.dataTable thead .sorting_desc::after { color: #64748b; }

  /* chart popup — identical to main page */
  #hist-overlay { display: none; position: fixed; inset: 0; background: rgba(0,0,0,.65); z-index: 199; }
  #hist-overlay.active { display: block; }
  #hist-modal { display: none; position: fixed; top: 50%; left: 50%; transform: translate(-50%,-50%); width: min(1280px,96vw); max-height: 90vh; background: #1a1d27; border: 1px solid #2d3148; border-radius: 12px; z-index: 200; overflow: hidden; flex-direction: column; box-shadow: 0 24px 64px rgba(0,0,0,.6); }
  #hist-modal.active { display: flex; }
  #hist-header { padding: 18px 20px 14px; border-bottom: 1px solid #2d3148; display: flex; justify-content: space-between; align-items: flex-start; flex-shrink: 0; }
  #hist-title { font-size: 1.1rem; font-weight: 700; color: #f8fafc; }
  #hist-subtitle { font-size: 0.75rem; color: #64748b; margin-top: 3px; }
  .interval-btns { display:flex; gap:4px; align-items:center; }
  .interval-btn { background:#1a1d27; border:1px solid #2d3148; color:#94a3b8; padding:4px 12px; border-radius:6px; font-size:0.75rem; cursor:pointer; }
  .interval-btn.active,.interval-btn:hover { background:#6366f1; border-color:#6366f1; color:#fff; }
  #hist-close { background: none; border: none; color: #64748b; font-size: 1.4rem; cursor: pointer; padding: 0 4px; line-height: 1; flex-shrink: 0; }
  #hist-close:hover { color: #e2e8f0; }
  #hist-stats { display: flex; border-bottom: 1px solid #2d3148; flex-shrink: 0; }
  .hstat { flex: 1; padding: 10px 16px; border-right: 1px solid #2d3148; }
  .hstat:last-child { border-right: none; }
  .hstat label { font-size: 0.65rem; text-transform: uppercase; letter-spacing: .06em; color: #64748b; display: block; margin-bottom: 3px; }
  .hstat span { font-size: 0.95rem; font-weight: 600; color: #f8fafc; }
  .hstat span.up { color: #34d399; } .hstat span.down { color: #f87171; }
  #hist-body { display: flex; flex: 1; min-height: 0; }
  #hist-charts { flex: 1; min-width: 0; display: flex; flex-direction: column; }
  #price-chart { flex: 1; min-height: 0; }
  #vol-chart { height: 110px; flex-shrink: 0; border-top: 1px solid #2d3148; }
  #hist-loading { padding: 60px; text-align: center; color: #64748b; font-size: 0.85rem; }
  #hist-company-panel { width: 320px; flex-shrink: 0; border-left: 1px solid #2d3148; overflow-y: auto; display: flex; flex-direction: column; }
  #hist-company-divider { padding: 10px 16px 6px; font-size: 0.65rem; text-transform: uppercase; letter-spacing: .1em; color: #4f5b8a; border-bottom: 1px solid #2d3148; flex-shrink: 0; }
  #hist-company { padding: 0 16px 16px; }
  .section { margin-top: 16px; }
  .section h3 { font-size: 0.72rem; text-transform: uppercase; letter-spacing: .08em; color: #818cf8; padding: 7px 0; border-bottom: 1px solid #2d3148; cursor: pointer; user-select: none; display: flex; align-items: center; gap: 6px; }
  .section h3:hover { color: #a5b4fc; }
  .section h3::before { content: '▶'; font-size: 0.55rem; display: inline-block; transition: transform 0.18s; flex-shrink: 0; }
  .section.open h3::before { transform: rotate(90deg); }
  .section h3 .field-count { margin-left: auto; font-size: 0.65rem; color: #4f5b8a; font-weight: 400; text-transform: none; letter-spacing: 0; }
  .field-grid { display: none; grid-template-columns: 1fr 1fr; gap: 6px 16px; margin-top: 10px; }
  .section.open .field-grid { display: grid; }
  .field { display: flex; flex-direction: column; gap: 1px; padding: 5px 0; }
  .field.wide { grid-column: 1 / -1; }
  .field label { font-size: 0.68rem; color: #64748b; text-transform: uppercase; letter-spacing: .04em; }
  .field span { font-size: 0.82rem; color: #e2e8f0; word-break: break-word; white-space: normal; }
  .summary-text { font-size: 0.78rem; color: #94a3b8; line-height: 1.5; max-height: 120px; overflow-y: auto; }

  /* guru preset */
  #guru-preset-wrap { padding: 10px 16px 10px; border-bottom: 1px solid #2d3148; flex-shrink: 0; display: flex; align-items: center; gap: 10px; }
  #guru-preset-label { font-size: 0.7rem; color: #64748b; font-weight: 600; text-transform: uppercase; letter-spacing: .06em; white-space: nowrap; flex-shrink: 0; }
  #guru-preset { flex: 1; background: #1a1d27; border: 1px solid #2d3148; color: #e2e8f0; padding: 6px 10px; border-radius: 6px; font-size: 0.8rem; cursor: pointer; outline: none; min-width: 0; }
  #guru-preset:focus { border-color: #818cf8; }
  #guru-preset option, #guru-preset optgroup { background: #1a1d27; color: #e2e8f0; }
  #guru-preset-badge { display: none; align-items: center; gap: 6px; flex-shrink: 0; }
  #guru-preset-dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
  #guru-preset-name { font-size: 0.75rem; color: #a5b4fc; font-weight: 600; white-space: nowrap; }
  #guru-preset-clear { background: none; border: none; color: #64748b; font-size: 0.85rem; cursor: pointer; padding: 0 2px; line-height: 1; flex-shrink: 0; }
  #guru-preset-clear:hover { color: #e2e8f0; }
  /* custom saved filters */
  #custom-filter-wrap { padding: 8px 12px 10px; border-bottom: 1px solid #2d3148; display: flex; flex-direction: column; gap: 6px; }
  #custom-filter-label { font-size: 0.68rem; text-transform: uppercase; letter-spacing: .08em; color: #818cf8; font-weight: 600; }
  #custom-filter-select { width: 100%; background: #1a1d27; border: 1px solid #2d3148; color: #e2e8f0; padding: 5px 8px; border-radius: 6px; font-size: 0.8rem; }
  #custom-filter-select:focus { outline: none; border-color: #818cf8; }
  #custom-filter-name { width: 100%; background: #1a1d27; border: 1px solid #2d3148; color: #e2e8f0; padding: 5px 8px; border-radius: 6px; font-size: 0.8rem; }
  #custom-filter-name::placeholder { color: #4f5b8a; }
  #custom-filter-name:focus { outline: none; border-color: #818cf8; }
  #custom-filter-btns { display: flex; gap: 6px; }
  #cf-save-btn { flex: 1; background: #3730a3; color: #fff; border: none; padding: 5px 10px; border-radius: 6px; font-size: 0.78rem; font-weight: 600; cursor: pointer; }
  #cf-save-btn:hover { background: #4338ca; }
  #cf-delete-btn { background: #1e2235; color: #f87171; border: 1px solid #2d3148; padding: 5px 10px; border-radius: 6px; font-size: 0.78rem; cursor: pointer; }
  #cf-delete-btn:hover { background: #450a0a; border-color: #f87171; }
  /* page tabs */
  .page-tabs { display: flex; background: #0d0f18; border-bottom: 1px solid #2d3148; flex-shrink: 0; padding: 0 16px; }
  .page-tab { padding: 9px 20px; font-size: 0.82rem; color: #64748b; cursor: pointer; border-bottom: 2px solid transparent; user-select: none; }
  .page-tab:hover { color: #e2e8f0; }
  .page-tab.active { color: #818cf8; border-bottom-color: #818cf8; font-weight: 600; }
  /* volume panel */
  #vol-panel { flex: 1; min-height: 0; display: none; overflow-y: auto; padding: 16px 24px 24px; flex-direction: column; }
  /* vol trades panel */
  #vt-panel { flex: 1; min-height: 0; display: none; overflow-y: auto; padding: 16px 24px 24px; flex-direction: column; }
  #vt-toolbar { display: flex; align-items: center; justify-content: space-between; gap: 10px; margin-bottom: 14px; flex-wrap: wrap; }
  #vt-toolbar-right { display: flex; align-items: center; gap: 10px; }
  #vt-status  { font-size: 0.75rem; color: #475569; }
  #vt-refresh-btn { background: #1e2235; border: 1px solid #2d3148; color: #94a3b8; padding: 5px 14px; border-radius: 6px; font-size: 0.78rem; cursor: pointer; }
  #vt-refresh-btn:hover { color: #e2e8f0; }
  #vt-stats-bar { display: flex; gap: 10px; flex-wrap: wrap; }
  .vt-stat { background: #1e2235; border: 1px solid #2d3148; border-radius: 8px; padding: 8px 16px; display: flex; flex-direction: column; gap: 3px; min-width: 110px; }
  .vt-stat-label { font-size: 0.68rem; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; }
  .vt-stat-val { font-size: 0.9rem; font-weight: 700; }
  .vt-pos { color: #34d399; }
  .vt-neg { color: #f87171; }
  .vt-neu { color: #94a3b8; }
  .pnl-pos { color: #34d399; font-weight: 600; }
  .pnl-neg { color: #f87171; font-weight: 600; }
  .status-open { color: #fbbf24; font-weight: 600; font-size: 0.75rem; }
  .status-sold { color: #64748b; font-size: 0.75rem; }
  .vt-table { width: 100%; border-collapse: collapse; font-size: 0.82rem; }
  .vt-table thead th { background: #1a1f2e; color: #94a3b8; font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.04em; padding: 8px 12px; border-bottom: 1px solid #2d3148; text-align: left; white-space: nowrap; }
  .vt-table thead th.num { text-align: right; }
  .vt-table tbody tr { border-bottom: 1px solid #1e2235; }
  .vt-table tbody tr:hover { background: #1e2235; }
  .vt-table tbody td { padding: 7px 12px; color: #cbd5e1; }
  .vt-table tbody td.num { text-align: right; font-variant-numeric: tabular-nums; }
  #vol-toolbar { display: flex; align-items: center; gap: 10px; margin-bottom: 14px; flex-wrap: wrap; }
  #vol-status { font-size: 0.75rem; color: #475569; margin-left: auto; }
  #vol-refresh-btn { background: #1e2235; border: 1px solid #2d3148; color: #94a3b8; padding: 5px 14px; border-radius: 6px; font-size: 0.78rem; cursor: pointer; }
  #vol-refresh-btn:hover { color: #e2e8f0; }
  .vol-filter-btns { display: flex; gap: 6px; }
  .vol-fbtn { background: #1a1d27; border: 1px solid #2d3148; color: #94a3b8; padding: 5px 14px; border-radius: 6px; font-size: 0.78rem; cursor: pointer; }
  .vol-fbtn.active { background: #3730a3; border-color: #3730a3; color: #fff; font-weight: 600; }
  .vol-fbtn.bullish.active { background: #064e3b; border-color: #059669; color: #34d399; }
  .vol-fbtn.bearish.active { background: #450a0a; border-color: #dc2626; color: #f87171; }
  .vol-fbtn.neutral.active { background: #1e2040; border-color: #818cf8; color: #a5b4fc; }
  #vol-tbl-wrap { background: #131620; border: 1px solid #2d3148; border-radius: 10px; overflow: hidden; }
  .vol-abnormal-row { background: #1a0505 !important; }
  .vol-up-row { background: #051a0a !important; }
  .badge-abnormal { background: #7f1d1d; color: #fca5a5; padding: 2px 8px; border-radius: 4px; font-size: 0.72rem; font-weight: 700; }
  .badge-vol-up   { background: #064e3b; color: #34d399; padding: 2px 8px; border-radius: 4px; font-size: 0.72rem; font-weight: 700; }
  .badge-normal { color: #475569; font-size: 0.78rem; }
</style>
</head>
<body>
<header>
  <h1>S&amp;P 500 Screener</h1>
  <nav class="nav-links">
    <a href="/" class="nav-link">Table</a>
    <a href="/screener" class="nav-link active">Screener</a>
    <a href="/gurus" class="nav-link">Guru Investing</a>
    <a href="/recommendations" class="nav-link">Recommendations</a>
  </nav>
</header>
<div class="page-tabs">
  <div class="page-tab active" id="tab-screener" onclick="switchTab('screener')">Screener</div>
  <div class="page-tab" id="tab-volume" onclick="switchTab('volume')">Volume Analysis</div>
  <div class="page-tab" id="tab-voltrades" onclick="switchTab('voltrades')">Vol Trades</div>
</div>

<div id="scr-wrap">
  <!-- LEFT: filters -->
  <div id="scr-filters">
    <div id="guru-preset-wrap">
      <span id="guru-preset-label">Guru</span>
      <select id="guru-preset">
        <option value="">— Load guru rules —</option>
      </select>
      <div id="guru-preset-badge">
        <span id="guru-preset-dot"></span>
        <span id="guru-preset-name"></span>
        <button id="guru-preset-clear" title="Clear guru rules">✕</button>
      </div>
    </div>
    <div id="custom-filter-wrap">
      <div id="custom-filter-label">Custom Filters</div>
      <select id="custom-filter-select">
        <option value="">— Load saved filter —</option>
      </select>
      <input type="text" id="custom-filter-name" placeholder="Filter name to save…" autocomplete="off">
      <div id="custom-filter-btns">
        <button id="cf-save-btn">Save</button>
        <button id="cf-delete-btn">Delete</button>
      </div>
    </div>
    <div id="param-search-wrap">
      <span id="param-search-icon">⌕</span>
      <input type="text" id="param-search" placeholder="Search parameters… (e.g. ROE, Beta, Sector)" autocomplete="off">
      <button id="param-search-clear" title="Clear">✕</button>
    </div>
    <div id="param-match-count"></div>
    <div id="scr-controls">
      <button id="scr-apply">Apply</button>
      <button id="scr-reset">Reset</button>
      <span id="scr-match">— matches</span>
    </div>
    <div id="scr-groups"></div>
  </div>
  <!-- RIGHT: results -->
  <div id="scr-results">
    <div id="scr-summary">
      Matching: <strong id="res-count">—</strong> companies
      <span id="res-hint" style="color:#4f5b8a">Set filters and click Apply</span>
      <span id="refresh-badge" style="margin-left:auto;font-size:0.72rem;color:#475569" title="Prices auto-refresh every 5 min">⟳ —</span>
    </div>
    <div id="portfolio-bar">
      <span class="pb-label">Portfolio avg</span>
      <div class="pb-chip"><span class="pb-period">1W</span><span class="pb-val" id="pb-w1">—</span></div>
      <div class="pb-chip"><span class="pb-period">1M</span><span class="pb-val" id="pb-m1">—</span></div>
      <div class="pb-chip"><span class="pb-period">3M</span><span class="pb-val" id="pb-m3">—</span></div>
      <div class="pb-chip"><span class="pb-period">6M</span><span class="pb-val" id="pb-m6">—</span></div>
      <div class="pb-chip"><span class="pb-period">1Y</span><span class="pb-val" id="pb-y1">—</span></div>
    </div>
    <div id="scr-table-wrap">
      <table id="scr-tbl">
        <thead><tr>
          <th>Ticker</th><th>Name</th><th>Sector</th>
          <th class="num">Price</th><th class="num">Mkt Cap</th>
          <th class="num">P/E</th><th class="num">Div Yield</th>
          <th class="num">Beta</th><th class="num">ROE</th>
          <th class="num">Rev Growth</th>
          <th class="num">Change</th>
          <th class="num">1W</th><th class="num">1M</th><th class="num">3M</th><th class="num">6M</th><th class="num">1Y</th>
          <th>Rec</th>
        </tr></thead>
        <tbody id="scr-tbody"></tbody>
      </table>
    </div>
  </div>
</div>

<!-- volume analysis panel -->
<div id="vol-panel">
  <div id="vol-toolbar">
    <div class="vol-filter-btns">
      <button class="vol-fbtn"            data-filter=""         onclick="setVolFilter(this, '')">All</button>
      <button class="vol-fbtn bullish active" data-filter="Bullish" onclick="setVolFilter(this, 'Bullish')">Bullish</button>
      <button class="vol-fbtn bearish"   data-filter="Bearish"  onclick="setVolFilter(this, 'Bearish')">Bearish</button>
      <button class="vol-fbtn neutral"   data-filter="Neutral"  onclick="setVolFilter(this, 'Neutral')">Neutral</button>
    </div>
    <button id="vol-refresh-btn" onclick="loadVolumeData()">&#x27F3; Refresh</button>
    <span id="vol-status">Loading…</span>
  </div>
  <div id="vol-tbl-wrap">
    <table id="vol-tbl" class="dataTable">
      <thead><tr>
        <th>Ticker</th>
        <th>Company</th>
        <th>Sector</th>
        <th class="num">Price</th>
        <th class="num">Today Vol</th>
        <th class="num">1W Avg Vol</th>
        <th class="num">1M Avg Vol</th>
        <th class="num">3M Avg Vol</th>
        <th class="num">Today Chg</th>
        <th class="num">Earnings Date</th>
        <th class="num">Vol Ratio</th>
        <th>Sentiment</th>
      </tr></thead>
      <tbody id="vol-tbody"></tbody>
    </table>
  </div>
</div>

<!-- vol trades panel -->
<div id="vt-panel">
  <div id="vt-toolbar">
    <div id="vt-stats-bar">
      <div class="vt-stat"><span class="vt-stat-label">Realized P&amp;L</span><span class="vt-stat-val vt-neu" id="vt-net-pnl">—</span></div>
      <div class="vt-stat"><span class="vt-stat-label">Win Rate %</span><span class="vt-stat-val vt-neu" id="vt-win-rate">—</span></div>
      <div class="vt-stat"><span class="vt-stat-label">Today's P&amp;L</span><span class="vt-stat-val vt-neu" id="vt-day-pct">—</span></div>
      <div class="vt-stat"><span class="vt-stat-label">Weekly (Mon–Fri)</span><span class="vt-stat-val vt-neu" id="vt-week-pct">—</span></div>
      <div class="vt-stat"><span class="vt-stat-label">Monthly (MTD)</span><span class="vt-stat-val vt-neu" id="vt-month-pct">—</span></div>
    </div>
    <div id="vt-toolbar-right">
      <span id="vt-status">—</span>
      <button id="vt-refresh-btn" onclick="loadVolTrades()">&#x27F3; Refresh</button>
    </div>
  </div>
  <div id="vt-tbl-wrap">
    <table id="vt-tbl" class="vt-table">
      <thead><tr>
        <th>Trade Date</th>
        <th>Ticker</th>
        <th class="num">Buy Time</th>
        <th class="num">Buy Price</th>
        <th class="num">Amount</th>
        <th class="num">Sell Time</th>
        <th class="num">Sell Price</th>
        <th class="num">P&amp;L ($)</th>
        <th class="num">P&amp;L (%)</th>
        <th>Status</th>
      </tr></thead>
      <tbody id="vt-tbody"></tbody>
    </table>
  </div>
</div>

<!-- chart popup -->
<div id="hist-overlay"></div>
<div id="hist-modal">
  <div id="hist-header">
    <div><h2 id="hist-title"></h2><div id="hist-subtitle">1-Year Price History</div></div>
    <div class="interval-btns">
      <button class="interval-btn active" data-interval="1d">Daily</button>
      <button class="interval-btn" data-interval="5m">5 Min</button>
    </div>
    <button id="hist-close">&#x2715;</button>
  </div>
  <div id="hist-stats">
    <div class="hstat"><label>1Y High</label><span id="hs-high">—</span></div>
    <div class="hstat"><label>1Y Low</label><span id="hs-low">—</span></div>
    <div class="hstat"><label>1Y Return</label><span id="hs-ret">—</span></div>
    <div class="hstat"><label>Avg Volume</label><span id="hs-vol">—</span></div>
    <div class="hstat"><label>Last Volume</label><span id="hs-lastvol">—</span></div>
    <div class="hstat"><label>Last Close</label><span id="hs-last">—</span></div>
  </div>
  <div id="hist-body">
    <div id="hist-charts">
      <div id="hist-loading">Loading chart…</div>
      <div id="price-chart"></div>
      <div id="vol-chart" style="display:none"></div>
    </div>
    <div id="hist-company-panel">
      <div id="hist-company-divider">Company Details</div>
      <div id="hist-company"></div>
    </div>
  </div>
</div>

<script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
<script src="https://cdn.datatables.net/1.13.8/js/jquery.dataTables.min.js"></script>
<script src="https://unpkg.com/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js"></script>
<script>
window.onerror = function(msg, src, line) {
  const el = document.getElementById('vt-status');
  if (el) el.textContent = 'JS ERR: ' + msg + ' (line ' + line + ')';
};
// ── format helpers ──────────────────────────────────────────────────────────
const fmt = {
  pct:   v => v == null ? '—' : (v*100).toFixed(1)+'%',
  num:  (v,d=2) => v == null ? '—' : Number(v).toLocaleString(undefined,{minimumFractionDigits:d,maximumFractionDigits:d}),
  cap:   v => { if(v==null)return '—'; const a=Math.abs(v); const s=v<0?'-':''; if(a>=1e12)return s+'$'+(a/1e12).toFixed(2)+'T'; if(a>=1e9)return s+'$'+(a/1e9).toFixed(2)+'B'; if(a>=1e6)return s+'$'+(a/1e6).toFixed(1)+'M'; return s+'$'+a.toLocaleString(); },
  dollar:v => v==null?'—':'$'+Number(v).toFixed(2),
  hint:  (v,fmt_)=> {
    if(v==null)return '—';
    if(fmt_==='dollar') return '$'+v.toFixed(2);
    if(fmt_==='cap')    { const a=Math.abs(v); if(a>=1e12)return '$'+(a/1e12).toFixed(1)+'T'; if(a>=1e9)return '$'+(a/1e9).toFixed(1)+'B'; if(a>=1e6)return '$'+(a/1e6).toFixed(0)+'M'; return '$'+a.toLocaleString(); }
    if(fmt_==='pct_frac'||fmt_==='pct_val') return v.toFixed(1)+'%';
    return v.toFixed(2);
  },
  rec: v => {
    if(!v||v==='none') return '<span class="rec-none">None</span>';
    const labels={strong_buy:'Strong Buy',buy:'Buy',hold:'Hold',sell:'Sell'};
    const cls={strong_buy:'rec-strong-buy',buy:'rec-buy',hold:'rec-hold',sell:'rec-sell'};
    return `<span class="${cls[v]||'rec-none'}">${labels[v]||v}</span>`;
  },
};

// ── build filter UI ─────────────────────────────────────────────────────────
let scrMeta = null;
let scrDt   = null;
let debounceTimer = null;

fetch('/api/screener/meta').then(r=>r.json()).then(meta => {
  scrMeta = meta;
  const container = document.getElementById('scr-groups');

  // Categorical group
  const catGrp = makeGroup('Company');
  const catBody = catGrp.querySelector('.fg-body');
  for (const [fid, info] of Object.entries(meta.categorical)) {
    const wrapper = document.createElement('div');
    wrapper.className = 'cat-field-wrapper';
    wrapper.dataset.label = info.label.toLowerCase();
    wrapper.style.marginBottom = '12px';
    const lbl = document.createElement('div');
    lbl.style.cssText = 'font-size:0.72rem;color:#94a3b8;margin-bottom:5px;font-weight:600;';
    lbl.textContent = info.label;
    const search = document.createElement('input');
    search.className = 'cat-search'; search.placeholder = 'Search…'; search.type='text';
    const list = document.createElement('div');
    list.className = 'cat-list'; list.dataset.fid = fid;
    const renderList = q => {
      list.innerHTML = '';
      info.values.filter(v=>!q||String(v).toLowerCase().includes(q.toLowerCase())).forEach(v=>{
        const item = document.createElement('label');
        item.className = 'cat-item';
        const cb = document.createElement('input'); cb.type='checkbox'; cb.value=v; cb.dataset.fid=fid;
        cb.addEventListener('change', scheduleApply);
        item.appendChild(cb); item.append(' '+v); list.appendChild(item);
      });
    };
    renderList('');
    search.addEventListener('input', ()=>renderList(search.value));
    wrapper.appendChild(lbl); wrapper.appendChild(search); wrapper.appendChild(list);
    catBody.appendChild(wrapper);
  }
  container.appendChild(catGrp);

  // Numeric groups
  for (const group of meta.groups) {
    const fields = Object.entries(meta.numeric).filter(([,f])=>f.group===group);
    if(!fields.length) continue;
    const grp = makeGroup(group);
    const body = grp.querySelector('.fg-body');
    for (const [fid, info] of fields) {
      const div = document.createElement('div');
      div.className = 'num-field';
      const hint = `${fmt.hint(info.p5, info.fmt)} – ${fmt.hint(info.p95, info.fmt)}`;
      div.innerHTML = `
        <div class="num-label">
          <span>${info.label}</span>
          <span class="num-range-hint">typical: ${hint}</span>
        </div>
        <div class="num-inputs">
          <input type="number" placeholder="Min" data-fid="${fid}" data-side="min" step="any">
          <span class="num-sep">—</span>
          <input type="number" placeholder="Max" data-fid="${fid}" data-side="max" step="any">
        </div>`;
      div.querySelectorAll('input').forEach(inp=>{
        inp.addEventListener('input', ()=>{ inp.classList.toggle('active', inp.value!==''); scheduleApply(); });
      });
      body.appendChild(div);
    }
    container.appendChild(grp);
  }

  // init DataTable (empty) — cols: 0=Ticker,1=Name,2=Sector,3=Price,4=MktCap,5=PE,6=Div,7=Beta,8=ROE,9=RevGrowth,10=Change,11=1W,12=1M,13=3M,14=6M,15=1Y,16=Rec
  scrDt = $('#scr-tbl').DataTable({ paging:false, order:[[4,'desc']], dom:'ti',
    columnDefs:[{orderable:false,targets:[1,2,16]}] });
  applyFilters();
  loadVolTrades();
});

function makeGroup(title) {
  const div = document.createElement('div');
  div.className = 'fg-group';
  div.innerHTML = `<div class="fg-header">${title}</div><div class="fg-body"></div>`;
  div.querySelector('.fg-header').addEventListener('click',()=>div.classList.toggle('open'));
  return div;
}

function scheduleApply() {
  clearTimeout(debounceTimer);
  debounceTimer = setTimeout(applyFilters, 350);
}

function gatherFilters() {
  const numeric = {}, categorical = {};
  document.querySelectorAll('.num-inputs input[data-fid]').forEach(inp => {
    if(!inp.value) return;
    const fid = inp.dataset.fid, side = inp.dataset.side;
    numeric[fid] = numeric[fid] || {};
    numeric[fid][side] = parseFloat(inp.value);
  });
  document.querySelectorAll('.cat-list input[type=checkbox]:checked').forEach(cb => {
    const fid = cb.dataset.fid;
    categorical[fid] = categorical[fid] || [];
    categorical[fid].push(cb.value);
  });
  return { numeric, categorical };
}

function applyFilters() {
  const filters = gatherFilters();
  const activeCount = Object.keys(filters.numeric).length + Object.keys(filters.categorical).length;
  document.getElementById('scr-match').textContent = activeCount ? `${activeCount} filter${activeCount>1?'s':''} active` : '— matches';

  fetch('/api/screener/run', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(filters) })
    .then(r=>r.json()).then(data => {
      document.getElementById('res-count').textContent = data.count;
      document.getElementById('res-hint').textContent = '';

      // Portfolio summary bar
      const pb = document.getElementById('portfolio-bar');
      const pf = data.portfolio || {};
      if (data.count > 0 && Object.keys(pf).some(k => pf[k] != null)) {
        pb.classList.add('visible');
        [['w1','pb-w1'],['m1','pb-m1'],['m3','pb-m3'],['m6','pb-m6'],['y1','pb-y1']].forEach(([k,id])=>{
          const el = document.getElementById(id);
          const v = pf[k];
          if (v == null) { el.textContent='—'; el.className='pb-val'; return; }
          el.textContent = (v>=0?'+':'')+v.toFixed(1)+'%';
          el.className = 'pb-val ' + (v>0?'up':v<0?'down':'');
        });
      } else { pb.classList.remove('visible'); }

      scrDt.clear();
      const tbody = document.getElementById('scr-tbody');
      tbody.innerHTML = '';
      const n = v => v ?? -Infinity;
      function chgCell(v) {
        if (v == null) return '<td class="num chg flat" data-order="-9999">—</td>';
        const cls = v>0?'up':v<0?'down':'flat';
        return `<td class="num chg ${cls}" data-order="${v}">${v>=0?'+':''}${v.toFixed(1)}%</td>`;
      }
      data.results.forEach(r => {
        const pc = r.pc || {};
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td><span class="tk" data-ticker="${r.ticker}" data-name="${r.longName||''}">${r.ticker}</span></td>
          <td>${r.longName||'—'}</td>
          <td><span class="sb">${r.sector||'—'}</span></td>
          <td class="num" data-order="${n(r.currentPrice)}">${r.currentPrice!=null?'$'+r.currentPrice.toFixed(2):'—'}</td>
          <td class="num" data-order="${n(r.marketCap)}">${fmt.cap(r.marketCap)}</td>
          <td class="num" data-order="${n(r.trailingPE)}">${r.trailingPE!=null?r.trailingPE.toFixed(1):'—'}</td>
          <td class="num" data-order="${n(r.dividendYield)}">${r.dividendYield!=null?r.dividendYield.toFixed(2)+'%':'—'}</td>
          <td class="num" data-order="${n(r.beta)}">${r.beta!=null?r.beta.toFixed(2):'—'}</td>
          <td class="num" data-order="${n(r.returnOnEquity)}">${r.returnOnEquity!=null?fmt.pct(r.returnOnEquity):'—'}</td>
          <td class="num" data-order="${n(r.revenueGrowth)}">${r.revenueGrowth!=null?fmt.pct(r.revenueGrowth):'—'}</td>
          ${chgCell(r.d1)}${chgCell(pc.w1)}${chgCell(pc.m1)}${chgCell(pc.m3)}${chgCell(pc.m6)}${chgCell(pc.y1)}
          <td>${fmt.rec(r.recommendationKey)}</td>`;
        tbody.appendChild(tr);
      });
      scrDt.rows.add($(tbody).find('tr')).draw();
      document.querySelectorAll('.tk').forEach(el=>{
        el.addEventListener('click',()=>openHistory(el.dataset.ticker, el.dataset.name));
      });
    });
}

document.getElementById('scr-apply').addEventListener('click', applyFilters);
document.getElementById('scr-reset').addEventListener('click', ()=>{
  document.querySelectorAll('.num-inputs input').forEach(i=>{i.value='';i.classList.remove('active');});
  document.querySelectorAll('.cat-list input[type=checkbox]').forEach(cb=>cb.checked=false);
  document.getElementById('scr-match').textContent='— matches';
  clearGuruPreset();
  applyFilters();
});

// ── Guru preset ─────────────────────────────────────────────────────────────
fetch('/api/guru/list').then(r=>r.json()).then(gurus=>{
  const sel = document.getElementById('guru-preset');

  // Master Guru
  const masterOpt = document.createElement('option');
  masterOpt.value = 'master';
  masterOpt.textContent = '⭐ Master Guru  –  Consensus of all gurus';
  masterOpt.style.fontWeight = '700';
  sel.appendChild(masterOpt);

  // Optimized Guru
  const optOpt = document.createElement('option');
  optOpt.value = 'optimized';
  optOpt.textContent = '🚀 Optimized Guru  –  Data-driven max returns';
  optOpt.style.fontWeight = '700';
  sel.appendChild(optOpt);

  // Divider
  const divider = document.createElement('option');
  divider.disabled = true;
  divider.textContent = '──────────────────';
  sel.appendChild(divider);

  // Individual gurus
  gurus.forEach(g=>{
    const opt = document.createElement('option');
    opt.value = g.slug;
    opt.textContent = `${g.name}  –  ${g.fund}`;
    sel.appendChild(opt);
  });
});

function clearGuruPreset() {
  document.getElementById('guru-preset').value = '';
  const badge = document.getElementById('guru-preset-badge');
  badge.style.display = 'none';
}

document.getElementById('guru-preset').addEventListener('change', e=>{
  const slug = e.target.value;
  if (!slug) { clearGuruPreset(); return; }
  fetch(`/api/guru/${slug}/screener-rules`).then(r=>r.json()).then(rules=>{
    // clear all current filters
    document.querySelectorAll('.num-inputs input').forEach(i=>{i.value='';i.classList.remove('active');});
    document.querySelectorAll('.cat-list input[type=checkbox]').forEach(cb=>cb.checked=false);

    // apply numeric rules
    for (const [fid, bounds] of Object.entries(rules.numeric||{})) {
      for (const side of ['min','max']) {
        if (bounds[side]==null) continue;
        const inp = document.querySelector(`.num-inputs input[data-fid="${fid}"][data-side="${side}"]`);
        if (inp) { inp.value = bounds[side]; inp.classList.add('active'); }
      }
    }
    // apply categorical rules
    for (const [fid, vals] of Object.entries(rules.categorical||{})) {
      vals.forEach(v=>{
        const cb = document.querySelector(`.cat-list input[data-fid="${fid}"][value="${v}"]`);
        if (cb) cb.checked = true;
      });
    }
    // open groups that have active filters
    document.querySelectorAll('.fg-group').forEach(grp=>{
      const active = grp.querySelectorAll('.num-inputs input.active, .cat-list input:checked').length;
      if (active) grp.classList.add('open');
    });
    // show badge
    const badge = document.getElementById('guru-preset-badge');
    const dot = document.getElementById('guru-preset-dot');
    const nameEl = document.getElementById('guru-preset-name');
    dot.style.background = rules.color || '#818cf8';
    nameEl.textContent = slug === 'master'
      ? `⭐ ${rules.name}  (${Object.keys(rules.numeric||{}).length} rules)`
      : `${rules.name}  (${Object.keys(rules.numeric||{}).length} rules)`;
    nameEl.style.color = slug === 'master' ? '#f59e0b' : '#a5b4fc';
    badge.style.display = 'flex';

    applyFilters();
  });
});

document.getElementById('guru-preset-clear').addEventListener('click', ()=>{
  document.querySelectorAll('.num-inputs input').forEach(i=>{i.value='';i.classList.remove('active');});
  document.querySelectorAll('.cat-list input[type=checkbox]').forEach(cb=>cb.checked=false);
  document.getElementById('scr-match').textContent='— matches';
  clearGuruPreset();
  applyFilters();
});

// ── Parameter search ────────────────────────────────────────────────────────
function highlight(text, q) {
  if (!q) return text;
  const idx = text.toLowerCase().indexOf(q);
  if (idx < 0) return text;
  return text.slice(0,idx) + '<mark class="param-highlight">' + text.slice(idx, idx+q.length) + '</mark>' + text.slice(idx+q.length);
}

function filterParams(raw) {
  const q = raw.trim().toLowerCase();
  const clearBtn   = document.getElementById('param-search-clear');
  const countEl    = document.getElementById('param-match-count');
  clearBtn.style.display = q ? 'block' : 'none';

  if (!q) {
    // Reset everything
    document.querySelectorAll('.fg-group').forEach(g => {
      g.classList.remove('param-hidden');
      g.querySelectorAll('.num-field').forEach(f => {
        f.classList.remove('param-hidden');
        const lbl = f.querySelector('.num-label span:first-child');
        if (lbl) lbl.innerHTML = lbl.textContent; // strip highlights
      });
      g.querySelectorAll('.cat-field-wrapper').forEach(w => w.classList.remove('param-hidden'));
    });
    countEl.style.display = 'none';
    return;
  }

  let totalVisible = 0;

  document.querySelectorAll('.fg-group').forEach(grp => {
    const headerEl   = grp.querySelector('.fg-header');
    const groupLabel = headerEl.textContent.toLowerCase();
    const groupMatch = groupLabel.includes(q);
    let grpVisible   = 0;

    // Numeric fields
    grp.querySelectorAll('.num-field').forEach(field => {
      const lblEl = field.querySelector('.num-label span:first-child');
      const label = lblEl ? lblEl.textContent.toLowerCase() : '';
      const show  = groupMatch || label.includes(q);
      field.classList.toggle('param-hidden', !show);
      if (show) {
        grpVisible++;
        if (lblEl) lblEl.innerHTML = highlight(lblEl.textContent, q);
      } else {
        if (lblEl) lblEl.innerHTML = lblEl.textContent;
      }
    });

    // Categorical field wrappers
    grp.querySelectorAll('.cat-field-wrapper').forEach(wrapper => {
      const label = wrapper.dataset.label || '';
      const show  = groupMatch || label.includes(q);
      wrapper.classList.toggle('param-hidden', !show);
      if (show) grpVisible++;
    });

    const show = grpVisible > 0 || groupMatch;
    grp.classList.toggle('param-hidden', !show);
    if (show) {
      totalVisible += grpVisible || 1;
      grp.classList.add('open'); // auto-expand matching groups
    }
  });

  countEl.textContent = totalVisible + ' matching parameter' + (totalVisible !== 1 ? 's' : '');
  countEl.style.display = 'block';
}

const paramInput = document.getElementById('param-search');
paramInput.addEventListener('input', e => filterParams(e.target.value));
document.getElementById('param-search-clear').addEventListener('click', () => {
  paramInput.value = '';
  filterParams('');
  paramInput.focus();
});

// ── chart popup ──────────────────────────────────────────────────────────
let priceChart=null, volChart=null, _histTicker=null, _histName=null;

function openHistory(ticker, name) {
  _histTicker=ticker; _histName=name||'';
  document.getElementById('hist-title').textContent=`${ticker}  —  ${_histName}`;
  document.getElementById('hist-loading').style.display='block';
  document.getElementById('price-chart').style.cssText='display:none';
  document.getElementById('vol-chart').style.display='none';
  ['hs-high','hs-low','hs-ret','hs-vol','hs-lastvol','hs-last'].forEach(id=>document.getElementById(id).textContent='…');
  document.getElementById('hist-company').innerHTML='';
  document.querySelectorAll('.interval-btn').forEach(b=>b.classList.remove('active'));
  document.querySelector('.interval-btn[data-interval="1d"]').classList.add('active');
  document.getElementById('hist-modal').classList.add('active');
  document.getElementById('hist-overlay').classList.add('active');

  fetch(`/api/company/${ticker}`).then(r=>r.json()).then(data=>{
    const container=document.getElementById('hist-company');
    container.innerHTML='';
    for(const [section,fields] of Object.entries(data.sections)){
      const pop=Object.entries(fields).filter(([,v])=>v!=null);
      if(!pop.length) continue;
      const div=document.createElement('div'); div.className='section';
      div.innerHTML=`<h3>${section}<span class="field-count">${pop.length} fields</span></h3><div class="field-grid"></div>`;
      div.querySelector('h3').addEventListener('click',()=>div.classList.toggle('open'));
      const grid=div.querySelector('.field-grid');
      pop.forEach(([key,val])=>{
        const f=document.createElement('div'); f.className='field'+(key==='longBusinessSummary'?' wide':'');
        const lbl=key.replace(/([A-Z])/g,' $1').replace(/^./,s=>s.toUpperCase());
        f.innerHTML=`<label>${lbl}</label><span>${typeof val==='boolean'?(val?'Yes':'No'):String(val)}</span>`;
        grid.appendChild(f);
      });
      container.appendChild(div);
    }
  }).catch(()=>{});

  _loadHistChart(ticker,'1d');
}

function _loadHistChart(ticker,interval){
  document.getElementById('hist-loading').style.display='block';
  document.getElementById('price-chart').style.cssText='display:none';
  document.getElementById('vol-chart').style.display='none';
  fetch(`/api/history/${ticker}?interval=${interval}`)
    .then(r=>r.json())
    .then(rows=>_renderHistChart(rows,interval))
    .catch(()=>{document.getElementById('hist-loading').style.display='none';});
}

function _renderHistChart(rows,interval){
  document.getElementById('hist-loading').style.display='none';
  document.getElementById('price-chart').style.cssText='';
  document.getElementById('vol-chart').style.display='block';
  if(interval!=='5m'){
    const closes=rows.map(r=>r.close);
    const high=Math.max(...rows.map(r=>r.high)), low=Math.min(...rows.map(r=>r.low));
    const ret=(closes.at(-1)-closes[0])/closes[0];
    const avgVol=rows.reduce((s,r)=>s+r.volume,0)/rows.length;
    document.getElementById('hs-high').textContent='$'+high.toFixed(2);
    document.getElementById('hs-low').textContent='$'+low.toFixed(2);
    const retEl=document.getElementById('hs-ret');
    retEl.textContent=(ret>=0?'+':'')+(ret*100).toFixed(1)+'%'; retEl.className=ret>=0?'up':'down';
    document.getElementById('hs-vol').textContent=fmt.cap(avgVol).replace('$','');
    document.getElementById('hs-lastvol').textContent=fmt.cap(rows.at(-1).volume).replace('$','');
    document.getElementById('hs-last').textContent='$'+closes.at(-1).toFixed(2);
  } else {
    ['hs-high','hs-low','hs-ret','hs-vol','hs-lastvol','hs-last'].forEach(id=>document.getElementById(id).textContent='—');
  }
  if(priceChart){priceChart.remove();priceChart=null;}
  if(volChart){volChart.remove();volChart=null;}
  const opts={layout:{background:{color:'#1a1d27'},textColor:'#94a3b8'},grid:{vertLines:{color:'#1e2235'},horzLines:{color:'#1e2235'}},crosshair:{mode:1},rightPriceScale:{borderColor:'#2d3148'},timeScale:{borderColor:'#2d3148',timeVisible:true},handleScroll:true,handleScale:true};
  const pEl=document.getElementById('price-chart');
  priceChart=LightweightCharts.createChart(pEl,{...opts,autoSize:true});
  const candles=priceChart.addCandlestickSeries({upColor:'#34d399',downColor:'#f87171',borderUpColor:'#34d399',borderDownColor:'#f87171',wickUpColor:'#34d399',wickDownColor:'#f87171'});
  candles.setData(rows.map(r=>({time:r.time,open:r.open,high:r.high,low:r.low,close:r.close})));
  priceChart.timeScale().fitContent();
  const vEl=document.getElementById('vol-chart');
  volChart=LightweightCharts.createChart(vEl,{...opts,autoSize:true});
  const volS=volChart.addHistogramSeries({color:'#4f5b8a',priceFormat:{type:'volume'},priceScaleId:'vol',scaleMargins:{top:0.1,bottom:0}});
  volS.setData(rows.map((r,i)=>({time:r.time,value:r.volume,color:i>0&&r.close>=rows[i-1].close?'#34d39966':'#f8717166'})));
  volChart.timeScale().fitContent();
  priceChart.timeScale().subscribeVisibleLogicalRangeChange(range=>{if(range)volChart.timeScale().setVisibleLogicalRange(range);});
  volChart.timeScale().subscribeVisibleLogicalRangeChange(range=>{if(range)priceChart.timeScale().setVisibleLogicalRange(range);});
}

document.querySelectorAll('.interval-btn').forEach(btn=>{
  btn.addEventListener('click',()=>{
    document.querySelectorAll('.interval-btn').forEach(b=>b.classList.remove('active'));
    btn.classList.add('active');
    if(_histTicker) _loadHistChart(_histTicker,btn.dataset.interval);
  });
});

function closeHistory(){
  document.getElementById('hist-modal').classList.remove('active');
  document.getElementById('hist-overlay').classList.remove('active');
}
document.getElementById('hist-close').addEventListener('click',closeHistory);
document.getElementById('hist-overlay').addEventListener('click',closeHistory);

// ── Custom saved filters ──────────────────────────────────────────────────────
let _savedFilters = [];   // [{id, name}]

function _loadSavedFilterList() {
  fetch('/api/screener/saved-filters').then(r => r.json()).then(list => {
    _savedFilters = list;
    const sel = document.getElementById('custom-filter-select');
    const prev = sel.value;
    sel.innerHTML = '<option value="">— Load saved filter —</option>';
    list.forEach(f => {
      const opt = document.createElement('option');
      opt.value = f.id;
      opt.textContent = f.name;
      sel.appendChild(opt);
    });
    if (prev) sel.value = prev;
  });
}

function _applyFilterState(filters) {
  document.querySelectorAll('.num-inputs input').forEach(i => { i.value = ''; i.classList.remove('active'); });
  document.querySelectorAll('.cat-list input[type=checkbox]').forEach(cb => cb.checked = false);
  for (const [fid, bounds] of Object.entries(filters.numeric || {})) {
    for (const side of ['min', 'max']) {
      if (bounds[side] == null) continue;
      const inp = document.querySelector(`.num-inputs input[data-fid="${fid}"][data-side="${side}"]`);
      if (inp) { inp.value = bounds[side]; inp.classList.add('active'); }
    }
  }
  for (const [fid, vals] of Object.entries(filters.categorical || {})) {
    vals.forEach(v => {
      const cb = document.querySelector(`.cat-list input[data-fid="${fid}"][value="${v}"]`);
      if (cb) cb.checked = true;
    });
  }
  document.querySelectorAll('.fg-group').forEach(grp => {
    const active = grp.querySelectorAll('.num-inputs input.active, .cat-list input:checked').length;
    if (active) grp.classList.add('open');
  });
  applyFilters();
}

document.getElementById('custom-filter-select').addEventListener('change', e => {
  const id = parseInt(e.target.value);
  if (!id) return;
  const entry = _savedFilters.find(f => f.id === id);
  if (entry) document.getElementById('custom-filter-name').value = entry.name;
  fetch('/api/screener/saved-filters/' + id).then(r => r.json()).then(data => {
    _applyFilterState(data.filters);
  });
});

document.getElementById('cf-save-btn').addEventListener('click', () => {
  const name = document.getElementById('custom-filter-name').value.trim();
  if (!name) { alert('Enter a filter name before saving.'); return; }
  const filters = gatherFilters();
  const hasFilters = Object.keys(filters.numeric).length + Object.keys(filters.categorical).length;
  if (!hasFilters) { alert('No active filters to save.'); return; }
  const existing = _savedFilters.find(f => f.name === name);
  if (existing && !confirm(`Overwrite saved filter "${name}"?`)) return;
  fetch('/api/screener/saved-filters', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({name, filters})
  }).then(r => r.json()).then(res => {
    if (res.ok) { _loadSavedFilterList(); }
  });
});

document.getElementById('cf-delete-btn').addEventListener('click', () => {
  const sel = document.getElementById('custom-filter-select');
  const id = parseInt(sel.value);
  if (!id) { alert('Select a saved filter to delete.'); return; }
  const entry = _savedFilters.find(f => f.id === id);
  if (!confirm(`Delete saved filter "${entry ? entry.name : id}"?`)) return;
  fetch('/api/screener/saved-filters/' + id, {method: 'DELETE'}).then(r => r.json()).then(res => {
    if (res.ok) {
      document.getElementById('custom-filter-name').value = '';
      _loadSavedFilterList();
    }
  });
});

_loadSavedFilterList();

// ── Volume Analysis tab ───────────────────────────────────────────────────────
let volDt = null, volLoaded = false;

function switchTab(name) {
  document.getElementById('scr-wrap').style.display  = name === 'screener'  ? 'flex' : 'none';
  document.getElementById('vol-panel').style.display = name === 'volume'    ? 'flex' : 'none';
  document.getElementById('vt-panel').style.display  = name === 'voltrades' ? 'flex' : 'none';
  document.getElementById('tab-screener').classList.toggle('active', name === 'screener');
  document.getElementById('tab-volume').classList.toggle('active', name === 'volume');
  document.getElementById('tab-voltrades').classList.toggle('active', name === 'voltrades');
  if (name === 'volume' && !volLoaded) loadVolumeData();
  if (name === 'voltrades') window.loadVolTrades && window.loadVolTrades();
}

window.loadVolTrades = function() {
  var statusEl = document.getElementById('vt-status');
  if (statusEl) statusEl.textContent = 'Loading…';
  fetch('/api/vol-trades')
    .then(function(r) { return r.json(); })
    .then(function(data) {
      var s = data.stats || {};
      function vtFmt(val, isDollar) {
        if (val == null) return '<span class="vt-neu">—</span>';
        var cls = val > 0 ? 'vt-pos' : val < 0 ? 'vt-neg' : 'vt-neu';
        var sign = val > 0 ? '+' : '';
        var str = isDollar ? sign + '$' + Math.abs(val).toFixed(2) : sign + val.toFixed(2) + '%';
        return '<span class="' + cls + '">' + str + '</span>';
      }
      var netPnlStr = (s.realized_pnl_dollar != null)
        ? vtFmt(s.realized_pnl_dollar, true) +
          (s.realized_pnl_pct != null ? ' <small>(' + (s.realized_pnl_pct >= 0 ? '+' : '') + s.realized_pnl_pct.toFixed(2) + '%)</small>' : '')
        : '<span class="vt-neu">—</span>';
      document.getElementById('vt-net-pnl').innerHTML   = netPnlStr;
      document.getElementById('vt-day-pct').innerHTML   = vtFmt(s.day_pnl_pct,   false);
      document.getElementById('vt-week-pct').innerHTML  = vtFmt(s.week_pnl_pct,  false);
      document.getElementById('vt-month-pct').innerHTML = vtFmt(s.month_pnl_pct, false);
      var winEl = document.getElementById('vt-win-rate');
      winEl.innerHTML = (s.success_rate != null)
        ? '<span class="' + (s.success_rate >= 50 ? 'vt-pos' : 'vt-neg') + '">' + s.success_rate.toFixed(1) + '%</span>'
        : '<span class="vt-neu">—</span>';
      var parts = [];
      if (data.buy_done)  parts.push('Bought ✓');
      if (data.sell_done) parts.push('Sold ✓');
      if (!data.buy_done && !data.sell_done) parts.push('Awaiting 10:00 ET buy');
      document.getElementById('vt-status').textContent =
        parts.join(' · ') + ' — ' + data.trades.length + ' trade' + (data.trades.length !== 1 ? 's' : '');
      var tbody = document.getElementById('vt-tbody');
      tbody.innerHTML = '';
      data.trades.forEach(function(t) {
        var tr = document.createElement('tr');
        var pnlD = t.pnl_dollar != null
          ? '<span class="' + (t.pnl_dollar >= 0 ? 'pnl-pos' : 'pnl-neg') + '">' +
            (t.pnl_dollar >= 0 ? '+' : '') + '$' + Math.abs(t.pnl_dollar).toFixed(2) + '</span>' : '—';
        var pnlP = t.pnl_pct != null
          ? '<span class="' + (t.pnl_pct >= 0 ? 'pnl-pos' : 'pnl-neg') + '">' +
            (t.pnl_pct >= 0 ? '+' : '') + t.pnl_pct.toFixed(2) + '%</span>' : '—';
        tr.innerHTML =
          '<td>' + t.trade_date + '</td>' +
          '<td><span class="tk" data-ticker="' + t.ticker + '">' + t.ticker + '</span></td>' +
          '<td class="num">' + (t.buy_time  || '—') + '</td>' +
          '<td class="num">' + (t.buy_price  != null ? '$' + t.buy_price.toFixed(2)  : '—') + '</td>' +
          '<td class="num">$' + t.amount_usd.toFixed(0) + '</td>' +
          '<td class="num">' + (t.sell_time || '—') + '</td>' +
          '<td class="num">' + (t.sell_price != null ? '$' + t.sell_price.toFixed(2) : '—') + '</td>' +
          '<td class="num">' + pnlD + '</td>' +
          '<td class="num">' + pnlP + '</td>' +
          '<td><span class="' + (t.status === 'open' ? 'status-open' : 'status-sold') + '">' + t.status.toUpperCase() + '</span></td>';
        tbody.appendChild(tr);
      });
      tbody.querySelectorAll('.tk').forEach(function(el) {
        el.addEventListener('click', function() { openHistory(el.dataset.ticker); });
      });
    })
    .catch(function(e) {
      var el = document.getElementById('vt-status');
      if (el) el.textContent = 'Error: ' + (e.message || 'fetch failed');
    });
};

function fmtVol(v) {
  if (v == null) return '<span style="color:#475569">—</span>';
  if (v >= 1e9) return (v/1e9).toFixed(2) + 'B';
  if (v >= 1e6) return (v/1e6).toFixed(2) + 'M';
  if (v >= 1e3) return (v/1e3).toFixed(0) + 'K';
  return v.toString();
}

function fmtPct(v) {
  if (v == null) return '<span style="color:#475569">—</span>';
  const cls = v > 0 ? 'color:#34d399' : v < 0 ? 'color:#f87171' : 'color:#64748b';
  return `<span style="${cls}">${v > 0 ? '+' : ''}${v.toFixed(2)}%</span>`;
}

function setVolFilter(btn, filter) {
  document.querySelectorAll('.vol-fbtn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  if (volDt) volDt.column(11).search(filter ? '^' + filter + '$' : '', true, false).draw();
}

function loadVolumeData() {
  document.getElementById('vol-status').textContent = 'Loading…';
  fetch('/api/volume-analysis').then(r => r.json()).then(data => {
    const tbody = document.getElementById('vol-tbody');
    tbody.innerHTML = '';
    data.forEach(d => {
      const tr = document.createElement('tr');
      tr.className = (d.d1 != null && d.d1 >= 0) ? 'vol-up-row' : 'vol-abnormal-row';
      tr.innerHTML = `
        <td><span class="tk" onclick="openHistory('${d.ticker}')">${d.ticker}</span></td>
        <td style="max-width:180px;overflow:hidden;text-overflow:ellipsis">${d.longName || '—'}</td>
        <td>${d.sector ? `<span class="sb">${d.sector}</span>` : '—'}</td>
        <td class="num">${d.current_price != null ? '$' + d.current_price.toFixed(2) : '—'}</td>
        <td class="num" data-order="${d.vol_today ?? -1}">${fmtVol(d.vol_today)}</td>
        <td class="num" data-order="${d.vol_1w ?? -1}">${fmtVol(d.vol_1w)}</td>
        <td class="num" data-order="${d.vol_1m ?? -1}">${fmtVol(d.vol_1m)}</td>
        <td class="num" data-order="${d.vol_3m ?? -1}">${fmtVol(d.vol_3m)}</td>
        <td class="num" data-order="${d.d1 ?? -999}">${fmtPct(d.d1)}</td>
        <td class="num">${d.earnings_date || '<span style="color:#475569">—</span>'}</td>
        <td class="num" data-order="${d.vol_ratio ?? 0}"><span class="${d.d1 != null && d.d1 >= 0 ? 'badge-vol-up' : 'badge-abnormal'}">${d.vol_ratio != null ? d.vol_ratio + '&#x00D7;' : '—'}</span></td>
        <td>${d.d1 == null ? 'Neutral' : d.d1 > 0 ? 'Bullish' : d.d1 < 0 ? 'Bearish' : 'Neutral'}</td>
      `;
      tbody.appendChild(tr);
    });
    if (volDt) { volDt.destroy(); volDt = null; }
    volDt = $('#vol-tbl').DataTable({
      paging: true, pageLength: 50, dom: 'tip',
      order: [[10, 'desc']],
      columnDefs: [
        { targets: [3,4,5,6,7,8,9,10], type: 'num' },
        { visible: false, targets: [11] },
      ],
    });
    volLoaded = true;
    document.querySelectorAll('.vol-fbtn').forEach(b => b.classList.remove('active'));
    const bullishBtn = document.querySelector('.vol-fbtn[data-filter="Bullish"]');
    bullishBtn.classList.add('active');
    if (volDt) volDt.column(11).search('^Bullish$', true, false).draw();
    document.getElementById('vol-status').textContent = `${data.length} stocks with abnormal volume`;
  }).catch(() => { document.getElementById('vol-status').textContent = 'Error loading'; });
}

// ── price refresh badge ──────────────────────────────────────────────────────
(function pollRefresh() {
  fetch('/api/last-refresh').then(r=>r.json()).then(d=>{
    const el = document.getElementById('refresh-badge');
    if (!el) return;
    if (d.status === 'open' && d.ts) {
      el.textContent = `⟳ ${d.ts}  (${d.count} prices)`;
      el.style.color = '#22d3ee';
    } else if (d.status === 'closed') {
      el.textContent = '⟳ market closed';
      el.style.color = '#475569';
    } else if (d.status === 'error') {
      el.textContent = '⟳ refresh error';
      el.style.color = '#f87171';
    } else {
      el.textContent = '⟳ awaiting market open';
      el.style.color = '#475569';
    }
  }).catch(()=>{});
  setTimeout(pollRefresh, 30000);
})();

</script>
</body>
</html>"""

STOCK_LB_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>S&amp;P 500 – Stock Picks</title>
<link rel="stylesheet" href="https://cdn.datatables.net/1.13.8/css/jquery.dataTables.min.css">
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: system-ui, sans-serif; background: #0f1117; color: #e2e8f0; min-height: 100vh; display: flex; flex-direction: column; }
  header { background: #1a1d27; border-bottom: 1px solid #2d3148; padding: 12px 24px; display: flex; align-items: center; gap: 16px; flex-shrink: 0; }
  header h1 { font-size: 1.25rem; font-weight: 700; color: #f8fafc; white-space: nowrap; }
  .nav-links { display: flex; gap: 4px; }
  .nav-link { font-size: 0.82rem; padding: 5px 14px; border-radius: 6px; text-decoration: none; color: #94a3b8; transition: background .15s, color .15s; }
  .nav-link:hover { background: #2d3148; color: #e2e8f0; }
  .nav-link.active { background: #3730a3; color: #fff; font-weight: 600; }

  #filter-bar { padding: 12px 24px; display: flex; gap: 10px; flex-wrap: wrap; align-items: center; background: #0f1117; border-bottom: 1px solid #1e2235; }
  #filter-bar input, #filter-bar select { background: #1a1d27; border: 1px solid #2d3148; color: #e2e8f0; padding: 6px 12px; border-radius: 6px; font-size: 0.83rem; outline: none; }
  #filter-bar input { min-width: 220px; }
  #filter-bar input:focus, #filter-bar select:focus { border-color: #3730a3; }
  #result-count { font-size: 0.78rem; color: #64748b; margin-left: 4px; }
  #loading-bar { height: 3px; background: #3730a3; width: 0%; transition: width .3s; }

  #main { padding: 0 24px 24px; overflow-x: auto; flex: 1; }
  .page-title { font-size: 1.3rem; font-weight: 700; color: #f8fafc; padding: 20px 0 4px; }
  .page-sub { font-size: 0.8rem; color: #64748b; margin-bottom: 16px; }

  table.dataTable { border-collapse: collapse; width: 100% !important; }
  table.dataTable thead th { background: #1a1d27; color: #64748b; font-size: 0.72rem; text-transform: uppercase; letter-spacing: .05em; border-bottom: 1px solid #2d3148; padding: 10px 12px; cursor: pointer; }
  table.dataTable tbody tr { border-bottom: 1px solid #1e2235; transition: background .1s; cursor: pointer; }
  table.dataTable tbody tr:hover { background: #1a1d27; }
  table.dataTable tbody td { padding: 9px 12px; font-size: 0.82rem; white-space: nowrap; }
  th.num, td.num { text-align: right !important; }
  .dataTables_wrapper .dataTables_filter, .dataTables_wrapper .dataTables_length { display: none; }
  .dataTables_info { font-size: 0.75rem; color: #64748b; padding: 8px 0; }
  table.dataTable thead .sorting::after, table.dataTable thead .sorting_asc::after,
  table.dataTable thead .sorting_desc::after { color: #64748b; }

  .ticker-badge { font-weight: 700; color: #818cf8; font-family: monospace; font-size: 0.9rem; text-decoration: underline dotted; }
  .ticker-badge:hover { color: #a5b4fc; }
  .guru-count-badge { display: inline-block; background: #3730a3; color: #a5b4fc; font-weight: 700; font-size: 0.78rem; padding: 2px 9px; border-radius: 99px; }
  .guru-pill { display: inline-block; font-size: 0.65rem; font-weight: 700; padding: 2px 7px; border-radius: 4px; margin: 1px; white-space: nowrap; }
  .sector-tag { background: #1e2235; color: #94a3b8; padding: 2px 6px; border-radius: 4px; font-size: 0.72rem; }
  .pos-change { color: #34d399; } .neg-change { color: #f87171; }
  .rec-strong-buy { color: #059669; font-weight: 700; }
  .rec-buy { color: #34d399; font-weight: 600; }
  .rec-hold { color: #fbbf24; font-weight: 600; }
  .rec-sell { color: #f87171; font-weight: 600; }
  .rec-none { color: #475569; }

  /* Chart popup */
  #hist-overlay { position: fixed; inset: 0; background: rgba(0,0,0,.7); z-index: 100; display: none; }
  #hist-overlay.active { display: block; }
  #hist-popup { position: fixed; top: 5vh; left: 50%; transform: translateX(-50%); width: 90vw; max-width: 1100px; height: 88vh; background: #1a1d27; border: 1px solid #2d3148; border-radius: 12px; z-index: 101; display: none; flex-direction: column; overflow: hidden; }
  #hist-popup.open { display: flex; }
  #hist-header { padding: 14px 20px; border-bottom: 1px solid #2d3148; display: flex; align-items: center; gap: 12px; flex-shrink: 0; }
  #hist-title { font-size: 1.1rem; font-weight: 700; color: #f8fafc; }
  #hist-meta { font-size: 0.78rem; color: #64748b; flex: 1; }
  .interval-btns { display:flex; gap:4px; align-items:center; }
  .interval-btn { background:#1a1d27; border:1px solid #2d3148; color:#94a3b8; padding:4px 12px; border-radius:6px; font-size:0.75rem; cursor:pointer; }
  .interval-btn.active,.interval-btn:hover { background:#6366f1; border-color:#6366f1; color:#fff; }
  #hist-close { background: none; border: 1px solid #2d3148; color: #94a3b8; border-radius: 6px; padding: 5px 12px; cursor: pointer; font-size: 0.8rem; }
  #hist-body { display: flex; flex: 1; overflow: hidden; }
  #hist-charts { flex: 1; display: flex; flex-direction: column; padding: 12px; gap: 8px; min-width: 0; }
  #hist-stats { display: flex; gap: 16px; flex-shrink: 0; padding: 0 4px 4px; flex-wrap: wrap; }
  .stat-item { font-size: 0.75rem; color: #94a3b8; } .stat-item span { color: #e2e8f0; font-weight: 600; }
  #price-chart { flex: 3; min-height: 0; }
  #vol-chart { flex: 1; min-height: 0; display: none; }
  #hist-company-panel { width: 300px; min-width: 300px; border-left: 1px solid #2d3148; overflow-y: auto; background: #131620; }
  #hist-company-divider { font-size: 0.68rem; text-transform: uppercase; letter-spacing: .06em; color: #475569; padding: 10px 14px 6px; border-bottom: 1px solid #1e2235; }
  #hist-company { padding: 0 0 16px; }
  .section { border-bottom: 1px solid #1e2235; }
  .section-header { display: flex; align-items: center; justify-content: space-between; padding: 9px 14px; cursor: pointer; font-size: 0.78rem; color: #94a3b8; font-weight: 600; }
  .section-header:hover { background: #1a1d27; }
  .section-chevron { font-size: 0.65rem; transition: transform .2s; }
  .section.open .section-chevron { transform: rotate(90deg); }
  .field-grid { display: none; grid-template-columns: 1fr; gap: 0; padding: 4px 14px 10px; }
  .section.open .field-grid { display: grid; }
  .field { padding: 4px 0; display: flex; flex-direction: column; gap: 2px; border-bottom: 1px solid #1a1d27; }
  .field label { font-size: 0.65rem; color: #64748b; text-transform: uppercase; letter-spacing: .04em; }
  .field span { font-size: 0.8rem; color: #e2e8f0; word-break: break-word; white-space: normal; }
  .summary-text { font-size: 0.75rem; color: #94a3b8; line-height: 1.5; max-height: 100px; overflow-y: auto; }
  .guru-sub-nav { background: #131620; border-bottom: 1px solid #2d3148; padding: 0 24px; display: flex; gap: 2px; flex-shrink: 0; }
  .guru-sub-tab { font-size: 0.82rem; padding: 9px 18px; text-decoration: none; color: #64748b; border-bottom: 2px solid transparent; margin-bottom: -1px; transition: color .15s; }
  .guru-sub-tab:hover { color: #e2e8f0; }
  .guru-sub-tab.active { color: #818cf8; border-bottom-color: #3730a3; font-weight: 600; }
</style>
</head>
<body>
<header>
  <h1>S&amp;P 500 Guru Investing</h1>
  <nav class="nav-links">
    <a href="/" class="nav-link">Table</a>
    <a href="/screener" class="nav-link">Screener</a>
    <a href="/gurus" class="nav-link active">Guru Investing</a>
    <a href="/recommendations" class="nav-link">Recommendations</a>
  </nav>
</header>
<div class="guru-sub-nav">
  <a href="/gurus" class="guru-sub-tab">Portfolio</a>
  <a href="/leaderboard" class="guru-sub-tab">Leaderboard</a>
  <a href="/stock-picks" class="guru-sub-tab active">Stock Picks</a>
</div>
<div id="loading-bar"></div>

<div id="filter-bar">
  <input id="search" type="text" placeholder="Search ticker or company…">
  <select id="sector-filter"><option value="">All Sectors</option></select>
  <select id="min-gurus">
    <option value="1">1+ Gurus</option>
    <option value="2" selected>2+ Gurus</option>
    <option value="3">3+ Gurus</option>
    <option value="4">4+ Gurus</option>
    <option value="5">5+ Gurus</option>
  </select>
  <span id="result-count"></span>
</div>

<div id="main">
  <h2 class="page-title">Stock Conviction Leaderboard</h2>
  <p class="page-sub" id="page-sub">Stocks held as equity positions across guru portfolios, ranked by number of gurus. Loading…</p>
  <div style="overflow-x:auto">
    <table id="stock-dt" style="width:100%">
      <thead><tr>
        <th class="num">#</th>
        <th>Ticker</th>
        <th>Company</th>
        <th>Sector</th>
        <th class="num">Gurus</th>
        <th>Held By</th>
        <th class="num">Total Value</th>
        <th class="num">Total Shares</th>
        <th class="num">Price</th>
        <th class="num">Mkt Cap</th>
        <th class="num">P/E</th>
        <th>Rec</th>
      </tr></thead>
      <tbody id="stock-body"></tbody>
    </table>
  </div>
</div>

<!-- Chart popup -->
<div id="hist-overlay"></div>
<div id="hist-popup">
  <div id="hist-header">
    <div id="hist-title">—</div>
    <div id="hist-meta"></div>
    <div class="interval-btns">
      <button class="interval-btn active" data-interval="1d">Daily</button>
      <button class="interval-btn" data-interval="5m">5 Min</button>
    </div>
    <button id="hist-close">✕ Close</button>
  </div>
  <div id="hist-body">
    <div id="hist-charts">
      <div id="hist-stats"></div>
      <div id="price-chart"></div>
      <div id="vol-chart"></div>
    </div>
    <div id="hist-company-panel">
      <div id="hist-company-divider">Company Details</div>
      <div id="hist-company"></div>
    </div>
  </div>
</div>

<script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
<script src="https://cdn.datatables.net/1.13.8/js/jquery.dataTables.min.js"></script>
<script src="https://unpkg.com/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js"></script>
<script>
function fmtCap(v) {
  if (v == null) return '—';
  const a = Math.abs(v);
  if (a >= 1e12) return '$' + (v/1e12).toFixed(2) + 'T';
  if (a >= 1e9)  return '$' + (v/1e9).toFixed(2) + 'B';
  if (a >= 1e6)  return '$' + (v/1e6).toFixed(1) + 'M';
  return '$' + v.toLocaleString();
}
function n(v) { return v ?? ''; }
function recBadge(v) {
  if (!v || v === 'none') return '<span class="rec-none">—</span>';
  const map = {strong_buy:'Strong Buy',buy:'Buy',hold:'Hold',sell:'Sell',strong_sell:'Strong Sell'};
  const cls = {strong_buy:'rec-strong-buy',buy:'rec-buy',hold:'rec-hold',sell:'rec-sell',strong_sell:'rec-sell'};
  return `<span class="${cls[v]||'rec-hold'}">${map[v]||v}</span>`;
}

let allData = [];
let dt = null;

fetch('/api/guru/stock-leaderboard').then(r => r.json()).then(resp => {
  document.getElementById('loading-bar').style.width = '100%';
  setTimeout(() => { document.getElementById('loading-bar').style.display = 'none'; }, 400);

  allData = resp.items || [];
  const loaded = resp.loaded_gurus || 0;
  const total  = resp.total_gurus  || 0;
  document.getElementById('page-sub').textContent =
    `Equity positions across ${loaded} of ${total} guru portfolios · ranked by conviction (guru count). ` +
    (resp.source === 'cache' ? 'From in-memory cache — visit Guru Investing to populate DB.' : 'From database.');

  // Populate sector dropdown
  const sectors = [...new Set(allData.map(d => d.sector).filter(Boolean))].sort();
  const sel = document.getElementById('sector-filter');
  sectors.forEach(s => { const o = document.createElement('option'); o.value = s; o.textContent = s; sel.appendChild(o); });

  renderTable();

  document.getElementById('search').addEventListener('input', () => { if (dt) dt.search(document.getElementById('search').value).draw(); updateCount(); });
  document.getElementById('sector-filter').addEventListener('change', applyFilters);
  document.getElementById('min-gurus').addEventListener('change', applyFilters);
}).catch(err => {
  document.getElementById('page-sub').textContent = 'Error loading data: ' + err.message;
});

function applyFilters() {
  const sector   = document.getElementById('sector-filter').value;
  const minGurus = parseInt(document.getElementById('min-gurus').value) || 1;
  const search   = document.getElementById('search').value.toLowerCase();

  const filtered = allData.filter(d =>
    d.guru_count >= minGurus &&
    (!sector || d.sector === sector) &&
    (!search || (d.ticker||'').toLowerCase().includes(search) || (d.longName||'').toLowerCase().includes(search))
  );
  rebuildTable(filtered);
}

function renderTable() {
  const minGurus = parseInt(document.getElementById('min-gurus').value) || 1;
  rebuildTable(allData.filter(d => d.guru_count >= minGurus));
}

function rebuildTable(data) {
  if (dt) { try { dt.destroy(); } catch(e){} dt = null; }
  const tbody = document.getElementById('stock-body');
  tbody.innerHTML = '';

  data.forEach((d, i) => {
    const pills = (d.gurus || []).map(g =>
      `<span class="guru-pill" style="background:${g.color}22;color:${g.color};border:1px solid ${g.color}44" title="${g.slug}">${g.short}</span>`
    ).join('');

    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td class="num">${i + 1}</td>
      <td><span class="ticker-badge" onclick="openHistory('${d.ticker}')">${d.ticker}</span></td>
      <td>${d.longName || '—'}</td>
      <td>${d.sector ? `<span class="sector-tag">${d.sector}</span>` : '—'}</td>
      <td class="num" data-order="${d.guru_count}"><span class="guru-count-badge">${d.guru_count}</span></td>
      <td>${pills}</td>
      <td class="num" data-order="${d.total_value}">${fmtCap(d.total_value)}</td>
      <td class="num" data-order="${d.total_shares}">${d.total_shares ? d.total_shares.toLocaleString() : '—'}</td>
      <td class="num" data-order="${n(d.currentPrice)}">${d.currentPrice != null ? '$' + d.currentPrice.toFixed(2) : '—'}</td>
      <td class="num" data-order="${n(d.marketCap)}">${fmtCap(d.marketCap)}</td>
      <td class="num" data-order="${n(d.trailingPE)}">${d.trailingPE != null ? d.trailingPE.toFixed(1) : '—'}</td>
      <td>${recBadge(d.recommendationKey)}</td>
    `;
    tbody.appendChild(tr);
  });

  dt = $('#stock-dt').DataTable({
    paging: false, dom: 'ti',
    order: [[4, 'desc'], [6, 'desc']],
    columnDefs: [{ targets: [4,6,7,8,9,10], type: 'num' }, { orderable: false, targets: [5] }]
  });
  updateCount();
}

function updateCount() {
  const shown = dt ? dt.rows({search:'applied'}).count() : 0;
  document.getElementById('result-count').textContent = shown + ' stocks';
}

// Chart popup
let priceChart = null, volChart = null, priceSeries = null, volSeries = null;
let _histTicker = null;

function openHistory(ticker) {
  _histTicker = ticker;
  document.getElementById('hist-title').textContent = ticker;
  document.getElementById('hist-meta').textContent = 'Loading…';
  document.getElementById('hist-stats').innerHTML = '';
  document.getElementById('hist-company').innerHTML = '<div style="padding:16px;color:#64748b;font-size:0.8rem">Loading…</div>';
  document.querySelectorAll('.interval-btn').forEach(b => b.classList.remove('active'));
  document.querySelector('.interval-btn[data-interval="1d"]').classList.add('active');
  document.getElementById('hist-popup').classList.add('open');
  document.getElementById('hist-overlay').classList.add('active');
  fetch(`/api/company/${ticker}`).then(r => r.json()).then(renderCompanyPanel).catch(() => {});
  _loadHistChart(ticker, '1d');
}

function _loadHistChart(ticker, interval) {
  document.getElementById('hist-meta').textContent = 'Loading…';
  fetch(`/api/history/${ticker}?interval=${interval}`)
    .then(r => r.json())
    .then(rows => _renderHistChart(rows, interval))
    .catch(err => { document.getElementById('hist-meta').textContent = 'Error: ' + err.message; });
}

function _renderHistChart(data, interval) {
  const rows = Array.isArray(data) ? data : (data.rows || []);
  if (!rows.length) { document.getElementById('hist-meta').textContent = 'No data'; return; }
  if (interval !== '5m') {
    const high = Math.max(...rows.map(r => r.high)), low = Math.min(...rows.map(r => r.low)), last = rows[rows.length-1];
    document.getElementById('hist-meta').textContent = `${rows.length} trading days  ·  ${rows[0].time} → ${rows[rows.length-1].time}`;
    document.getElementById('hist-stats').innerHTML = `
      <div class="stat-item">Last Close <span>$${last.close.toFixed(2)}</span></div>
      <div class="stat-item">52W High <span>$${high.toFixed(2)}</span></div>
      <div class="stat-item">52W Low <span>$${low.toFixed(2)}</span></div>
      <div class="stat-item">Last Volume <span>${(last.volume/1e6).toFixed(2)}M</span></div>`;
  } else {
    document.getElementById('hist-meta').textContent = `${rows.length} bars (5 min)`;
    document.getElementById('hist-stats').innerHTML = '';
  }
  const pEl = document.getElementById('price-chart'), vEl = document.getElementById('vol-chart');
  pEl.innerHTML = ''; vEl.innerHTML = ''; vEl.style.display = 'block';
  if (priceChart) { priceChart.remove(); priceChart = null; }
  if (volChart)   { volChart.remove();   volChart = null; }
  const opts = { autoSize: true, layout: { background: { color: '#1a1d27' }, textColor: '#94a3b8' }, grid: { vertLines: { color: '#1e2235' }, horzLines: { color: '#1e2235' } }, timeScale: { borderColor: '#2d3148', timeVisible: true }, rightPriceScale: { borderColor: '#2d3148' } };
  priceChart = LightweightCharts.createChart(pEl, opts);
  priceSeries = priceChart.addCandlestickSeries({ upColor: '#34d399', downColor: '#f87171', borderUpColor: '#34d399', borderDownColor: '#f87171', wickUpColor: '#34d399', wickDownColor: '#f87171' });
  priceSeries.setData(rows.map(r => ({ time:r.time, open:r.open, high:r.high, low:r.low, close:r.close })));
  priceChart.timeScale().fitContent();
  volChart = LightweightCharts.createChart(vEl, opts);
  volSeries = volChart.addHistogramSeries({ color: '#3730a3', priceFormat: { type: 'volume' } });
  volSeries.setData(rows.map((r, i) => ({ time: r.time, value: r.volume, color: i > 0 && r.close >= rows[i-1].close ? '#34d39966' : '#f8717166' })));
  volChart.timeScale().fitContent();
  priceChart.timeScale().subscribeVisibleLogicalRangeChange(range => { if (range) volChart.timeScale().setVisibleLogicalRange(range); });
  volChart.timeScale().subscribeVisibleLogicalRangeChange(range => { if (range) priceChart.timeScale().setVisibleLogicalRange(range); });
}

function renderCompanyPanel(data) {
  const container = document.getElementById('hist-company');
  container.innerHTML = '';
  if (!data || !data.sections) return;
  for (const [sectionName, fields] of Object.entries(data.sections)) {
    if (!fields || Object.keys(fields).length === 0) continue;
    const sec = document.createElement('div'); sec.className = 'section';
    sec.innerHTML = `<div class="section-header"><span>${sectionName}</span><span class="section-chevron">▶</span></div>`;
    const grid = document.createElement('div'); grid.className = 'field-grid';
    for (const [key, val] of Object.entries(fields)) {
      const fEl = document.createElement('div'); fEl.className = 'field';
      const displayVal = val == null ? '—' : (typeof val === 'object' ? JSON.stringify(val) : (key.toLowerCase().includes('summary') ? `<div class="summary-text">${String(val)}</div>` : String(val)));
      fEl.innerHTML = `<label>${key}</label><span>${displayVal}</span>`;
      grid.appendChild(fEl);
    }
    sec.appendChild(grid);
    sec.querySelector('.section-header').addEventListener('click', () => sec.classList.toggle('open'));
    container.appendChild(sec);
  }
}

document.querySelectorAll('.interval-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.interval-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    if (_histTicker) _loadHistChart(_histTicker, btn.dataset.interval);
  });
});

function closeHistory() {
  document.getElementById('hist-popup').classList.remove('open');
  document.getElementById('hist-overlay').classList.remove('active');
}
document.getElementById('hist-close').addEventListener('click', closeHistory);
document.getElementById('hist-overlay').addEventListener('click', closeHistory);

</script>
</body>
</html>"""


LEADERBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>S&amp;P 500 – Guru Leaderboard</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: system-ui, sans-serif; background: #0f1117; color: #e2e8f0; min-height: 100vh; display: flex; flex-direction: column; }
  header { background: #1a1d27; border-bottom: 1px solid #2d3148; padding: 12px 24px; display: flex; align-items: center; gap: 16px; flex-shrink: 0; }
  header h1 { font-size: 1.25rem; font-weight: 700; color: #f8fafc; white-space: nowrap; }
  .nav-links { display: flex; gap: 4px; }
  .nav-link { font-size: 0.82rem; padding: 5px 14px; border-radius: 6px; text-decoration: none; color: #94a3b8; transition: background .15s, color .15s; }
  .nav-link:hover { background: #2d3148; color: #e2e8f0; }
  .nav-link.active { background: #3730a3; color: #fff; font-weight: 600; }

  #page-wrap { flex: 1; padding: 24px; max-width: 1300px; width: 100%; margin: 0 auto; }
  .page-title { font-size: 1.4rem; font-weight: 700; color: #f8fafc; margin-bottom: 4px; }
  .page-sub { font-size: 0.82rem; color: #64748b; margin-bottom: 20px; }

  #progress-wrap { background: #1a1d27; border: 1px solid #2d3148; border-radius: 8px; padding: 10px 16px; margin-bottom: 20px; display: flex; align-items: center; gap: 16px; }
  #progress-label { font-size: 0.8rem; color: #94a3b8; white-space: nowrap; min-width: 200px; }
  #progress-bar-track { flex: 1; height: 6px; background: #2d3148; border-radius: 3px; overflow: hidden; }
  #progress-bar-fill { height: 100%; width: 0%; background: #3730a3; border-radius: 3px; transition: width .3s ease; }
  #progress-done { font-size: 0.78rem; color: #34d399; white-space: nowrap; min-width: 80px; text-align: right; }

  .table-wrap { background: #131620; border: 1px solid #2d3148; border-radius: 10px; overflow: hidden; }
  table { width: 100%; border-collapse: collapse; }
  thead th { background: #1a1d27; color: #64748b; font-size: 0.72rem; text-transform: uppercase; letter-spacing: .06em; padding: 11px 14px; border-bottom: 1px solid #2d3148; white-space: nowrap; }
  thead th.sortable { cursor: pointer; user-select: none; }
  thead th.sortable:hover { color: #e2e8f0; }
  thead th.sort-asc::after { content: ' ↑'; color: #818cf8; }
  thead th.sort-desc::after { content: ' ↓'; color: #818cf8; }
  thead th.num { text-align: right; }
  tbody tr { border-bottom: 1px solid #1e2235; transition: background .1s; cursor: pointer; }
  tbody tr:last-child { border-bottom: none; }
  tbody tr:hover { background: #1a1d27; }
  tbody td { padding: 12px 14px; font-size: 0.83rem; white-space: nowrap; }
  td.num { text-align: right; }

  .rank-cell { font-size: 1.05rem; font-weight: 700; color: #3730a3; width: 44px; text-align: center !important; }
  .rank-1 { color: #f59e0b; }
  .rank-2 { color: #94a3b8; }
  .rank-3 { color: #cd7c2f; }

  .guru-name { font-weight: 700; color: #f8fafc; font-size: 0.88rem; }
  .guru-fund { font-size: 0.72rem; color: #64748b; margin-top: 2px; }
  .style-pill { display: inline-block; font-size: 0.65rem; font-weight: 700; padding: 2px 8px; border-radius: 99px; letter-spacing: .04em; text-transform: uppercase; white-space: nowrap; }

  .pos-change { color: #34d399; font-weight: 700; }
  .neg-change { color: #f87171; font-weight: 700; }
  .change-bar-wrap { display: flex; align-items: center; gap: 8px; justify-content: flex-end; }
  .change-bar { height: 4px; border-radius: 2px; }
  .change-bar.pos { background: #34d399; }
  .change-bar.neg { background: #f87171; }

  .na-cell { color: #2d3148; }
  .skeleton { display: inline-block; background: #1e2235; border-radius: 4px; height: 12px; vertical-align: middle; animation: pulse 1.4s ease-in-out infinite; }
  @keyframes pulse { 0%, 100% { opacity: .3; } 50% { opacity: .8; } }
  .loading-row td { color: #475569; text-align: center; padding: 32px; font-size: 0.85rem; }
  .guru-sub-nav { background: #131620; border-bottom: 1px solid #2d3148; padding: 0 24px; display: flex; gap: 2px; flex-shrink: 0; }
  .guru-sub-tab { font-size: 0.82rem; padding: 9px 18px; text-decoration: none; color: #64748b; border-bottom: 2px solid transparent; margin-bottom: -1px; transition: color .15s; }
  .guru-sub-tab:hover { color: #e2e8f0; }
  .guru-sub-tab.active { color: #818cf8; border-bottom-color: #3730a3; font-weight: 600; }
</style>
</head>
<body>
<header>
  <h1>S&amp;P 500 Guru Investing</h1>
  <nav class="nav-links">
    <a href="/" class="nav-link">Table</a>
    <a href="/screener" class="nav-link">Screener</a>
    <a href="/gurus" class="nav-link active">Guru Investing</a>
    <a href="/recommendations" class="nav-link">Recommendations</a>
  </nav>
</header>
<div class="guru-sub-nav">
  <a href="/gurus" class="guru-sub-tab">Portfolio</a>
  <a href="/leaderboard" class="guru-sub-tab active">Leaderboard</a>
  <a href="/stock-picks" class="guru-sub-tab">Stock Picks</a>
</div>

<div id="page-wrap">
  <h2 class="page-title">Guru Portfolio Leaderboard</h2>
  <p class="page-sub">Ranked by estimated portfolio return since last 13F filing date. Calculated as shares held × current price vs reported value at filing.</p>

  <div id="progress-wrap">
    <div id="progress-label">Fetching portfolios from SEC EDGAR…  0 / 0</div>
    <div id="progress-bar-track"><div id="progress-bar-fill"></div></div>
    <div id="progress-done"></div>
  </div>

  <div class="table-wrap">
    <table id="lb-table">
      <thead>
        <tr>
          <th style="width:52px;text-align:center">#</th>
          <th>Investor</th>
          <th>Style</th>
          <th class="num sortable" data-sort="filing_date">Filing Date</th>
          <th class="num sortable" data-sort="days">Days Since</th>
          <th class="num sortable" data-sort="filing_val">Value at Filing</th>
          <th class="num sortable" data-sort="curr_val">Current Est.</th>
          <th class="num sortable" data-sort="chg_abs">Change $</th>
          <th class="num sortable sort-desc" data-sort="chg_pct">Change %</th>
        </tr>
      </thead>
      <tbody id="lb-body">
        <tr class="loading-row"><td colspan="9">Loading guru portfolios…</td></tr>
      </tbody>
    </table>
  </div>
</div>

<script>
let rows = [];
let total = 0, done = 0;
let sortCol = 'chg_pct', sortDir = -1;

function fmtCap(v) {
  if (v == null || v === 0) return '—';
  const a = Math.abs(v);
  if (a >= 1e12) return '$' + (v/1e12).toFixed(2) + 'T';
  if (a >= 1e9)  return '$' + (v/1e9).toFixed(2) + 'B';
  if (a >= 1e6)  return '$' + (v/1e6).toFixed(1) + 'M';
  return '$' + v.toLocaleString();
}

function updateProgress() {
  const pct = total > 0 ? done / total * 100 : 0;
  document.getElementById('progress-bar-fill').style.width = pct + '%';
  document.getElementById('progress-label').textContent =
    done < total ? `Fetching portfolios from SEC EDGAR…  ${done} / ${total}` : `All ${total} guru portfolios loaded`;
  document.getElementById('progress-done').textContent = done === total && total > 0 ? '✓ Done' : '';
}

function getMaxAbsPct() {
  let m = 0;
  for (const r of rows) if (r.chg_pct != null) m = Math.max(m, Math.abs(r.chg_pct));
  return m || 1;
}

function sortRows() {
  const col = sortCol;
  rows.sort((a, b) => {
    const av = a[col], bv = b[col];
    if (av == null && bv == null) return 0;
    if (av == null) return 1;
    if (bv == null) return -1;
    if (typeof av === 'string') return sortDir * av.localeCompare(bv);
    return sortDir * (av - bv);
  });
}

function skel(w) { return `<span class="skeleton" style="width:${w}px">&nbsp;</span>`; }

function renderTable() {
  sortRows();
  const maxPct = getMaxAbsPct();
  const tbody = document.getElementById('lb-body');
  tbody.innerHTML = '';

  rows.forEach((r, i) => {
    const rank = i + 1;
    const rankLabel = rank === 1 ? '🥇' : rank === 2 ? '🥈' : rank === 3 ? '🥉' : rank;
    const rankCls = rank <= 3 ? 'rank-' + rank : '';

    const stylePill = `<span class="style-pill" style="background:${r.color}22;color:${r.color};border:1px solid ${r.color}55">${r.style}</span>`;

    let chgPctHtml, chgAbsHtml, filingValHtml, currValHtml;
    if (!r.loaded) {
      chgPctHtml  = skel(64);
      chgAbsHtml  = skel(56);
      filingValHtml = skel(56);
      currValHtml   = skel(56);
    } else {
      filingValHtml = r.filing_val ? fmtCap(r.filing_val) : '<span class="na-cell">N/A</span>';
      currValHtml   = r.curr_val   ? fmtCap(r.curr_val)   : '<span class="na-cell">N/A</span>';
      if (r.chg_pct != null) {
        const cls  = r.chg_pct >= 0 ? 'pos-change' : 'neg-change';
        const barW = Math.round(Math.abs(r.chg_pct) / maxPct * 56 + 4);
        const sign = r.chg_pct >= 0 ? '+' : '';
        chgPctHtml = `<div class="change-bar-wrap">
          <div class="change-bar ${r.chg_pct>=0?'pos':'neg'}" style="width:${barW}px"></div>
          <span class="${cls}">${sign}${r.chg_pct.toFixed(2)}%</span>
        </div>`;
      } else {
        chgPctHtml = '<span class="na-cell">—</span>';
      }
      if (r.chg_abs != null) {
        const cls  = r.chg_abs >= 0 ? 'pos-change' : 'neg-change';
        const sign = r.chg_abs >= 0 ? '+' : '-';
        chgAbsHtml = `<span class="${cls}">${sign}${fmtCap(Math.abs(r.chg_abs))}</span>`;
      } else {
        chgAbsHtml = '<span class="na-cell">—</span>';
      }
    }

    const days = r.days != null ? r.days + 'd' : (r.loaded ? '<span class="na-cell">—</span>' : skel(32));
    const filingDate = r.filing_date || (r.loaded ? '<span class="na-cell">—</span>' : skel(64));

    const tr = document.createElement('tr');
    tr.onclick = () => { window.location = '/gurus?guru=' + r.slug; };
    tr.innerHTML = `
      <td class="rank-cell ${rankCls}">${rankLabel}</td>
      <td>
        <div class="guru-name">${r.name}</div>
        <div class="guru-fund">${r.fund}</div>
      </td>
      <td>${stylePill}</td>
      <td class="num">${filingDate}</td>
      <td class="num">${days}</td>
      <td class="num">${filingValHtml}</td>
      <td class="num">${currValHtml}</td>
      <td class="num">${chgAbsHtml}</td>
      <td class="num">${chgPctHtml}</td>
    `;
    tbody.appendChild(tr);
  });
}

// Column sort
document.querySelectorAll('thead th.sortable').forEach(th => {
  th.addEventListener('click', () => {
    const col = th.dataset.sort;
    if (sortCol === col) {
      sortDir *= -1;
    } else {
      sortCol = col;
      sortDir = (col === 'filing_date') ? 1 : -1;
    }
    document.querySelectorAll('thead th').forEach(t => t.classList.remove('sort-asc', 'sort-desc'));
    th.classList.add(sortDir > 0 ? 'sort-asc' : 'sort-desc');
    renderTable();
  });
});

// Bootstrap: load guru list then fire all holdings fetches in parallel
fetch('/api/guru/list').then(r => r.json()).then(list => {
  total = list.length;
  rows = list.map(g => ({
    slug: g.slug, name: g.name, fund: g.fund, style: g.style, color: g.color,
    filing_date: null, days: null, filing_val: null, curr_val: null,
    chg_abs: null, chg_pct: null, loaded: false,
  }));
  updateProgress();
  renderTable();

  list.forEach(g => {
    fetch(`/api/guru/${g.slug}/holdings`)
      .then(r => r.json())
      .then(data => {
        const row = rows.find(r => r.slug === g.slug);
        if (!row) return;
        row.loaded = true;
        row.filing_date = data.filing_date || null;
        if (data.filing_date) {
          row.days = Math.floor((Date.now() - new Date(data.filing_date)) / (1000*60*60*24));
        }
        row.filing_val = data.filing_total_value  || null;
        row.curr_val   = data.current_total_value || null;
        if (row.filing_val && row.curr_val) {
          row.chg_abs = row.curr_val - row.filing_val;
          row.chg_pct = row.chg_abs / row.filing_val * 100;
        }
        done++;
        updateProgress();
        renderTable();
      })
      .catch(() => {
        const row = rows.find(r => r.slug === g.slug);
        if (row) row.loaded = true;
        done++;
        updateProgress();
        renderTable();
      });
  });
});
</script>
</body>
</html>"""


GURUS_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>S&amp;P 500 – Guru Investing</title>
<link rel="stylesheet" href="https://cdn.datatables.net/1.13.8/css/jquery.dataTables.min.css">
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: system-ui, sans-serif; background: #0f1117; color: #e2e8f0; min-height: 100vh; display: flex; flex-direction: column; }
  header { background: #1a1d27; border-bottom: 1px solid #2d3148; padding: 12px 24px; display: flex; align-items: center; gap: 16px; flex-shrink: 0; }
  header h1 { font-size: 1.25rem; font-weight: 700; color: #f8fafc; white-space: nowrap; }
  .nav-links { display: flex; gap: 4px; }
  .nav-link { font-size: 0.82rem; padding: 5px 14px; border-radius: 6px; text-decoration: none; color: #94a3b8; transition: background .15s, color .15s; }
  .nav-link:hover { background: #2d3148; color: #e2e8f0; }
  .nav-link.active { background: #3730a3; color: #fff; font-weight: 600; }

  /* Guru selector bar */
  #guru-selector-bar { background: #131620; border-bottom: 1px solid #2d3148; padding: 12px 24px; display: flex; align-items: center; gap: 14px; flex-shrink: 0; flex-wrap: wrap; }
  #guru-selector-bar label { font-size: 0.78rem; color: #64748b; font-weight: 600; text-transform: uppercase; letter-spacing: .05em; white-space: nowrap; }
  #guru-select { background: #1a1d27; border: 1px solid #2d3148; color: #e2e8f0; padding: 8px 14px; border-radius: 8px; font-size: 0.85rem; min-width: 320px; cursor: pointer; outline: none; }
  #guru-select:focus { border-color: #3730a3; }
  #guru-select option, #guru-select optgroup { background: #1a1d27; color: #e2e8f0; }
  #guru-style-pill { display: none; font-size: 0.72rem; font-weight: 700; padding: 3px 12px; border-radius: 99px; letter-spacing: .04em; text-transform: uppercase; }

  /* Main layout */
  #guru-layout { display: flex; flex: 1; overflow: hidden; }

  /* Left sidebar */
  #guru-sidebar { width: 320px; min-width: 320px; background: #131620; border-right: 1px solid #2d3148; overflow-y: auto; padding: 20px; display: flex; flex-direction: column; gap: 16px; }
  #guru-placeholder { color: #475569; font-size: 0.85rem; text-align: center; padding: 40px 0; }
  .style-badge { display: inline-block; font-size: 0.7rem; font-weight: 700; padding: 3px 10px; border-radius: 99px; letter-spacing: .05em; text-transform: uppercase; margin-bottom: 8px; }
  #guru-card h2 { font-size: 1.2rem; font-weight: 700; color: #f8fafc; }
  #guru-fund { font-size: 0.8rem; color: #64748b; margin-top: 2px; margin-bottom: 12px; }
  #guru-quote { font-size: 0.78rem; color: #94a3b8; font-style: italic; line-height: 1.55; border-left: 3px solid #2d3148; padding-left: 10px; margin-bottom: 12px; }
  #guru-desc { font-size: 0.8rem; color: #94a3b8; line-height: 1.6; }
  .rules-panel { margin-top: 4px; }
  .rules-panel h3 { font-size: 0.72rem; text-transform: uppercase; letter-spacing: .06em; color: #64748b; margin-bottom: 10px; }
  .rule-item { display: flex; justify-content: space-between; align-items: center; padding: 6px 0; border-bottom: 1px solid #1e2235; font-size: 0.78rem; }
  .rule-label { color: #94a3b8; }
  .rule-value { color: #a5b4fc; font-weight: 600; font-family: monospace; font-size: 0.8rem; }
  .cat-rule { color: #fbbf24; font-size: 0.75rem; padding: 5px 0; border-bottom: 1px solid #1e2235; }

  /* Right main area */
  #guru-main { flex: 1; display: flex; flex-direction: column; overflow: hidden; }

  /* Tabs */
  .tab-bar { display: flex; gap: 2px; padding: 12px 20px 0; background: #0f1117; border-bottom: 1px solid #2d3148; flex-shrink: 0; }
  .tab-btn { padding: 8px 20px; border-radius: 6px 6px 0 0; border: 1px solid transparent; background: transparent; color: #64748b; font-size: 0.85rem; cursor: pointer; transition: all .15s; }
  .tab-btn:hover { color: #e2e8f0; background: #1a1d27; }
  .tab-btn.active { background: #1a1d27; border-color: #2d3148 #2d3148 #1a1d27; color: #e2e8f0; font-weight: 600; }
  .tab-badge { display: inline-block; background: #2d3148; color: #94a3b8; border-radius: 99px; font-size: 0.7rem; padding: 1px 7px; margin-left: 6px; vertical-align: middle; }
  .tab-panel { display: none; flex: 1; overflow: auto; padding: 16px 20px; }
  .tab-panel.active { display: flex; flex-direction: column; gap: 12px; }

  /* Status bar */
  .status-bar { font-size: 0.78rem; color: #64748b; display: flex; align-items: center; gap: 8px; flex-shrink: 0; }
  .status-bar .filing-info { background: #1a1d27; border: 1px solid #2d3148; border-radius: 6px; padding: 4px 12px; }
  .overlap-info { color: #fbbf24; font-size: 0.75rem; }

  /* Tables */
  table.dataTable { border-collapse: collapse; width: 100% !important; }
  table.dataTable thead th { background: #1a1d27; color: #94a3b8; font-size: 0.72rem; text-transform: uppercase; letter-spacing: .05em; border-bottom: 1px solid #2d3148; padding: 9px 10px; cursor: pointer; }
  table.dataTable tbody tr { border-bottom: 1px solid #1e2235; transition: background .1s; }
  table.dataTable tbody tr:hover { background: #1a1d27; }
  table.dataTable tbody td { padding: 8px 10px; font-size: 0.8rem; white-space: nowrap; }
  th.num, td.num { text-align: right !important; }
  .ticker-link { font-weight: 700; color: #818cf8; font-family: monospace; font-size: 0.88rem; cursor: pointer; text-decoration: underline dotted; }
  .ticker-link:hover { color: #a5b4fc; }
  .ticker-none { color: #475569; font-family: monospace; font-size: 0.82rem; }
  .sector-tag { background: #1e2235; color: #94a3b8; padding: 2px 6px; border-radius: 4px; font-size: 0.72rem; }
  .rec-strong-buy { color: #059669; font-weight: 700; }
  .rec-buy { color: #34d399; font-weight: 600; }
  .rec-hold { color: #fbbf24; font-weight: 600; }
  .rec-sell { color: #f87171; font-weight: 600; }
  .rec-none { color: #475569; }
  .match-yes { color: #34d399; font-size: 0.72rem; font-weight: 700; }
  .held-yes { background: #14532d; color: #86efac; font-size: 0.7rem; font-weight: 700; padding: 2px 6px; border-radius: 4px; }
  .pos-change { color: #34d399; } .neg-change { color: #f87171; }
  .type-badge { font-size: 0.68rem; font-weight: 700; padding: 2px 6px; border-radius: 4px; font-family: monospace; }
  .type-eq   { background: #1e3a5f; color: #60a5fa; }
  .type-call { background: #14532d; color: #86efac; }
  .type-put  { background: #450a0a; color: #fca5a5; }
  .chg-badge { font-size: 0.7rem; font-weight: 700; padding: 2px 7px; border-radius: 4px; white-space: nowrap; }
  .chg-new     { background: #713f12; color: #fcd34d; }
  .chg-added   { background: #14532d; color: #4ade80; }
  .chg-reduced { background: #450a0a; color: #f87171; }
  .chg-held    { color: #475569; font-size: 0.72rem; }
  .perf-panel { margin-top: 16px; border-top: 1px solid #2d3148; padding-top: 12px; }
  .perf-panel h3 { font-size: 0.72rem; text-transform: uppercase; letter-spacing: .06em; color: #64748b; margin-bottom: 10px; }
  .perf-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
  .perf-item { display: flex; flex-direction: column; gap: 2px; }
  .perf-item label { font-size: 0.65rem; color: #64748b; text-transform: uppercase; letter-spacing: .04em; }
  .perf-item span  { font-size: 0.88rem; font-weight: 600; color: #e2e8f0; }
  .perf-item span.pos-change { color: #34d399; }
  .perf-item span.neg-change { color: #f87171; }
  .perf-days-val   { color: #94a3b8 !important; font-size: 0.82rem !important; }
  .loading-msg { color: #64748b; font-size: 0.85rem; padding: 24px 0; text-align: center; }
  .error-msg { color: #f87171; font-size: 0.85rem; padding: 24px 0; }
  .dataTables_wrapper { color: #e2e8f0; }
  .dataTables_filter, .dataTables_length { display: none; }
  .dataTables_info { font-size: 0.75rem; color: #64748b; padding: 8px 0; }
  table.dataTable thead .sorting::after, table.dataTable thead .sorting_asc::after,
  table.dataTable thead .sorting_desc::after { color: #64748b; }
  tr.guru-held { background: #0d2218 !important; }
  tr.guru-held:hover { background: #14301f !important; }

  /* Chart popup (same as main page) */
  #hist-overlay { position: fixed; inset: 0; background: rgba(0,0,0,.7); z-index: 100; display: none; }
  #hist-overlay.active { display: block; }
  #hist-popup { position: fixed; top: 5vh; left: 50%; transform: translateX(-50%); width: 90vw; max-width: 1100px; height: 88vh; background: #1a1d27; border: 1px solid #2d3148; border-radius: 12px; z-index: 101; display: none; flex-direction: column; overflow: hidden; }
  #hist-popup.open { display: flex; }
  #hist-header { padding: 14px 20px; border-bottom: 1px solid #2d3148; display: flex; align-items: center; gap: 12px; flex-shrink: 0; }
  #hist-title { font-size: 1.1rem; font-weight: 700; color: #f8fafc; }
  #hist-meta { font-size: 0.78rem; color: #64748b; flex: 1; }
  .interval-btns { display:flex; gap:4px; align-items:center; }
  .interval-btn { background:#1a1d27; border:1px solid #2d3148; color:#94a3b8; padding:4px 12px; border-radius:6px; font-size:0.75rem; cursor:pointer; }
  .interval-btn.active,.interval-btn:hover { background:#6366f1; border-color:#6366f1; color:#fff; }
  #hist-close { background: none; border: 1px solid #2d3148; color: #94a3b8; border-radius: 6px; padding: 5px 12px; cursor: pointer; font-size: 0.8rem; }
  #hist-body { display: flex; flex: 1; overflow: hidden; }
  #hist-charts { flex: 1; display: flex; flex-direction: column; padding: 12px; gap: 8px; min-width: 0; }
  #hist-stats { display: flex; gap: 16px; flex-shrink: 0; padding: 0 4px 4px; flex-wrap: wrap; }
  .stat-item { font-size: 0.75rem; color: #94a3b8; } .stat-item span { color: #e2e8f0; font-weight: 600; }
  #price-chart { flex: 3; min-height: 0; }
  #vol-chart { flex: 1; min-height: 0; display: none; }
  #hist-company-panel { width: 300px; min-width: 300px; border-left: 1px solid #2d3148; overflow-y: auto; background: #131620; }
  #hist-company-divider { font-size: 0.68rem; text-transform: uppercase; letter-spacing: .06em; color: #475569; padding: 10px 14px 6px; border-bottom: 1px solid #1e2235; }
  #hist-company { padding: 0 0 16px; }
  .section { border-bottom: 1px solid #1e2235; }
  .section-header { display: flex; align-items: center; justify-content: space-between; padding: 9px 14px; cursor: pointer; font-size: 0.78rem; color: #94a3b8; font-weight: 600; }
  .section-header:hover { background: #1a1d27; }
  .section-chevron { font-size: 0.65rem; transition: transform .2s; }
  .section.open .section-chevron { transform: rotate(90deg); }
  .field-grid { display: none; grid-template-columns: 1fr; gap: 0; padding: 4px 14px 10px; }
  .section.open .field-grid { display: grid; }
  .field { padding: 4px 0; display: flex; flex-direction: column; gap: 2px; border-bottom: 1px solid #1a1d27; }
  .field label { font-size: 0.65rem; color: #64748b; text-transform: uppercase; letter-spacing: .04em; }
  .field span { font-size: 0.8rem; color: #e2e8f0; word-break: break-word; white-space: normal; }
  .summary-text { font-size: 0.75rem; color: #94a3b8; line-height: 1.5; max-height: 100px; overflow-y: auto; }
  .guru-sub-nav { background: #131620; border-bottom: 1px solid #2d3148; padding: 0 24px; display: flex; gap: 2px; flex-shrink: 0; }
  .guru-sub-tab { font-size: 0.82rem; padding: 9px 18px; text-decoration: none; color: #64748b; border-bottom: 2px solid transparent; margin-bottom: -1px; transition: color .15s; }
  .guru-sub-tab:hover { color: #e2e8f0; }
  .guru-sub-tab.active { color: #818cf8; border-bottom-color: #3730a3; font-weight: 600; }
</style>
</head>
<body>
<header>
  <h1>S&amp;P 500 Guru Investing</h1>
  <nav class="nav-links">
    <a href="/" class="nav-link">Table</a>
    <a href="/screener" class="nav-link">Screener</a>
    <a href="/gurus" class="nav-link active">Guru Investing</a>
    <a href="/recommendations" class="nav-link">Recommendations</a>
  </nav>
</header>
<div class="guru-sub-nav">
  <a href="/gurus" class="guru-sub-tab active">Portfolio</a>
  <a href="/leaderboard" class="guru-sub-tab">Leaderboard</a>
  <a href="/stock-picks" class="guru-sub-tab">Stock Picks</a>
</div>

<div id="guru-selector-bar">
  <label for="guru-select">Investor</label>
  <select id="guru-select">
    <option value="">— Choose a guru investor —</option>
  </select>
  <span id="guru-style-pill"></span>
</div>

<div id="guru-layout">
  <!-- Left: guru card -->
  <aside id="guru-sidebar">
    <div id="guru-placeholder">← Select a guru above to view their 13F holdings and investment strategy</div>
    <div id="guru-card" style="display:none">
      <div id="guru-style-badge" class="style-badge"></div>
      <h2 id="guru-name"></h2>
      <div id="guru-fund"></div>
      <blockquote id="guru-quote"></blockquote>
      <p id="guru-desc"></p>
      <div class="rules-panel">
        <h3>Investment Rules</h3>
        <div id="guru-rules"></div>
      </div>
      <div class="perf-panel" id="guru-perf" style="display:none">
        <h3>Portfolio Performance Since Filing</h3>
        <div class="perf-grid">
          <div class="perf-item">
            <label>Filing Date</label>
            <span id="perf-date" class="perf-days-val">—</span>
          </div>
          <div class="perf-item">
            <label>Days Since Filing</label>
            <span id="perf-days" class="perf-days-val">—</span>
          </div>
          <div class="perf-item">
            <label>Value at Filing</label>
            <span id="perf-filing-val">—</span>
          </div>
          <div class="perf-item">
            <label>Current Est. Value</label>
            <span id="perf-curr-val">—</span>
          </div>
          <div class="perf-item" style="grid-column:1/-1">
            <label>Change Since Filing</label>
            <span id="perf-chg" style="font-size:1.05rem">—</span>
          </div>
        </div>
      </div>
    </div>
  </aside>

  <!-- Right: tabs -->
  <div id="guru-main">
    <div class="tab-bar">
      <button class="tab-btn active" data-tab="holdings">
        Portfolio Holdings <span class="tab-badge" id="badge-holdings">—</span>
      </button>
    </div>

    <!-- Holdings tab -->
    <div id="tab-holdings" class="tab-panel active">
      <div class="status-bar">
        <span id="holdings-filing" class="filing-info">No data loaded</span>
        <span id="holdings-overlap" class="overlap-info"></span>
      </div>
      <div id="holdings-msg" class="loading-msg">Select a guru above to load their latest 13F filing.</div>
      <div id="holdings-wrap" style="display:none; overflow-x:auto;">
        <table id="holdings-dt" style="width:100%">
          <thead><tr>
            <th class="num">#</th>
            <th>Change</th>
            <th>Type</th>
            <th>Ticker</th>
            <th>Company (13F Name)</th>
            <th>Sector</th>
            <th class="num">Position Value</th>
            <th class="num">Weight</th>
            <th class="num">Shares</th>
            <th class="num">Price</th>
            <th class="num">P/E</th>
            <th class="num">ROE</th>
            <th class="num">1Y Return</th>
            <th>Rec</th>
            <th>Rules ✓</th>
          </tr></thead>
          <tbody id="holdings-body"></tbody>
        </table>
      </div>
    </div>

  </div>
</div>

<!-- Chart popup -->
<div id="hist-overlay"></div>
<div id="hist-popup">
  <div id="hist-header">
    <div id="hist-title">—</div>
    <div id="hist-meta"></div>
    <div class="interval-btns">
      <button class="interval-btn active" data-interval="1d">Daily</button>
      <button class="interval-btn" data-interval="5m">5 Min</button>
    </div>
    <button id="hist-close">✕ Close</button>
  </div>
  <div id="hist-body">
    <div id="hist-charts">
      <div id="hist-stats"></div>
      <div id="price-chart"></div>
      <div id="vol-chart"></div>
    </div>
    <div id="hist-company-panel">
      <div id="hist-company-divider">Company Details</div>
      <div id="hist-company"></div>
    </div>
  </div>
</div>

<script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
<script src="https://cdn.datatables.net/1.13.8/js/jquery.dataTables.min.js"></script>
<script src="https://unpkg.com/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js"></script>
<script>
// ── Formatting helpers ─────────────────────────────────────────────────────
function fmtCap(v) {
  if (v == null) return '—';
  const a = Math.abs(v);
  if (a >= 1e12) return (v/1e12).toFixed(2) + 'T';
  if (a >= 1e9)  return (v/1e9).toFixed(2) + 'B';
  if (a >= 1e6)  return (v/1e6).toFixed(1) + 'M';
  return v.toLocaleString();
}
function fmtPct(v, scale100) {
  if (v == null) return '—';
  const pct = scale100 ? v * 100 : v;
  const cls = pct >= 0 ? 'pos-change' : 'neg-change';
  return `<span class="${cls}">${pct.toFixed(1)}%</span>`;
}
function fmtNum(v, dec) {
  if (v == null) return '—';
  return v.toFixed(dec ?? 2);
}
function fmtDollar(v) {
  if (v == null) return '—';
  return '$' + v.toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2});
}
function fmtRuleValue(fmt, val) {
  if (fmt === 'pct_frac' || fmt === 'pct_val') return val.toFixed(1) + '%';
  if (fmt === 'dollar') return '$' + val.toLocaleString();
  if (fmt === 'cap') return '$' + fmtCap(val);
  return val.toLocaleString();
}
function recBadge(v) {
  if (!v || v === 'none') return '<span class="rec-none">—</span>';
  const map = {strong_buy:'Strong Buy',buy:'Buy',hold:'Hold',sell:'Sell',strong_sell:'Strong Sell'};
  const cls = {strong_buy:'rec-strong-buy',buy:'rec-buy',hold:'rec-hold',sell:'rec-sell',strong_sell:'rec-sell'};
  return `<span class="${cls[v]||'rec-hold'}">${map[v]||v}</span>`;
}
function n(v) { return v ?? ''; }

// ── State ──────────────────────────────────────────────────────────────────
let currentSlug = null;
let _activeLoad = 0;    // incremented each load; callbacks check against this
let holdingsTickers = new Set();
let holdingsDt = null;
let _guruColors = {};   // slug → color

// ── Style grouping for dropdown ────────────────────────────────────────────
const STYLE_GROUPS = [
  { label: 'Value Investing',         styles: ['Quality Value', 'Deep Value', 'Deep Value / Contrarian', 'Activist Value', 'Dhandho Value', 'Event-Driven Value'] },
  { label: 'Quality & Concentrated',  styles: ['Concentrated Quality'] },
  { label: 'Growth & Momentum',       styles: ['Macro Growth', 'High-Growth Tech', 'Growth & Momentum'] },
  { label: 'Macro / Risk Parity',     styles: ['All Weather / Risk Parity'] },
  { label: 'Distressed & Macro',      styles: ['Distressed & Macro', 'Global Macro & Momentum'] },
  { label: 'Disruptive Innovation',   styles: ['Disruptive Innovation'] },
  { label: 'Quantitative / Multi-Strat', styles: ['Multi-Strategy Quantitative'] },
];

fetch('/api/guru/list').then(r => r.json()).then(list => {
  const sel = document.getElementById('guru-select');
  const byStyle = {};
  list.forEach(g => {
    _guruColors[g.slug] = g.color;
    if (!byStyle[g.style]) byStyle[g.style] = [];
    byStyle[g.style].push(g);
  });
  STYLE_GROUPS.forEach(grp => {
    const og = document.createElement('optgroup');
    og.label = grp.label;
    grp.styles.forEach(style => {
      (byStyle[style] || []).forEach(g => {
        const opt = document.createElement('option');
        opt.value = g.slug;
        opt.textContent = g.name + '  –  ' + g.fund;
        og.appendChild(opt);
      });
    });
    if (og.children.length) sel.appendChild(og);
  });
  sel.addEventListener('change', () => {
    const slug = sel.value;
    if (slug) selectGuru(slug);
  });
  // Auto-select from ?guru= query param (e.g. clicking from leaderboard)
  const _qGuru = new URLSearchParams(location.search).get('guru');
  if (_qGuru) { sel.value = _qGuru; if (sel.value) selectGuru(_qGuru); }
});

function selectGuru(slug) {
  currentSlug = slug;
  // Update style pill
  const pill = document.getElementById('guru-style-pill');
  const color = _guruColors[slug] || '#3730a3';
  pill.style.display = 'inline-block';
  pill.style.background = color + '33';
  pill.style.color = color;
  pill.style.border = '1px solid ' + color + '66';
  // pill text filled after info loads
  loadGuru(slug);
}

// ── Load guru ──────────────────────────────────────────────────────────────
function loadGuru(slug) {
  const loadId = ++_activeLoad;   // cancel stale responses

  // Reset UI to loading state
  ['holdings-msg'].forEach(id => {
    const el = document.getElementById(id);
    el.style.display = '';
    el.className = 'loading-msg';
  });
  document.getElementById('holdings-msg').textContent = 'Fetching 13F from SEC EDGAR… (may take a few seconds)';
  document.getElementById('holdings-wrap').style.display = 'none';
  document.getElementById('holdings-filing').textContent = 'Loading…';
  document.getElementById('holdings-overlap').textContent = '';
  document.getElementById('badge-holdings').textContent = '…';

  // Destroy any existing DataTable immediately so there's no stale DOM
  if (holdingsDt) { try { holdingsDt.destroy(); } catch(e) {} holdingsDt = null; }
  document.getElementById('holdings-body').innerHTML = '';

  Promise.all([
    fetch(`/api/guru/${slug}/info`).then(r => r.json()),
    fetch(`/api/guru/${slug}/holdings`).then(r => r.json()),
  ]).then(([info, holdingsData]) => {
    if (loadId !== _activeLoad) return;   // another guru was selected; discard
    renderGuruCard(info);
    renderPerformance(holdingsData);
    renderHoldings(holdingsData, info);
  }).catch(err => {
    if (loadId !== _activeLoad) return;
    document.getElementById('holdings-msg').textContent = 'Error: ' + err.message;
    document.getElementById('holdings-msg').className = 'error-msg';
  });
}

// ── Guru card ──────────────────────────────────────────────────────────────
function renderGuruCard(info) {
  const card = document.getElementById('guru-card');
  card.style.display = 'block';
  document.getElementById('guru-placeholder').style.display = 'none';

  const badge = document.getElementById('guru-style-badge');
  badge.textContent = info.style;
  badge.style.background = info.color + '33';
  badge.style.color = info.color;
  badge.style.border = '1px solid ' + info.color + '66';
  // Update selector-bar pill
  const pill = document.getElementById('guru-style-pill');
  pill.textContent = info.style;
  pill.style.display = 'inline-block';
  pill.style.background = info.color + '33';
  pill.style.color = info.color;
  pill.style.border = '1px solid ' + info.color + '66';

  document.getElementById('guru-name').textContent = info.name;
  document.getElementById('guru-fund').textContent = info.fund;
  document.getElementById('guru-quote').textContent = '"' + info.quote + '"';
  document.getElementById('guru-desc').textContent = info.description;

  // Render rules
  const rulesDiv = document.getElementById('guru-rules');
  rulesDiv.innerHTML = '';
  const numRules = info.rules?.numeric || {};
  const catRules = info.rules?.categorical || {};
  const meta = info.rule_meta || {};

  for (const [fid, bounds] of Object.entries(numRules)) {
    const m = meta[fid] || {};
    const label = m.label || fid;
    const fmt = m.fmt || 'num';
    const parts = [];
    if (bounds.min != null) parts.push('≥ ' + fmtRuleValue(fmt, bounds.min));
    if (bounds.max != null) parts.push('≤ ' + fmtRuleValue(fmt, bounds.max));
    const div = document.createElement('div');
    div.className = 'rule-item';
    div.innerHTML = `<span class="rule-label">${label}</span><span class="rule-value">${parts.join(' and ')}</span>`;
    rulesDiv.appendChild(div);
  }
  for (const [fid, vals] of Object.entries(catRules)) {
    if (!vals || !vals.length) continue;
    const div = document.createElement('div');
    div.className = 'cat-rule';
    div.textContent = fid + ': ' + vals.join(', ');
    rulesDiv.appendChild(div);
  }
}

// ── Type / Change badge helpers ────────────────────────────────────────────
function typeBadge(pc) {
  const t = (pc || '').toUpperCase();
  if (t === 'CALL') return '<span class="type-badge type-call">CALL</span>';
  if (t === 'PUT')  return '<span class="type-badge type-put">PUT</span>';
  return '<span class="type-badge type-eq">EQ</span>';
}
function changeBadge(chg) {
  if (!chg)           return '<span style="color:#2d3148">—</span>';
  if (chg === 'new')     return '<span class="chg-badge chg-new">★ New</span>';
  if (chg === 'added')   return '<span class="chg-badge chg-added">▲ Added</span>';
  if (chg === 'reduced') return '<span class="chg-badge chg-reduced">▼ Reduced</span>';
  if (chg === 'held')    return '<span class="chg-held">Hold</span>';
  return '<span style="color:#2d3148">—</span>';
}

// ── Portfolio performance (sidebar) ───────────────────────────────────────
function renderPerformance(data) {
  const panel = document.getElementById('guru-perf');
  if (!data.filing_date) { panel.style.display = 'none'; return; }

  const filingDate = new Date(data.filing_date);
  const days = Math.floor((Date.now() - filingDate) / (1000 * 60 * 60 * 24));

  document.getElementById('perf-date').textContent = data.filing_date;
  document.getElementById('perf-days').textContent = days + (days === 1 ? ' day' : ' days');

  const fv = data.filing_total_value  || 0;
  const cv = data.current_total_value || 0;

  document.getElementById('perf-filing-val').textContent = fv > 0 ? '$' + fmtCap(fv) : '—';
  document.getElementById('perf-curr-val').textContent   = cv > 0 ? '$' + fmtCap(cv) : '—';

  const chgEl = document.getElementById('perf-chg');
  if (fv > 0 && cv > 0) {
    const pct = (cv - fv) / fv * 100;
    const abs = Math.abs(cv - fv);
    chgEl.textContent = (pct >= 0 ? '+' : '') + pct.toFixed(2) + '%  (' + (pct >= 0 ? '+' : '-') + '$' + fmtCap(abs) + ')';
    chgEl.className = pct >= 0 ? 'pos-change' : 'neg-change';
  } else {
    chgEl.textContent = '—';
    chgEl.className = '';
  }

  panel.style.display = 'block';
}

// ── Holdings table ─────────────────────────────────────────────────────────
function renderHoldings(data, info) {
  // Destroy before touching DOM (safety — already done in loadGuru but guard here too)
  if (holdingsDt) { try { holdingsDt.destroy(); } catch(e) {} holdingsDt = null; }

  holdingsTickers = new Set(
    (data.holdings || []).filter(h => h.ticker).map(h => h.ticker)
  );

  const rulesNumeric = info.rules?.numeric || {};
  const ruleMeta = info.rule_meta || {};

  document.getElementById('holdings-filing').textContent =
    data.filing_date ? 'Latest 13F: ' + data.filing_date : 'Filing date unknown';

  const tbody = document.getElementById('holdings-body');
  tbody.innerHTML = '';

  if (!data.holdings || data.holdings.length === 0) {
    const msg = document.getElementById('holdings-msg');
    msg.textContent = 'No 13F holdings found. The fund may be below the $100M AUM threshold or filed under a different entity.';
    msg.className = 'error-msg';
    msg.style.display = '';
    document.getElementById('badge-holdings').textContent = '0';
    return;
  }

  let matchCount = 0;
  data.holdings.forEach((h, i) => {
    const matchesRules = passesRules(h, rulesNumeric, ruleMeta);
    if (matchesRules) matchCount++;

    const roe = h.returnOnEquity != null ? h.returnOnEquity * 100 : null;
    const ret = h['52WeekChange']  != null ? h['52WeekChange']  * 100 : null;

    const tr = document.createElement('tr');
    if (matchesRules) tr.classList.add('guru-held');

    const tickerCell = h.ticker
      ? `<span class="ticker-link" onclick="openHistory('${h.ticker}')">${h.ticker}</span>`
      : `<span class="ticker-none">—</span>`;

    tr.innerHTML = `
      <td class="num">${i + 1}</td>
      <td>${changeBadge(h.change)}</td>
      <td>${typeBadge(h.put_call)}</td>
      <td>${tickerCell}</td>
      <td>${h.name}</td>
      <td>${h.sector ? `<span class="sector-tag">${h.sector}</span>` : '—'}</td>
      <td class="num" data-order="${n(h.value)}">${'$' + fmtCap(h.value)}</td>
      <td class="num" data-order="${n(h.weight)}">${h.weight != null ? h.weight.toFixed(2) + '%' : '—'}</td>
      <td class="num" data-order="${n(h.shares)}">${h.shares ? h.shares.toLocaleString() : '—'}</td>
      <td class="num" data-order="${n(h.currentPrice)}">${h.currentPrice != null ? '$' + h.currentPrice.toFixed(2) : '—'}</td>
      <td class="num" data-order="${n(h.trailingPE)}">${h.trailingPE != null ? h.trailingPE.toFixed(1) : '—'}</td>
      <td class="num" data-order="${n(roe)}">${roe != null ? `<span class="${roe>=0?'pos-change':'neg-change'}">${roe.toFixed(1)}%</span>` : '—'}</td>
      <td class="num" data-order="${n(ret)}">${ret != null ? `<span class="${ret>=0?'pos-change':'neg-change'}">${ret.toFixed(1)}%</span>` : '—'}</td>
      <td>${recBadge(h.recommendationKey)}</td>
      <td>${matchesRules ? '<span class="match-yes">✓ All Rules</span>' : '<span style="color:#475569;font-size:0.72rem">—</span>'}</td>
    `;
    tbody.appendChild(tr);
  });

  holdingsDt = $('#holdings-dt').DataTable({
    paging: false, dom: 'ti', order: [[6, 'desc']],
    columnDefs: [
      { targets: [6,7,8,9,10,11,12], type: 'num' },
      { orderable: false, targets: [1,2] },
    ]
  });

  document.getElementById('holdings-msg').style.display = 'none';
  document.getElementById('holdings-wrap').style.display = 'block';
  document.getElementById('badge-holdings').textContent = data.holdings.length;
  document.getElementById('holdings-overlap').textContent =
    matchCount > 0 ? `${matchCount} holding${matchCount>1?'s':''} match all investment rules` : '';
}


// ── Rule checker (client-side, for holdings "match" column) ────────────────
function passesRules(holding, numericRules, ruleMeta) {
  for (const [fid, bounds] of Object.entries(numericRules)) {
    const raw = holding[fid];
    if (raw == null) return false;
    const fmt = (ruleMeta[fid] || {}).fmt || 'num';
    const displayVal = fmt === 'pct_frac' ? raw * 100 : raw;
    if (bounds.min != null && displayVal < bounds.min) return false;
    if (bounds.max != null && displayVal > bounds.max) return false;
  }
  return true;
}

// ── Tab switching ──────────────────────────────────────────────────────────
document.querySelectorAll('.tab-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
    btn.classList.add('active');
    document.getElementById('tab-' + btn.dataset.tab).classList.add('active');
    if (btn.dataset.tab === 'holdings' && holdingsDt) holdingsDt.columns.adjust().draw();
  });
});

// ── Chart popup ──────────────────────────────────────────────────────────
let priceChart = null, volChart = null, priceSeries = null, volSeries = null;
let _histTicker = null;

function openHistory(ticker) {
  _histTicker = ticker;
  document.getElementById('hist-title').textContent = ticker;
  document.getElementById('hist-meta').textContent = 'Loading…';
  document.getElementById('hist-stats').innerHTML = '';
  document.getElementById('hist-company').innerHTML = '<div style="padding:16px;color:#64748b;font-size:0.8rem">Loading…</div>';
  document.querySelectorAll('.interval-btn').forEach(b => b.classList.remove('active'));
  document.querySelector('.interval-btn[data-interval="1d"]').classList.add('active');
  document.getElementById('hist-popup').classList.add('open');
  document.getElementById('hist-overlay').classList.add('active');
  fetch(`/api/company/${ticker}`).then(r => r.json()).then(renderCompanyPanel).catch(() => {});
  _loadHistChart(ticker, '1d');
}

function _loadHistChart(ticker, interval) {
  document.getElementById('hist-meta').textContent = 'Loading…';
  fetch(`/api/history/${ticker}?interval=${interval}`)
    .then(r => r.json())
    .then(data => _renderHistChart(data, interval))
    .catch(err => { document.getElementById('hist-meta').textContent = 'Error: ' + err.message; });
}

function _renderHistChart(data, interval) {
  const rows = Array.isArray(data) ? data : (data.rows || []);
  if (!rows.length) { document.getElementById('hist-meta').textContent = 'No data'; return; }
  if (interval !== '5m') {
    const high = Math.max(...rows.map(r => r.high)), low = Math.min(...rows.map(r => r.low)), last = rows[rows.length-1];
    document.getElementById('hist-meta').textContent = `${rows.length} trading days  ·  ${rows[0].time} → ${rows[rows.length-1].time}`;
    document.getElementById('hist-stats').innerHTML = `
      <div class="stat-item">Last Close <span>$${last.close.toFixed(2)}</span></div>
      <div class="stat-item">52W High <span>$${high.toFixed(2)}</span></div>
      <div class="stat-item">52W Low <span>$${low.toFixed(2)}</span></div>
      <div class="stat-item">Last Volume <span>${(last.volume/1e6).toFixed(2)}M</span></div>`;
  } else {
    document.getElementById('hist-meta').textContent = `${rows.length} bars (5 min)`;
    document.getElementById('hist-stats').innerHTML = '';
  }
  const pEl = document.getElementById('price-chart'), vEl = document.getElementById('vol-chart');
  pEl.innerHTML = ''; vEl.innerHTML = ''; vEl.style.display = 'block';
  if (priceChart) { priceChart.remove(); priceChart = null; }
  if (volChart)   { volChart.remove();   volChart = null; }
  const opts = { autoSize: true, layout: { background: { color: '#1a1d27' }, textColor: '#94a3b8' }, grid: { vertLines: { color: '#1e2235' }, horzLines: { color: '#1e2235' } }, timeScale: { borderColor: '#2d3148', timeVisible: true }, rightPriceScale: { borderColor: '#2d3148' } };
  priceChart = LightweightCharts.createChart(pEl, opts);
  priceSeries = priceChart.addCandlestickSeries({ upColor: '#34d399', downColor: '#f87171', borderUpColor: '#34d399', borderDownColor: '#f87171', wickUpColor: '#34d399', wickDownColor: '#f87171' });
  priceSeries.setData(rows.map(r => ({ time:r.time, open:r.open, high:r.high, low:r.low, close:r.close })));
  priceChart.timeScale().fitContent();
  volChart = LightweightCharts.createChart(vEl, opts);
  volSeries = volChart.addHistogramSeries({ color: '#3730a3', priceFormat: { type: 'volume' } });
  volSeries.setData(rows.map((r, i) => ({ time: r.time, value: r.volume, color: i > 0 && r.close >= rows[i-1].close ? '#34d39966' : '#f8717166' })));
  volChart.timeScale().fitContent();
  priceChart.timeScale().subscribeVisibleLogicalRangeChange(range => { if (range) volChart.timeScale().setVisibleLogicalRange(range); });
  volChart.timeScale().subscribeVisibleLogicalRangeChange(range => { if (range) priceChart.timeScale().setVisibleLogicalRange(range); });
}

function renderCompanyPanel(data) {
  const container = document.getElementById('hist-company');
  container.innerHTML = '';
  if (!data || !data.sections) return;
  for (const [sectionName, fields] of Object.entries(data.sections)) {
    if (!fields || Object.keys(fields).length === 0) continue;
    const sec = document.createElement('div'); sec.className = 'section';
    sec.innerHTML = `<div class="section-header"><span>${sectionName}</span><span class="section-chevron">▶</span></div>`;
    const grid = document.createElement('div'); grid.className = 'field-grid';
    for (const [key, val] of Object.entries(fields)) {
      const fEl = document.createElement('div'); fEl.className = 'field';
      const displayVal = (val == null) ? '—' : (typeof val === 'object' ? JSON.stringify(val) : (key.toLowerCase().includes('summary') ? `<div class="summary-text">${String(val)}</div>` : String(val)));
      fEl.innerHTML = `<label>${key}</label><span>${displayVal}</span>`;
      grid.appendChild(fEl);
    }
    sec.appendChild(grid);
    sec.querySelector('.section-header').addEventListener('click', () => sec.classList.toggle('open'));
    container.appendChild(sec);
  }
}

document.querySelectorAll('.interval-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.interval-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    if (_histTicker) _loadHistChart(_histTicker, btn.dataset.interval);
  });
});

function closeHistory() {
  document.getElementById('hist-popup').classList.remove('open');
  document.getElementById('hist-overlay').classList.remove('active');
}
document.getElementById('hist-close').addEventListener('click', closeHistory);
document.getElementById('hist-overlay').addEventListener('click', closeHistory);
</script>
</body>
</html>"""

RECO_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>S&P 500 – Recommendations</title>
<link rel="stylesheet" href="https://cdn.datatables.net/1.13.8/css/jquery.dataTables.min.css">
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: system-ui, sans-serif; background: #0f1117; color: #e2e8f0; min-height: 100vh; display: flex; flex-direction: column; }
  header { background: #1a1d27; border-bottom: 1px solid #2d3148; padding: 12px 24px; display: flex; align-items: center; gap: 16px; flex-shrink: 0; }
  header h1 { font-size: 1.25rem; font-weight: 700; color: #f8fafc; white-space: nowrap; }
  .nav-links { display: flex; gap: 4px; margin-left: 8px; }
  .nav-link { font-size: 0.82rem; padding: 5px 14px; border-radius: 6px; text-decoration: none; color: #94a3b8; transition: background .15s, color .15s; }
  .nav-link:hover { background: #2d3148; color: #e2e8f0; }
  .nav-link.active { background: #3730a3; color: #fff; font-weight: 600; }

  #page-wrap { flex: 1; padding: 20px 24px; overflow-x: auto; }

  /* summary bar */
  #summary-bar { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 18px; }
  .sum-card { background: #1a1d27; border: 1px solid #2d3148; border-radius: 8px; padding: 10px 18px; display: flex; flex-direction: column; gap: 2px; min-width: 120px; cursor: pointer; transition: border-color .15s, background .15s; user-select: none; }
  .sum-card:hover { border-color: #4f5b8a; background: #1e2235; }
  .sum-card.active { border-color: #6366f1; background: #1e1b4b; }
  .sum-card .sum-label { font-size: 0.7rem; text-transform: uppercase; letter-spacing: .06em; color: #64748b; }
  .sum-card .sum-val { font-size: 1.35rem; font-weight: 700; }
  .sum-card.strong-buy .sum-val { color: #059669; }
  .sum-card.buy       .sum-val { color: #34d399; }
  .sum-card.hold      .sum-val { color: #fbbf24; }
  .sum-card.under     .sum-val { color: #f97316; }
  .sum-card.sell      .sum-val { color: #f87171; }
  .sum-card.total     .sum-val { color: #818cf8; }

  /* toolbar */
  #toolbar { display: flex; gap: 10px; align-items: center; margin-bottom: 14px; flex-wrap: wrap; }
  #toolbar input { background: #1a1d27; border: 1px solid #2d3148; color: #e2e8f0; padding: 6px 12px; border-radius: 6px; font-size: 0.83rem; min-width: 220px; outline: none; }
  #toolbar input:focus { border-color: #3730a3; }
  #label-filter { display: flex; gap: 6px; }
  .lf-btn { background: #1a1d27; border: 1px solid #2d3148; color: #94a3b8; padding: 5px 14px; border-radius: 6px; font-size: 0.78rem; cursor: pointer; }
  .lf-btn.active { background: #3730a3; border-color: #3730a3; color: #fff; }
  #refresh-btn { background: #1e2235; border: 1px solid #2d3148; color: #94a3b8; padding: 5px 14px; border-radius: 6px; font-size: 0.78rem; cursor: pointer; margin-left: auto; }
  #refresh-btn:hover { color: #e2e8f0; }
  #sentiment-run-btn { background: #1e1b4b; border: 1px solid #3730a3; color: #818cf8; padding: 5px 14px; border-radius: 6px; font-size: 0.78rem; cursor: pointer; }
  #sentiment-run-btn:hover:not(:disabled) { background: #2d2a7a; color: #a5b4fc; }
  #sentiment-run-btn:disabled { opacity: 0.45; cursor: not-allowed; }
  #last-computed { font-size: 0.75rem; color: #475569; }

  /* table */
  .table-wrap { background: #131620; border: 1px solid #2d3148; border-radius: 10px; overflow: hidden; }
  table.dataTable { width: 100% !important; border-collapse: collapse; }
  table.dataTable thead th { background: #1a1d27; color: #64748b; font-size: 0.72rem; text-transform: uppercase; letter-spacing: .06em; padding: 11px 12px; border-bottom: 1px solid #2d3148; white-space: nowrap; cursor: pointer; }
  table.dataTable thead th:hover { color: #e2e8f0; }
  table.dataTable tbody tr { border-bottom: 1px solid #1e2235; cursor: pointer; transition: background .1s; }
  table.dataTable tbody tr:hover { background: #1a1d27; }
  table.dataTable tbody td { padding: 9px 12px; font-size: 0.82rem; white-space: nowrap; }
  td.num, th.num { text-align: right; }
  .dataTables_wrapper .dataTables_info, .dataTables_wrapper .dataTables_paginate { padding: 10px 14px; font-size: 0.78rem; color: #64748b; }
  .dataTables_wrapper .dataTables_paginate .paginate_button { color: #94a3b8 !important; padding: 4px 10px; border-radius: 4px; }
  .dataTables_wrapper .paginate_button.current { background: #3730a3 !important; color: #fff !important; border: none !important; }

  /* cells */
  .rank-num { font-size: 0.95rem; font-weight: 700; color: #475569; width: 36px; text-align: center; }
  .rank-1 { color: #f59e0b; }
  .rank-2 { color: #94a3b8; }
  .rank-3 { color: #b45309; }
  .ticker-link { font-weight: 700; color: #818cf8; font-family: monospace; font-size: 0.9rem; cursor: pointer; text-decoration: underline dotted; text-underline-offset: 3px; }
  .ticker-link:hover { color: #a5b4fc; }
  .sector-tag { background: #1e2235; color: #94a3b8; padding: 2px 7px; border-radius: 4px; font-size: 0.72rem; }

  /* score bar */
  .score-cell { display: flex; align-items: center; gap: 8px; min-width: 110px; }
  .score-bar-wrap { flex: 1; height: 6px; background: #2d3148; border-radius: 3px; overflow: hidden; min-width: 60px; }
  .score-bar { height: 100%; border-radius: 3px; transition: width .3s; }
  .score-num { font-size: 0.85rem; font-weight: 700; min-width: 32px; text-align: right; }

  /* sub-scores */
  .sub-scores { display: flex; gap: 4px; }
  .ss-pill { font-size: 0.68rem; padding: 2px 6px; border-radius: 4px; font-weight: 600; }
  .ss-pill.hi  { background: #064e3b; color: #34d399; }
  .ss-pill.mid { background: #422006; color: #fbbf24; }
  .ss-pill.lo  { background: #450a0a; color: #f87171; }

  /* label badge */
  .label-strong-buy { background: #064e3b; color: #34d399; padding: 3px 8px; border-radius: 5px; font-size: 0.72rem; font-weight: 700; }
  .label-buy        { background: #052e16; color: #6ee7b7; padding: 3px 8px; border-radius: 5px; font-size: 0.72rem; font-weight: 700; }
  .label-hold       { background: #422006; color: #fbbf24; padding: 3px 8px; border-radius: 5px; font-size: 0.72rem; font-weight: 700; }
  .label-under      { background: #431407; color: #fb923c; padding: 3px 8px; border-radius: 5px; font-size: 0.72rem; font-weight: 700; }
  .label-sell       { background: #450a0a; color: #f87171; padding: 3px 8px; border-radius: 5px; font-size: 0.72rem; font-weight: 700; }

  /* change pct */
  .pos { color: #34d399; }
  .neg { color: #f87171; }
  .neu { color: #64748b; }

  /* guru chips */
  .guru-chip { background: #1e1b4b; color: #818cf8; padding: 2px 7px; border-radius: 4px; font-size: 0.72rem; font-weight: 600; }

  /* weight panel */
  #summary-bar { align-items: flex-start; }
  .sum-cards { display: flex; gap: 12px; flex-wrap: wrap; }
  #weight-panel { flex: 1; min-width: 340px; background: #1a1d27; border: 1px solid #2d3148; border-radius: 8px; padding: 12px 16px; }
  .wp-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; gap: 8px; }
  .wp-title { font-size: 0.72rem; text-transform: uppercase; letter-spacing: .06em; color: #64748b; display: flex; align-items: center; gap: 6px; }
  .wp-note  { font-size: 0.68rem; color: #334155; font-style: italic; flex: 1; }
  #info-btn { background: none; border: none; color: #475569; font-size: 0.9rem; cursor: pointer; line-height: 1; padding: 0; transition: color .15s; }
  #info-btn:hover { color: #818cf8; }
  /* info popup */
  #info-overlay { display: none; position: fixed; inset: 0; z-index: 200; }
  #info-overlay.open { display: block; }
  #info-popup { position: fixed; top: 50%; left: 50%; transform: translate(-50%, -50%); width: min(92vw, 580px); max-height: 85vh; overflow-y: auto; background: #1a1d27; border: 1px solid #2d3148; border-radius: 12px; z-index: 201; padding: 22px 24px; box-shadow: 0 20px 60px rgba(0,0,0,.6); display: none; }
  #info-popup.open { display: block; }
  .ip-title { font-size: 1rem; font-weight: 700; color: #f8fafc; margin-bottom: 4px; }
  .ip-sub   { font-size: 0.75rem; color: #475569; margin-bottom: 18px; }
  .ip-section { margin-bottom: 16px; }
  .ip-section h3 { font-size: 0.7rem; text-transform: uppercase; letter-spacing: .07em; color: #6366f1; margin-bottom: 8px; padding-bottom: 5px; border-bottom: 1px solid #2d3148; }
  .ip-row { display: flex; justify-content: space-between; align-items: center; padding: 4px 0; font-size: 0.8rem; }
  .ip-row:not(:last-child) { border-bottom: 1px solid #1e2235; }
  .ip-key  { color: #94a3b8; font-family: monospace; font-size: 0.75rem; }
  .ip-val  { color: #e2e8f0; font-weight: 600; }
  .ip-bar-wrap { flex: 1; margin: 0 10px; height: 4px; background: #2d3148; border-radius: 2px; overflow: hidden; }
  .ip-bar  { height: 100%; background: #6366f1; border-radius: 2px; }
  .ip-note { font-size: 0.72rem; color: #475569; font-style: italic; margin-top: 4px; }
  #info-close { position: absolute; top: 14px; right: 16px; background: none; border: none; color: #64748b; font-size: 1.2rem; cursor: pointer; }
  #info-close:hover { color: #e2e8f0; }
  #reset-weights { background: #1e2235; border: 1px solid #2d3148; color: #94a3b8; padding: 3px 10px; border-radius: 5px; font-size: 0.72rem; cursor: pointer; }
  #reset-weights:hover { color: #e2e8f0; border-color: #4f5b8a; }
  .weight-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px 24px; }
  .weight-row { display: flex; align-items: center; gap: 8px; }
  .weight-label { font-size: 0.72rem; color: #94a3b8; width: 76px; flex-shrink: 0; }
  .weight-slider { flex: 1; -webkit-appearance: none; appearance: none; height: 4px; border-radius: 2px; background: #2d3148; outline: none; cursor: pointer; }
  .weight-slider::-webkit-slider-thumb { -webkit-appearance: none; width: 14px; height: 14px; border-radius: 50%; background: #6366f1; cursor: pointer; transition: background .15s; }
  .weight-slider::-webkit-slider-thumb:hover { background: #818cf8; }
  .weight-slider::-moz-range-thumb { width: 14px; height: 14px; border-radius: 50%; background: #6366f1; cursor: pointer; border: none; }
  .weight-val { font-size: 0.78rem; color: #818cf8; width: 32px; text-align: right; font-weight: 700; font-variant-numeric: tabular-nums; }

  /* chart popup (reused from main page) */
  #hist-overlay { display:none; position:fixed; inset:0; background:rgba(0,0,0,.55); z-index:99; }
  #hist-overlay.active { display:block; }
  #hist-popup { display:none; position:fixed; top:5vh; left:50%; transform:translateX(-50%); width:min(95vw,1100px); max-height:90vh; background:#1a1d27; border:1px solid #2d3148; border-radius:12px; z-index:100; overflow:auto; flex-direction:column; }
  #hist-popup.open { display:flex; }
  #hist-header { display:flex; align-items:center; gap:12px; padding:14px 20px; border-bottom:1px solid #2d3148; flex-shrink:0; }
  #hist-title { font-size:1.1rem; font-weight:700; color:#f8fafc; flex:1; }
  #hist-meta { font-size:0.78rem; color:#64748b; }
  .interval-btns { display:flex; gap:4px; align-items:center; }
  .interval-btn { background:#1a1d27; border:1px solid #2d3148; color:#94a3b8; padding:4px 12px; border-radius:6px; font-size:0.75rem; cursor:pointer; }
  .interval-btn.active, .interval-btn:hover { background:#6366f1; border-color:#6366f1; color:#fff; }
  #hist-close { background:none; border:none; color:#64748b; font-size:1.3rem; cursor:pointer; padding:0 4px; }
  #hist-close:hover { color:#e2e8f0; }
  #hist-stats { display:flex; gap:16px; padding:10px 20px; background:#131620; border-bottom:1px solid #1e2235; flex-wrap:wrap; flex-shrink:0; }
  .stat-item { display:flex; flex-direction:column; gap:2px; }
  .stat-item > *:first-child { font-size:0.68rem; color:#64748b; text-transform:uppercase; letter-spacing:.05em; }
  .stat-item span { font-size:0.9rem; font-weight:600; color:#e2e8f0; }
  #hist-body { display:flex; flex:1; min-height:0; overflow:hidden; }
  #hist-charts { flex:1; display:flex; flex-direction:column; min-width:0; padding:12px; gap:8px; }
  #price-chart { flex:3; min-height:0; }
  #vol-chart   { flex:1; min-height:80px; }
  #hist-company-wrap { width:300px; flex-shrink:0; border-left:1px solid #2d3148; overflow-y:auto; }
  #hist-company { padding:12px; }
  .section h3 { font-size:0.78rem; text-transform:uppercase; letter-spacing:.05em; color:#64748b; padding:8px 0 4px; cursor:pointer; display:flex; align-items:center; justify-content:space-between; border-bottom:1px solid #1e2235; margin-bottom:4px; }
  .section h3:hover { color:#e2e8f0; }
  .field-grid { display:none; grid-template-columns:1fr 1fr; gap:4px 8px; padding-bottom:6px; }
  .section.open .field-grid { display:grid; }
  .field { display:flex; flex-direction:column; gap:1px; }
  .field label { font-size:0.65rem; color:#475569; text-transform:uppercase; }
  .field span { font-size:0.78rem; color:#e2e8f0; }
</style>
<script src="https://cdn.jsdelivr.net/npm/lightweight-charts@4/dist/lightweight-charts.standalone.production.js"></script>
<script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
<script src="https://cdn.datatables.net/1.13.8/js/jquery.dataTables.min.js"></script>
</head>
<body>
<header>
  <h1>S&amp;P 500</h1>
  <nav class="nav-links">
    <a href="/" class="nav-link">Table</a>
    <a href="/screener" class="nav-link">Screener</a>
    <a href="/gurus" class="nav-link">Guru Investing</a>
    <a href="/recommendations" class="nav-link active">Recommendations</a>
  </nav>
</header>

<div id="page-wrap">
  <div id="summary-bar">
    <div class="sum-cards">
      <div class="sum-card total"      data-label="">            <span class="sum-label">Total Scored</span> <span class="sum-val" id="s-total">—</span></div>
      <div class="sum-card strong-buy" data-label="Strong Buy"> <span class="sum-label">Strong Buy</span>   <span class="sum-val" id="s-sb">—</span></div>
      <div class="sum-card buy"        data-label="Buy">        <span class="sum-label">Buy</span>          <span class="sum-val" id="s-b">—</span></div>
      <div class="sum-card hold"       data-label="Hold">       <span class="sum-label">Hold</span>         <span class="sum-val" id="s-h">—</span></div>
      <div class="sum-card under"      data-label="Underperform"><span class="sum-label">Underperform</span><span class="sum-val" id="s-u">—</span></div>
      <div class="sum-card sell"       data-label="Sell">       <span class="sum-label">Sell</span>         <span class="sum-val" id="s-s">—</span></div>
    </div>

    <div id="weight-panel">
      <div class="wp-header">
        <span class="wp-title">Signal Weights <button id="info-btn" title="How scoring works">ⓘ</button></span>
        <span class="wp-note">auto-normalized · drag to rebalance</span>
        <button id="reset-weights">↺ Reset</button>
      </div>
      <div class="weight-grid">
        <div class="weight-row">
          <span class="weight-label">Momentum</span>
          <input type="range" class="weight-slider" id="w-momentum"    data-signal="momentum"    min="0" max="100" value="15">
          <span class="weight-val" id="wv-momentum">15%</span>
        </div>
        <div class="weight-row">
          <span class="weight-label">Quality</span>
          <input type="range" class="weight-slider" id="w-fundamental" data-signal="fundamental" min="0" max="100" value="25">
          <span class="weight-val" id="wv-fundamental">25%</span>
        </div>
        <div class="weight-row">
          <span class="weight-label">Valuation</span>
          <input type="range" class="weight-slider" id="w-valuation"   data-signal="valuation"   min="0" max="100" value="15">
          <span class="weight-val" id="wv-valuation">15%</span>
        </div>
        <div class="weight-row">
          <span class="weight-label">Guru</span>
          <input type="range" class="weight-slider" id="w-guru"        data-signal="guru"        min="0" max="100" value="15">
          <span class="weight-val" id="wv-guru">15%</span>
        </div>
        <div class="weight-row">
          <span class="weight-label">Analyst</span>
          <input type="range" class="weight-slider" id="w-analyst"     data-signal="analyst"     min="0" max="100" value="15">
          <span class="weight-val" id="wv-analyst">15%</span>
        </div>
        <div class="weight-row">
          <span class="weight-label">Sentiment</span>
          <input type="range" class="weight-slider" id="w-sentiment"   data-signal="sentiment"   min="0" max="100" value="15">
          <span class="weight-val" id="wv-sentiment">15%</span>
        </div>
      </div>
      <div id="sentiment-status" style="margin-top:8px;font-size:0.68rem;color:#334155;">Sentiment: loading…</div>
    </div>
  </div>

  <div id="toolbar">
    <input id="search-box" type="text" placeholder="Search ticker or company…">
    <div id="label-filter">
      <button class="lf-btn active" data-label="">All</button>
      <button class="lf-btn" data-label="Strong Buy">Strong Buy</button>
      <button class="lf-btn" data-label="Buy">Buy</button>
      <button class="lf-btn" data-label="Hold">Hold</button>
      <button class="lf-btn" data-label="Underperform">Underperform</button>
      <button class="lf-btn" data-label="Sell">Sell</button>
    </div>
    <span id="last-computed"></span>
    <button id="refresh-btn">⟳ Refresh Scores</button>
    <button id="sentiment-run-btn">⚡ Run Sentiment</button>
  </div>

  <div class="table-wrap">
    <table id="reco-tbl" class="dataTable">
      <thead><tr>
        <th class="num">Rank</th>
        <th>Ticker</th>
        <th>Company</th>
        <th>Sector</th>
        <th class="num">Score</th>
        <th>Label</th>
        <th class="num">Momentum</th>
        <th class="num">Quality</th>
        <th class="num">Valuation</th>
        <th class="num">Guru</th>
        <th class="num">Analyst</th>
        <th>Sentiment</th>
        <th class="num">Price</th>
        <th class="num">Mkt Cap</th>
        <th class="num">Change</th>
        <th class="num">3M Ret</th>
        <th class="num">1Y Ret</th>
        <th class="num">Gurus</th>
      </tr></thead>
      <tbody id="reco-body"></tbody>
    </table>
  </div>
</div>

<!-- info popup -->
<div id="info-overlay"></div>
<div id="info-popup">
  <button id="info-close">&#x2715;</button>
  <div class="ip-title">How Stocks Are Scored</div>
  <div class="ip-sub">Composite = weighted sum of 5 signals (each 0–100 percentile rank within S&amp;P 500). Weights are auto-normalized.</div>

  <div class="ip-section">
    <h3>Signal Weights &nbsp;(drag sliders to override)</h3>
    <div class="ip-row"><span class="ip-key">Momentum</span>         <div class="ip-bar-wrap"><div class="ip-bar" style="width:15%"></div></div><span class="ip-val">15%</span></div>
    <div class="ip-row"><span class="ip-key">Quality</span>          <div class="ip-bar-wrap"><div class="ip-bar" style="width:25%"></div></div><span class="ip-val">25%</span></div>
    <div class="ip-row"><span class="ip-key">Valuation</span>        <div class="ip-bar-wrap"><div class="ip-bar" style="width:15%"></div></div><span class="ip-val">15%</span></div>
    <div class="ip-row"><span class="ip-key">Guru Conviction</span>  <div class="ip-bar-wrap"><div class="ip-bar" style="width:15%"></div></div><span class="ip-val">15%</span></div>
    <div class="ip-row"><span class="ip-key">Analyst Consensus</span><div class="ip-bar-wrap"><div class="ip-bar" style="width:15%"></div></div><span class="ip-val">15%</span></div>
    <div class="ip-row"><span class="ip-key">Market Sentiment</span> <div class="ip-bar-wrap"><div class="ip-bar" style="width:15%;background:#34d399"></div></div><span class="ip-val" style="color:#34d399">15%</span></div>
  </div>

  <div class="ip-section">
    <h3>Momentum — 3M/6M/1Y price return blend</h3>
    <div class="ip-row"><span class="ip-key">3-Month return</span> <div class="ip-bar-wrap"><div class="ip-bar" style="width:30%"></div></div><span class="ip-val">30%</span></div>
    <div class="ip-row"><span class="ip-key">6-Month return</span> <div class="ip-bar-wrap"><div class="ip-bar" style="width:40%"></div></div><span class="ip-val">40%</span></div>
    <div class="ip-row"><span class="ip-key">1-Year return</span>  <div class="ip-bar-wrap"><div class="ip-bar" style="width:30%"></div></div><span class="ip-val">30%</span></div>
    <div class="ip-note">Weighted return is percentile-ranked across S&amp;P 500. Higher score = stronger relative momentum.</div>
  </div>

  <div class="ip-section">
    <h3>Fundamental Quality — profitability &amp; growth</h3>
    <div class="ip-row"><span class="ip-key">returnOnEquity</span>   <div class="ip-bar-wrap"><div class="ip-bar" style="width:30%"></div></div><span class="ip-val">30%</span></div>
    <div class="ip-row"><span class="ip-key">grossMargins</span>     <div class="ip-bar-wrap"><div class="ip-bar" style="width:20%"></div></div><span class="ip-val">20%</span></div>
    <div class="ip-row"><span class="ip-key">operatingMargins</span> <div class="ip-bar-wrap"><div class="ip-bar" style="width:20%"></div></div><span class="ip-val">20%</span></div>
    <div class="ip-row"><span class="ip-key">revenueGrowth</span>    <div class="ip-bar-wrap"><div class="ip-bar" style="width:15%"></div></div><span class="ip-val">15%</span></div>
    <div class="ip-row"><span class="ip-key">earningsGrowth</span>   <div class="ip-bar-wrap"><div class="ip-bar" style="width:15%"></div></div><span class="ip-val">15%</span></div>
    <div class="ip-note">Each metric independently percentile-ranked, then blended.</div>
  </div>

  <div class="ip-section">
    <h3>Valuation — lower multiple = higher score</h3>
    <div class="ip-row"><span class="ip-key">trailingPE</span>                   <div class="ip-bar-wrap"><div class="ip-bar" style="width:30%"></div></div><span class="ip-val">30%</span></div>
    <div class="ip-row"><span class="ip-key">priceToBook</span>                  <div class="ip-bar-wrap"><div class="ip-bar" style="width:25%"></div></div><span class="ip-val">25%</span></div>
    <div class="ip-row"><span class="ip-key">priceToSalesTrailing12Months</span> <div class="ip-bar-wrap"><div class="ip-bar" style="width:25%"></div></div><span class="ip-val">25%</span></div>
    <div class="ip-row"><span class="ip-key">enterpriseToEbitda</span>           <div class="ip-bar-wrap"><div class="ip-bar" style="width:20%"></div></div><span class="ip-val">20%</span></div>
    <div class="ip-note">Inverted rank — cheapest stock scores 100. Negative values clamped to 0 before ranking.</div>
  </div>

  <div class="ip-section">
    <h3>Guru Conviction — 13F filing change tags</h3>
    <div class="ip-row"><span class="ip-key">New position</span>     <span class="ip-val" style="color:#34d399">+3.0 pts</span></div>
    <div class="ip-row"><span class="ip-key">Added to</span>         <span class="ip-val" style="color:#6ee7b7">+2.0 pts</span></div>
    <div class="ip-row"><span class="ip-key">Held unchanged</span>   <span class="ip-val" style="color:#94a3b8">+1.0 pts</span></div>
    <div class="ip-row"><span class="ip-key">Reduced</span>          <span class="ip-val" style="color:#f87171">−0.5 pts</span></div>
    <div class="ip-note">Sum across all 21 gurus; normalized to 0–100 relative to highest-conviction stock.</div>
  </div>

  <div class="ip-section">
    <h3>Analyst Consensus — Yahoo Finance recommendation</h3>
    <div class="ip-row"><span class="ip-key">Strong Buy</span>   <div class="ip-bar-wrap"><div class="ip-bar" style="width:100%"></div></div><span class="ip-val">100</span></div>
    <div class="ip-row"><span class="ip-key">Buy</span>          <div class="ip-bar-wrap"><div class="ip-bar" style="width:80%"></div></div><span class="ip-val">80</span></div>
    <div class="ip-row"><span class="ip-key">Hold</span>         <div class="ip-bar-wrap"><div class="ip-bar" style="width:50%"></div></div><span class="ip-val">50</span></div>
    <div class="ip-row"><span class="ip-key">Underperform</span> <div class="ip-bar-wrap"><div class="ip-bar" style="width:25%"></div></div><span class="ip-val">25</span></div>
    <div class="ip-row"><span class="ip-key">Sell</span>         <div class="ip-bar-wrap"><div class="ip-bar" style="width:5%"></div></div> <span class="ip-val">5</span></div>
    <div class="ip-note">Missing rating defaults to 50 (neutral) so no stock is unfairly penalized.</div>
  </div>

  <div class="ip-section">
    <h3>Market Sentiment — StockTwits + Reddit + FinBERT</h3>
    <div class="ip-row"><span class="ip-key">StockTwits sentiment</span><div class="ip-bar-wrap"><div class="ip-bar" style="width:40%"></div></div><span class="ip-val">40%</span></div>
    <div class="ip-row"><span class="ip-key">Reddit sentiment</span>    <div class="ip-bar-wrap"><div class="ip-bar" style="width:30%"></div></div><span class="ip-val">30%</span></div>
    <div class="ip-row"><span class="ip-key">Mention volume spike</span><div class="ip-bar-wrap"><div class="ip-bar" style="width:20%"></div></div><span class="ip-val">20%</span></div>
    <div class="ip-row"><span class="ip-key">Sentiment momentum</span>  <div class="ip-bar-wrap"><div class="ip-bar" style="width:10%"></div></div><span class="ip-val">10%</span></div>
    <div class="ip-note">Scored via FinBERT (finance-aware NLP). Covers Top-20 + Bottom-20 stocks. Runs once daily; refreshes after 12:00 ET. Un-scored stocks default to 50 (neutral).</div>
    <div style="margin-top:8px">
      <div class="ip-row"><span class="ip-key">≥ 80</span><span class="ip-val" style="color:#34d399">Strong Bullish</span></div>
      <div class="ip-row"><span class="ip-key">≥ 60</span><span class="ip-val" style="color:#6ee7b7">Bullish</span></div>
      <div class="ip-row"><span class="ip-key">≥ 40</span><span class="ip-val" style="color:#64748b">Neutral</span></div>
      <div class="ip-row"><span class="ip-key">≥ 20</span><span class="ip-val" style="color:#fb923c">Bearish</span></div>
      <div class="ip-row"><span class="ip-key">&lt; 20</span><span class="ip-val" style="color:#f87171">Strong Bearish</span></div>
    </div>
  </div>

  <div class="ip-section">
    <h3>Score → Label thresholds</h3>
    <div class="ip-row"><span class="ip-key">≥ 68</span><span class="ip-val" style="color:#34d399">Strong Buy</span></div>
    <div class="ip-row"><span class="ip-key">≥ 57</span><span class="ip-val" style="color:#6ee7b7">Buy</span></div>
    <div class="ip-row"><span class="ip-key">≥ 44</span><span class="ip-val" style="color:#fbbf24">Hold</span></div>
    <div class="ip-row"><span class="ip-key">≥ 32</span><span class="ip-val" style="color:#fb923c">Underperform</span></div>
    <div class="ip-row"><span class="ip-key">&lt; 32</span><span class="ip-val" style="color:#f87171">Sell</span></div>
  </div>
</div>

<!-- chart popup -->
<div id="hist-overlay"></div>
<div id="hist-popup">
  <div id="hist-header">
    <div id="hist-title"></div>
    <span id="hist-meta"></span>
    <div class="interval-btns">
      <button class="interval-btn active" data-interval="1d">Daily</button>
      <button class="interval-btn" data-interval="5m">5 Min</button>
    </div>
    <button id="hist-close">&#x2715;</button>
  </div>
  <div id="hist-stats"></div>
  <div id="hist-body">
    <div id="hist-charts">
      <div id="price-chart"></div>
      <div id="vol-chart"></div>
    </div>
    <div id="hist-company-wrap">
      <div id="hist-company"></div>
    </div>
  </div>
</div>

<script>
const fmt = {
  price: v => v == null ? '—' : '$' + Number(v).toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2}),
  cap:   v => { if (v == null) return '—'; const a=Math.abs(v); if(a>=1e12) return '$'+(v/1e12).toFixed(2)+'T'; if(a>=1e9) return '$'+(v/1e9).toFixed(2)+'B'; if(a>=1e6) return '$'+(v/1e6).toFixed(1)+'M'; return '$'+v.toLocaleString(); },
  pct:   v => v == null ? '—' : (v*100).toFixed(1)+'%',
  num:   (v,d=1) => v == null ? '—' : Number(v).toFixed(d),
};

function scoreColor(s) {
  if (s >= 75) return '#34d399';
  if (s >= 62) return '#6ee7b7';
  if (s >= 45) return '#fbbf24';
  if (s >= 32) return '#fb923c';
  return '#f87171';
}

function subPill(label, val) {
  const cls = val >= 65 ? 'hi' : val >= 40 ? 'mid' : 'lo';
  return `<span class="ss-pill ${cls}" title="${label}">${Math.round(val)}</span>`;
}

function labelBadge(l) {
  const key = l.toLowerCase().replace(' ', '-').replace('underperform','under');
  return `<span class="label-${key}">${l}</span>`;
}

function pctCell(v) {
  if (v == null) return '<span class="neu">—</span>';
  const cls = v > 0 ? 'pos' : v < 0 ? 'neg' : 'neu';
  return `<span class="${cls}">${v > 0 ? '+' : ''}${v.toFixed(1)}%</span>`;
}

const _SENT_STYLE = {
  'Strong Bullish': ['#064e3b','#34d399'],
  'Bullish':        ['#052e16','#6ee7b7'],
  'Neutral':        ['#1e2235','#64748b'],
  'Bearish':        ['#431407','#fb923c'],
  'Strong Bearish': ['#450a0a','#f87171'],
};
function sentBadge(cls, score, posts) {
  if (!cls) return '<span class="neu" title="Not yet scored">—</span>';
  const [bg, fg] = _SENT_STYLE[cls] || _SENT_STYLE['Neutral'];
  const tip = `${cls} · score ${score} · ${posts} posts`;
  return `<span style="background:${bg};color:${fg};padding:2px 6px;border-radius:4px;font-size:0.68rem;font-weight:600;white-space:nowrap" title="${tip}">${cls}</span>`;
}

// ── Weight engine ─────────────────────────────────────────────────────────────
const BASELINE_WEIGHTS = { momentum: 15, fundamental: 25, valuation: 15, guru: 15, analyst: 15, sentiment: 15 };
const LABEL_THRESHOLDS = [[68,'Strong Buy'],[57,'Buy'],[44,'Hold'],[32,'Underperform'],[0,'Sell']];

function computeLabel(s) {
  for (const [thr, lbl] of LABEL_THRESHOLDS) if (s >= thr) return lbl;
  return 'Sell';
}

function getNormWeights() {
  const raw = {};
  document.querySelectorAll('.weight-slider').forEach(sl => { raw[sl.dataset.signal] = parseFloat(sl.value); });
  const total = Object.values(raw).reduce((a, b) => a + b, 0) || 1;
  const norm = {};
  for (const k in raw) norm[k] = raw[k] / total;
  return norm;
}

function recomputeAndRender() {
  if (!allData.length) return;
  const w = getNormWeights();
  const scored = allData.map(d => {
    const score = Math.round((
      d.momentum    * w.momentum    +
      d.fundamental * w.fundamental +
      d.valuation   * w.valuation   +
      d.guru        * w.guru        +
      d.analyst     * w.analyst     +
      (d.sentiment ?? 50) * (w.sentiment ?? 0)
    ) * 10) / 10;
    return { ...d, score, label: computeLabel(score) };
  });
  scored.sort((a, b) => b.score - a.score);
  scored.forEach((d, i) => d.rank = i + 1);
  updateSummary(scored);
  buildTable(scored);
}

document.querySelectorAll('.weight-slider').forEach(sl => {
  sl.addEventListener('input', function() {
    document.getElementById('wv-' + this.dataset.signal).textContent = this.value + '%';
    recomputeAndRender();
  });
});

document.getElementById('reset-weights').addEventListener('click', () => {
  document.querySelectorAll('.weight-slider').forEach(sl => {
    const sig = sl.dataset.signal;
    sl.value = BASELINE_WEIGHTS[sig];
    document.getElementById('wv-' + sig).textContent = BASELINE_WEIGHTS[sig] + '%';
  });
  recomputeAndRender();
});
// ─────────────────────────────────────────────────────────────────────────────

let dt = null, allData = [], activeLabel = '';

function buildTable(data) {
  if (dt) { dt.destroy(); dt = null; }   // must destroy before touching DOM
  const tbody = document.getElementById('reco-body');
  tbody.innerHTML = '';
  data.forEach(d => {
    const rankCls = d.rank <= 3 ? `rank-${d.rank}` : '';
    const tr = document.createElement('tr');
    tr.dataset.label = d.label;
    tr.innerHTML = `
      <td class="num"><span class="rank-num ${rankCls}">${d.rank}</span></td>
      <td><span class="ticker-link" onclick="openHistory('${d.ticker}')">${d.ticker}</span></td>
      <td style="max-width:200px;overflow:hidden;text-overflow:ellipsis">${d.longName || '—'}</td>
      <td>${d.sector ? `<span class="sector-tag">${d.sector}</span>` : '—'}</td>
      <td class="num" data-order="${d.score}">
        <div class="score-cell">
          <div class="score-bar-wrap"><div class="score-bar" style="width:${d.score}%;background:${scoreColor(d.score)}"></div></div>
          <span class="score-num" style="color:${scoreColor(d.score)}">${d.score}</span>
        </div>
      </td>
      <td>${labelBadge(d.label)}</td>
      <td class="num" data-order="${d.momentum}">${subPill('Momentum', d.momentum)}</td>
      <td class="num" data-order="${d.fundamental}">${subPill('Quality', d.fundamental)}</td>
      <td class="num" data-order="${d.valuation}">${subPill('Valuation', d.valuation)}</td>
      <td class="num" data-order="${d.guru}">${subPill('Guru', d.guru)}</td>
      <td class="num" data-order="${d.analyst}">${subPill('Analyst', d.analyst)}</td>
      <td data-order="${d.sentiment ?? 50}">${sentBadge(d.sentiment_label, d.sentiment, d.sentiment_posts)}</td>
      <td class="num">${fmt.price(d.currentPrice)}</td>
      <td class="num">${fmt.cap(d.marketCap)}</td>
      <td class="num" data-order="${d.d1 ?? -999}">${pctCell(d.d1)}</td>
      <td class="num" data-order="${d.m3 ?? -999}">${pctCell(d.m3)}</td>
      <td class="num" data-order="${d.y1 ?? -999}">${pctCell(d.y1)}</td>
      <td class="num">${d.guru_count > 0 ? `<span class="guru-chip">${d.guru_count} guru${d.guru_count>1?'s':''}</span>` : '<span class="neu">—</span>'}</td>
    `;
    tbody.appendChild(tr);
  });
  dt = $('#reco-tbl').DataTable({
    paging: true, pageLength: 50,
    dom: 'tip',
    order: [[4, 'desc']],
    columnDefs: [
      { targets: [0,4,6,7,8,9,10,12,13,14,15,16], type: 'num' },
      { orderable: false, targets: [1,5,17] },
    ],
  });
}

function updateSummary(data) {
  const counts = { 'Strong Buy': 0, 'Buy': 0, 'Hold': 0, 'Underperform': 0, 'Sell': 0 };
  data.forEach(d => { if (counts[d.label] != null) counts[d.label]++; });
  document.getElementById('s-total').textContent = data.length;
  document.getElementById('s-sb').textContent = counts['Strong Buy'];
  document.getElementById('s-b').textContent  = counts['Buy'];
  document.getElementById('s-h').textContent  = counts['Hold'];
  document.getElementById('s-u').textContent  = counts['Underperform'];
  document.getElementById('s-s').textContent  = counts['Sell'];
}

function setLabelFilter(label) {
  activeLabel = label;
  document.querySelectorAll('.lf-btn').forEach(b => b.classList.toggle('active', b.dataset.label === label));
  document.querySelectorAll('.sum-card').forEach(c => c.classList.toggle('active', c.dataset.label === label));
  // col 5 = Label; use regex anchors for exact match, empty string = show all
  if (dt) dt.column(5).search(label ? '^' + label + '$' : '', true, false).draw();
}

document.getElementById('search-box').addEventListener('input', function() {
  if (dt) dt.search(this.value).draw();
});

document.querySelectorAll('.lf-btn').forEach(btn => {
  btn.addEventListener('click', () => setLabelFilter(btn.dataset.label));
});

document.querySelectorAll('.sum-card').forEach(card => {
  card.addEventListener('click', () => setLabelFilter(card.dataset.label));
});

function loadData() {
  document.getElementById('last-computed').textContent = 'Computing…';
  fetch('/api/recommendations').then(r => r.json()).then(data => {
    allData = data;
    recomputeAndRender();
    document.getElementById('last-computed').textContent = `Last scored: ${new Date().toLocaleTimeString()}`;
  }).catch(() => { document.getElementById('last-computed').textContent = 'Error loading'; });
}

document.getElementById('refresh-btn').addEventListener('click', () => {
  fetch('/api/recommendations', { method: 'DELETE' }).catch(() => {});
  loadData();
});

const _sentBtn = document.getElementById('sentiment-run-btn');
_sentBtn.addEventListener('click', () => {
  _sentBtn.disabled = true;
  _sentBtn.textContent = '⏳ Running…';
  const el = document.getElementById('sentiment-status');
  el.textContent = 'Sentiment: pass triggered — computing…';
  el.style.color = '#fbbf24';
  fetch('/api/sentiment/run', { method: 'POST' })
    .then(r => r.json())
    .then(res => {
      if (!res.ok) {
        el.textContent = 'Sentiment: ' + res.message;
        el.style.color = '#f87171';
        _sentBtn.disabled = false;
        _sentBtn.textContent = '⚡ Run Sentiment';
        return;
      }
      el.textContent = `Sentiment: scoring ${res.tickers.length} tickers in background…`;
      // Poll until done, then reload recommendations to reflect new scores
      const poll = setInterval(() => {
        fetch('/api/sentiment/status').then(r => r.json()).then(s => {
          if (!s.running) {
            clearInterval(poll);
            _sentBtn.disabled = false;
            _sentBtn.textContent = '⚡ Run Sentiment';
            updateSentimentStatus();
            loadData();   // reload recommendations with updated sentiment scores
          } else {
            el.textContent = `Sentiment: computing… (${s.scored_today} scored so far)`;
          }
        });
      }, 5000);
    })
    .catch(err => {
      el.textContent = 'Sentiment: request failed — ' + err.message;
      el.style.color = '#f87171';
      _sentBtn.disabled = false;
      _sentBtn.textContent = '⚡ Run Sentiment';
    });
});

loadData();

// ── Sentiment status ──────────────────────────────────────────────────────────
function updateSentimentStatus() {
  fetch('/api/sentiment/status').then(r => r.json()).then(s => {
    const el = document.getElementById('sentiment-status');
    if (s.running) {
      el.textContent = `Sentiment: computing… (${s.scored_today} stocks scored so far)`;
      el.style.color = '#fbbf24';
      setTimeout(updateSentimentStatus, 8000);   // poll while running
    } else if (s.scored_today > 0) {
      const t = s.last_run ? new Date(s.last_run).toLocaleTimeString() : '?';
      el.textContent = `Sentiment: ${s.scored_today} stocks scored · last updated ${t}`;
      el.style.color = '#34d399';
    } else {
      el.textContent = 'Sentiment: will compute on first recommendation load';
      el.style.color = '#475569';
    }
  }).catch(() => {
    document.getElementById('sentiment-status').textContent = 'Sentiment: status unavailable';
  });
}
updateSentimentStatus();

// ── Chart popup ──────────────────────────────────────────────────────────────
let priceChart = null, volChart = null, priceSeries = null, volSeries = null;
let _histTicker = null;

function openHistory(ticker) {
  _histTicker = ticker;
  document.getElementById('hist-title').textContent = ticker;
  document.getElementById('hist-meta').textContent = 'Loading…';
  document.getElementById('hist-stats').innerHTML = '';
  document.getElementById('hist-company').innerHTML = '<div style="padding:16px;color:#64748b;font-size:0.8rem">Loading…</div>';
  document.querySelectorAll('.interval-btn').forEach(b => b.classList.remove('active'));
  document.querySelector('.interval-btn[data-interval="1d"]').classList.add('active');
  document.getElementById('hist-popup').classList.add('open');
  document.getElementById('hist-overlay').classList.add('active');
  fetch(`/api/company/${ticker}`).then(r => r.json()).then(renderCompanyPanel).catch(() => {});
  _loadHistChart(ticker, '1d');
}

function _loadHistChart(ticker, interval) {
  document.getElementById('hist-meta').textContent = 'Loading…';
  fetch(`/api/history/${ticker}?interval=${interval}`)
    .then(r => r.json())
    .then(data => _renderHistChart(data, interval))
    .catch(err => { document.getElementById('hist-meta').textContent = 'Error: ' + err.message; });
}

function _renderHistChart(data, interval) {
  const rows = Array.isArray(data) ? data : (data.rows || []);
  if (!rows.length) { document.getElementById('hist-meta').textContent = 'No data'; return; }
  if (interval !== '5m') {
    const high = Math.max(...rows.map(r => r.high)), low = Math.min(...rows.map(r => r.low)), last = rows[rows.length-1];
    document.getElementById('hist-meta').textContent = `${rows.length} trading days  ·  ${rows[0].time} → ${rows[rows.length-1].time}`;
    document.getElementById('hist-stats').innerHTML = `
      <div class="stat-item"><div>Last Close</div><span>${'$'+last.close.toFixed(2)}</span></div>
      <div class="stat-item"><div>52W High</div><span>${'$'+high.toFixed(2)}</span></div>
      <div class="stat-item"><div>52W Low</div><span>${'$'+low.toFixed(2)}</span></div>
      <div class="stat-item"><div>Last Volume</div><span>${(last.volume/1e6).toFixed(2)}M</span></div>`;
  } else {
    document.getElementById('hist-meta').textContent = `${rows.length} bars (5 min)`;
    document.getElementById('hist-stats').innerHTML = '';
  }
  const pEl = document.getElementById('price-chart'), vEl = document.getElementById('vol-chart');
  pEl.innerHTML = ''; vEl.innerHTML = ''; vEl.style.display = 'block';
  if (priceChart) { priceChart.remove(); priceChart = null; }
  if (volChart)   { volChart.remove();   volChart = null; }
  const opts = { autoSize: true, layout: { background: { color: '#1a1d27' }, textColor: '#94a3b8' }, grid: { vertLines: { color: '#1e2235' }, horzLines: { color: '#1e2235' } }, timeScale: { borderColor: '#2d3148', timeVisible: true }, rightPriceScale: { borderColor: '#2d3148' } };
  priceChart = LightweightCharts.createChart(pEl, opts);
  priceSeries = priceChart.addCandlestickSeries({ upColor: '#34d399', downColor: '#f87171', borderUpColor: '#34d399', borderDownColor: '#f87171', wickUpColor: '#34d399', wickDownColor: '#f87171' });
  priceSeries.setData(rows.map(r => ({ time:r.time, open:r.open, high:r.high, low:r.low, close:r.close })));
  priceChart.timeScale().fitContent();
  volChart = LightweightCharts.createChart(vEl, opts);
  volSeries = volChart.addHistogramSeries({ color: '#3730a3', priceFormat: { type: 'volume' } });
  volSeries.setData(rows.map((r,i) => ({ time:r.time, value:r.volume, color: i>0&&r.close>=rows[i-1].close?'#34d39966':'#f8717166' })));
  volChart.timeScale().fitContent();
  priceChart.timeScale().subscribeVisibleLogicalRangeChange(range => { if (range) volChart.timeScale().setVisibleLogicalRange(range); });
  volChart.timeScale().subscribeVisibleLogicalRangeChange(range => { if (range) priceChart.timeScale().setVisibleLogicalRange(range); });
}

function renderCompanyPanel(data) {
  const c = document.getElementById('hist-company'); c.innerHTML = '';
  if (!data || !data.sections) return;
  for (const [sn, fields] of Object.entries(data.sections)) {
    if (!fields || !Object.keys(fields).length) continue;
    const sec = document.createElement('div'); sec.className = 'section';
    sec.innerHTML = `<h3>${sn}</h3><div class="field-grid"></div>`;
    sec.querySelector('h3').addEventListener('click', () => sec.classList.toggle('open'));
    const grid = sec.querySelector('.field-grid');
    for (const [k,v] of Object.entries(fields)) {
      const f = document.createElement('div'); f.className = 'field';
      f.innerHTML = `<label>${k}</label><span>${v == null ? '—' : String(v)}</span>`;
      grid.appendChild(f);
    }
    c.appendChild(sec);
  }
}

document.querySelectorAll('.interval-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.interval-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    if (_histTicker) _loadHistChart(_histTicker, btn.dataset.interval);
  });
});

function closeHistory() {
  document.getElementById('hist-popup').classList.remove('open');
  document.getElementById('hist-overlay').classList.remove('active');
}
document.getElementById('hist-close').addEventListener('click', closeHistory);
document.getElementById('hist-overlay').addEventListener('click', closeHistory);

// ── Info popup ────────────────────────────────────────────────────────────────
function openInfo()  { document.getElementById('info-popup').classList.add('open'); document.getElementById('info-overlay').classList.add('open'); }
function closeInfo() { document.getElementById('info-popup').classList.remove('open'); document.getElementById('info-overlay').classList.remove('open'); }
document.getElementById('info-btn').addEventListener('click', openInfo);
document.getElementById('info-close').addEventListener('click', closeInfo);
document.getElementById('info-overlay').addEventListener('click', closeInfo);
</script>
</body>
</html>"""

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
