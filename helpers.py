"""Shared utility functions used by workers and blueprints."""
import math
import re
import time
import requests
import numpy as np
import pandas as pd
from datetime import datetime as _dt, date as _date, time as _time, timedelta as _timedelta
from bisect import bisect_right as _bisect_right
from zoneinfo import ZoneInfo

import state
from db_config import get_engine
from config import _NYSE_TZ, _NYSE_HOLIDAYS, SCREENER_NUM_GROUPS, SCREENER_CAT_FIELDS

# ── Scalar helpers ─────────────────────────────────────────────────────────────
def clean(val):
    if isinstance(val, np.generic):
        val = val.item()
    if isinstance(val, float) and math.isnan(val):
        return None
    return val

def row_to_dict(row):
    return {k: clean(v) for k, v in row.items()}

# ── Market calendar ────────────────────────────────────────────────────────────
def _market_is_open() -> bool:
    now = _dt.now(_NYSE_TZ)
    if now.weekday() >= 5:
        return False
    if now.date() in _NYSE_HOLIDAYS:
        return False
    return _time(9, 30) <= now.time() < _time(16, 0)

# ── yfinance session ───────────────────────────────────────────────────────────
def _yf_session() -> requests.Session:
    s = requests.Session()
    s.verify = False
    s.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})
    return s

# ── Intraday DB helpers ────────────────────────────────────────────────────────
def _save_intraday_bars(bars_df: pd.DataFrame, tickers: list) -> int:
    from sqlalchemy import text as _t
    from datetime import datetime, timezone, timedelta

    if bars_df.empty:
        return 0
    if not isinstance(bars_df.columns, pd.MultiIndex):
        bars_df.columns = pd.MultiIndex.from_tuples(
            [(c, tickers[0]) for c in bars_df.columns]
        )
    if "Close" not in bars_df.columns.get_level_values(0):
        return 0

    engine = get_engine()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=60)).replace(tzinfo=None)
    saved  = 0

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
        with engine.connect() as conn:
            conn.execute(INSERT_SQL, rows)
            conn.commit()
        saved += len(rows)

    return saved


def _get_intraday(ticker: str) -> list:
    from sqlalchemy import text as _t
    try:
        engine = get_engine()
        with engine.connect() as conn:
            rows = conn.execute(_t("""
                SELECT dt, open, high, low, close, volume
                FROM sp500_intraday WHERE ticker=:tk ORDER BY dt
            """), {"tk": ticker}).fetchall()
        return [{"time": int(r.dt.timestamp()),
                 "open": r.open, "high": r.high,
                 "low":  r.low,  "close": r.close,
                 "volume": r.volume} for r in rows]
    except Exception as e:
        print(f"[intraday] load error [{ticker}]: {e}")
        return []

# ── Price change history ───────────────────────────────────────────────────────
def _price_changes(ticker: str) -> dict:
    rows = state.hist_by_ticker.get(ticker)
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
            changes[key] = round(
                (last_close - rows[idx]["close"]) / rows[idx]["close"] * 100, 2
            )
    return changes

# ── Data loaders (called at startup, result stored in state) ──────────────────
def load_info() -> pd.DataFrame:
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
        print(f"Warning: could not load sp500_info ({e}) — starting with empty data.")
        return pd.DataFrame()


def load_history() -> dict:
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
        print(f"Warning: could not load sp500_history ({e}) — charts will be empty.")
        return {}

# ── Screener metadata ──────────────────────────────────────────────────────────
def build_screener_meta() -> tuple[dict, dict]:
    df = state.df
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
                "min":    round(float(s.min())           * mult, 4),
                "max":    round(float(s.max())           * mult, 4),
                "p5":     round(float(s.quantile(0.05)) * mult, 4),
                "p95":    round(float(s.quantile(0.95)) * mult, 4),
                "median": round(float(s.median())       * mult, 4),
            }
    cat_meta = {}
    for f in SCREENER_CAT_FIELDS:
        fid = f["id"]
        if fid not in df.columns:
            continue
        vals = sorted(df[fid].dropna().unique().tolist())
        cat_meta[fid] = {"label": f["label"], "values": vals}
    return num_meta, cat_meta
