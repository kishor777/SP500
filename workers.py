"""Background workers: price refresh loop, data backfills, paper trades."""
import time
import pandas as pd
from datetime import datetime as _dt, date as _date

import state
from helpers import _market_is_open, _yf_session, _save_intraday_bars, load_history, get_setting
from db_config import get_engine
from config import SCHEDULER_CONFIG, _NYSE_TZ

# ── Intraday backfill (runs once at startup) ───────────────────────────────────
def _intraday_backfill():
    import yfinance as yf
    from sqlalchemy import text as _t
    try:
        engine = get_engine()
        with engine.connect() as conn:
            count = conn.execute(_t("SELECT COUNT(*) FROM sp500_intraday")).scalar()
        if count and count > SCHEDULER_CONFIG["intraday_skip_threshold"]:
            return
    except Exception:
        return

    tickers = list(state.df.index)
    batch_size = SCHEDULER_CONFIG["intraday_batch_size"]
    _days = SCHEDULER_CONFIG["intraday_backfill_days"]
    batches = [tickers[i:i+batch_size] for i in range(0, len(tickers), batch_size)]
    total_saved = 0
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
        time.sleep(1)
    print(f"[intraday] Backfill complete — {total_saved:,} bars total")


# ── 2-year daily history backfill ─────────────────────────────────────────────
def _history_backfill_2y():
    import yfinance as yf
    from sqlalchemy import text as _t
    from datetime import timedelta

    target_start = (_date.today() - timedelta(days=SCHEDULER_CONFIG["hist_backfill_days"])).isoformat()
    try:
        engine = get_engine()
        with engine.connect() as conn:
            earliest = conn.execute(_t("SELECT MIN(date) FROM sp500_history")).scalar()
        if earliest and str(earliest) <= target_start:
            state._hist_backfill_status.update({"state": "done", "msg": "Already complete"})
            print(f"[hist-backfill] Already have data from {earliest} — skipping")
            return
    except Exception as e:
        print(f"[hist-backfill] Check failed ({e}), proceeding with backfill")

    tickers = list(state.df.index)
    batch_size = SCHEDULER_CONFIG["hist_batch_size"]
    batches = [tickers[i:i+batch_size] for i in range(0, len(tickers), batch_size)]
    state._hist_backfill_status.update({"state": "running", "done": 0,
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
            state._hist_backfill_status.update({
                "done": i, "rows_added": total_rows,
                "msg": f"{i}/{len(batches)} batches — {total_rows:,} rows added"
            })
            print(f"[hist-backfill] Batch {i}/{len(batches)} — {batch_rows} rows (total {total_rows:,})")
        except Exception as e:
            print(f"[hist-backfill] Batch {i} error: {e}")
        time.sleep(1)

    state.hist_by_ticker = load_history()
    state._hist_backfill_status.update({"state": "done",
                                        "msg": f"Complete — {total_rows:,} rows added, history reloaded"})
    print(f"[hist-backfill] Done — {total_rows:,} rows added. hist_by_ticker reloaded.")


# ── Paper trades ───────────────────────────────────────────────────────────────
def _compute_quality(metrics: dict) -> dict:
    """Return quality percentile score (0-100) per ticker across 4 fundamental metrics."""
    quality = {}
    counts  = {}
    for col_idx in range(4):
        vals = [(tk, v[col_idx]) for tk, v in metrics.items() if v[col_idx] is not None]
        vals.sort(key=lambda x: x[1])
        for rank, (tk, _) in enumerate(vals):
            quality[tk] = quality.get(tk, 0) + (rank / len(vals) * 100)
            counts[tk]  = counts.get(tk, 0) + 1
    return {tk: round(quality[tk] / counts[tk], 1) for tk in quality}


def _vol_trade_buy(today_str: str):
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

            # Quality filter: compute percentile rank across 4 metrics, keep >= 70
            qrows = conn.execute(_t(
                "SELECT ticker, returnOnEquity, profitMargins, grossMargins, operatingMargins "
                "FROM sp500_info"
            )).fetchall()
        quality_map = _compute_quality({r.ticker: [r.returnOnEquity, r.profitMargins,
                                                    r.grossMargins, r.operatingMargins]
                                         for r in qrows})
    except Exception as e:
        print(f"[vol-trades] buy data query error: {e}")
        return

    now_et  = _dt.now(_NYSE_TZ)
    open_t  = now_et.replace(hour=9,  minute=30, second=0, microsecond=0)
    close_t = now_et.replace(hour=16, minute=0,  second=0, microsecond=0)
    elapsed = (now_et - open_t).total_seconds()
    prorate = max(0.01, min(1.0, elapsed / (close_t - open_t).total_seconds()))

    candidates = []
    for ticker in list(state.df.index):
        ticker = str(ticker)
        if quality_map.get(ticker, 0) < get_setting("vol_trade_quality_min", 70):
            continue
        vol_1m = vol_1m_map.get(ticker)
        im = intra_map.get(ticker, {})
        vol_today = im.get("vol_today")
        live = im.get("last_close")
        expected = vol_1m * prorate if vol_1m else None
        if not (vol_today and expected and vol_today > get_setting("vol_trade_vol_ratio_min", 2.0) * expected and live):
            continue
        prev_close = prev_close_map.get(ticker)
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
                VALUES (:d, :tk, :bt, :bp, :amt, 'open')
            """), {"d": today_str, "tk": c["ticker"], "bt": buy_time, "bp": c["buy_price"],
                   "amt": get_setting("vol_trade_amount_usd", 1000)})
        conn.commit()
    print(f"[vol-trades] bought {[c['ticker'] for c in top3]} @ {buy_time.strftime('%H:%M ET')}")


def _vol_trade_sell(today_str: str):
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
                SET sell_time=:st, sell_price=:sp, pnl_dollar=:pd, pnl_pct=:pp, status='sold'
                WHERE id=:id
            """), {"st": sell_time, "sp": sell_price, "pd": pnl_dollar,
                   "pp": pnl_pct, "id": t.id})
        conn.commit()
    print(f"[vol-trades] sold {[t.ticker for t in open_trades]} @ {sell_time.strftime('%H:%M ET')}")


# ── Price refresh loop ─────────────────────────────────────────────────────────
def _price_refresh_loop():
    import yfinance as yf

    while True:
        time.sleep(get_setting("refresh_interval_sec", SCHEDULER_CONFIG["refresh_interval_sec"]))
        if not _market_is_open():
            state._last_refresh["status"] = "closed"
            print(f"[refresh] market closed — skipping ({_dt.now(_NYSE_TZ).strftime('%H:%M ET')})")
            continue
        try:
            tickers = list(state.df.index)
            sess = _yf_session()

            daily = yf.download(tickers, period="2d", interval="1d",
                                auto_adjust=True, progress=False, threads=True, session=sess)
            if not daily.empty:
                today   = _date.today().isoformat()
                updated = 0
                for ticker, price in daily["Close"].iloc[-1].items():
                    if pd.isna(price) or ticker not in state.df.index:
                        continue
                    price = round(float(price), 4)
                    state.df.at[ticker, "currentPrice"] = price
                    rows = state.hist_by_ticker.get(ticker)
                    if rows:
                        if rows[-1]["time"] == today:
                            rows[-1]["close"] = price
                        else:
                            rows.append({"time": today, "open": price, "high": price,
                                         "low": price, "close": price, "volume": 0})
                    updated += 1
                state._last_refresh.update({"ts": _dt.now(_NYSE_TZ).strftime("%H:%M:%S ET"),
                                            "count": updated, "status": "open",
                                            "epoch": time.time()})
                print(f"[refresh] {updated} prices updated at {state._last_refresh['ts']}")

            intra = yf.download(tickers, period="1d", interval="5m",
                                auto_adjust=True, progress=False, threads=True, session=sess)
            if not intra.empty:
                saved = _save_intraday_bars(intra, tickers)
                print(f"[intraday] {saved} bars upserted")

            _td = _date.today().isoformat()
            _now_et = _dt.now(_NYSE_TZ)
            _now_min  = _now_et.hour * 60 + _now_et.minute
            _buy_min  = get_setting("vol_trade_buy_hour",    SCHEDULER_CONFIG["vol_trade_buy_hour"])   * 60 \
                      + get_setting("vol_trade_buy_minute",  SCHEDULER_CONFIG["vol_trade_buy_minute"])
            _sell_min = get_setting("vol_trade_sell_hour",   SCHEDULER_CONFIG["vol_trade_sell_hour"])  * 60 \
                      + get_setting("vol_trade_sell_minute", SCHEDULER_CONFIG["vol_trade_sell_minute"])
            if _now_min >= _buy_min and state._vol_trade_buy_date != _td:
                _vol_trade_buy(_td)
                state._vol_trade_buy_date = _td
            if _now_min >= _sell_min and state._vol_trade_sell_date != _td:
                _vol_trade_sell(_td)
                state._vol_trade_sell_date = _td

            # Daily fundamentals refresh (runs once per day at first refresh)
            if not hasattr(state, '_fund_update_date') or state._fund_update_date != _td:
                _fundamentals_daily_update()
                state._fund_update_date = _td

        except Exception as _ex:
            state._last_refresh["status"] = "error"
            print(f"[refresh] error: {_ex}")


def _delayed_backfill():
    time.sleep(10)
    _intraday_backfill()
    _history_backfill_2y()
    _fundamentals_backfill()


# ── Historical fundamentals backfill ──────────────────────────────────────────

def _safe_row(df, *names):
    """Find the first matching row in a DataFrame by trying multiple field names."""
    if df is None or df.empty:
        return None
    for n in names:
        if n in df.index:
            return df.loc[n]
    return None


def _safe_int(series, col):
    if series is None:
        return None
    try:
        v = series.get(col) if hasattr(series, 'get') else (series[col] if col in series.index else None)
        return int(v) if v is not None and not pd.isna(v) else None
    except Exception:
        return None


def _fetch_ticker_fundamentals(ticker: str, years: int = 2) -> tuple[list, list]:
    """Return (fundamental_rows, analyst_rows) for a single ticker."""
    import yfinance as yf
    from datetime import timedelta

    cutoff = (_date.today() - timedelta(days=365 * years)).isoformat()
    t = yf.Ticker(ticker, session=_yf_session())

    fund_rows = []
    try:
        fin = t.quarterly_financials
        bal = t.quarterly_balance_sheet

        if fin is not None and not fin.empty:
            revenue_s   = _safe_row(fin, 'Total Revenue', 'TotalRevenue')
            gross_s     = _safe_row(fin, 'Gross Profit', 'GrossProfit')
            op_s        = _safe_row(fin, 'Operating Income', 'OperatingIncome', 'EBIT',
                                    'Total Operating Income As Reported')
            ni_s        = _safe_row(fin, 'Net Income', 'NetIncome',
                                    'Net Income Common Stockholders',
                                    'Net Income From Continuing Operation Net Minority Interest')
            ebitda_s    = _safe_row(fin, 'EBITDA', 'Normalized EBITDA')
            equity_s    = _safe_row(bal, 'Stockholders Equity', 'Common Stock Equity',
                                    'Total Stockholder Equity') if bal is not None else None
            assets_s    = _safe_row(bal, 'Total Assets', 'TotalAssets') if bal is not None else None
            debt_s      = _safe_row(bal, 'Total Debt', 'TotalDebt',
                                    'Long Term Debt') if bal is not None else None

            cols = list(fin.columns)
            for col in cols:
                period_date = pd.Timestamp(col).date().isoformat()
                if period_date < cutoff:
                    continue

                rev  = _safe_int(revenue_s,  col)
                gp   = _safe_int(gross_s,    col)
                oi   = _safe_int(op_s,       col)
                ni   = _safe_int(ni_s,       col)
                eb   = _safe_int(ebitda_s,   col)
                bv   = _safe_int(equity_s,   col)
                ta   = _safe_int(assets_s,   col)
                td   = _safe_int(debt_s,     col)

                gm   = round(gp / rev, 4) if gp is not None and rev else None
                om   = round(oi / rev, 4) if oi is not None and rev else None
                nm   = round(ni / rev, 4) if ni is not None and rev else None

                # YoY growth vs same quarter 1 year ago
                yoy_col = None
                pd_date = pd.Timestamp(col).date()
                for c in cols:
                    diff = (pd_date - pd.Timestamp(c).date()).days
                    if 340 < diff < 395:
                        yoy_col = c
                        break

                rev_growth = earn_growth = None
                if yoy_col:
                    rev_y  = _safe_int(revenue_s, yoy_col)
                    ni_y   = _safe_int(ni_s,      yoy_col)
                    if rev and rev_y and rev_y != 0:
                        rev_growth  = round((rev - rev_y) / abs(rev_y), 4)
                    if ni is not None and ni_y is not None and ni_y != 0:
                        earn_growth = round((ni - ni_y) / abs(ni_y), 4)

                # ROE: TTM net income / avg book value (approximated with single period)
                roe = None
                if ni is not None and bv and bv != 0:
                    # Annualise quarterly net income × 4 / book value
                    roe = round((ni * 4) / abs(bv), 4)

                fund_rows.append({
                    "ticker": ticker, "period_date": period_date,
                    "total_revenue": rev, "gross_profit": gp,
                    "operating_income": oi, "net_income": ni, "ebitda": eb,
                    "gross_margin": gm, "operating_margin": om, "net_margin": nm,
                    "revenue_growth": rev_growth, "earnings_growth": earn_growth,
                    "book_value": bv, "total_assets": ta, "total_debt": td, "roe": roe,
                })
    except Exception as e:
        print(f"[fund] {ticker} fundamentals error: {e}")

    analyst_rows = []
    try:
        # yfinance 0.2.x: upgrade/downgrade history is under upgrades_downgrades
        # (ticker.recommendations now returns the aggregated consensus summary, not history)
        recs = t.upgrades_downgrades
        if recs is not None and not recs.empty:
            recs = recs.reset_index()
            # Column names vary: GradeDate / Date / index
            date_col = next((c for c in recs.columns
                             if c.lower() in ("gradedate", "date", "index")), recs.columns[0])
            for _, row in recs.iterrows():
                try:
                    rec_date = pd.Timestamp(row[date_col]).date().isoformat()
                    if rec_date < cutoff:
                        continue
                    analyst_rows.append({
                        "ticker":   ticker,
                        "rec_date": rec_date,
                        "firm":     str(row.get("Firm", row.get("firm", "")))[:200],
                        "rec_to":   str(row.get("ToGrade", row.get("To Grade",
                                        row.get("toGrade", ""))))[:100],
                        "rec_from": str(row.get("FromGrade", row.get("From Grade",
                                        row.get("fromGrade", ""))))[:100],
                        "action":   str(row.get("Action", row.get("action", "")))[:50],
                    })
                except Exception:
                    pass
    except Exception as e:
        print(f"[fund] {ticker} analyst error: {e}")

    return fund_rows, analyst_rows


def _fundamentals_backfill():
    """One-time download of 2-year quarterly fundamentals for all S&P 500 tickers."""
    from sqlalchemy import text as _t

    engine = get_engine()
    try:
        with engine.connect() as conn:
            count = conn.execute(_t(
                "SELECT COUNT(DISTINCT ticker) FROM sp500_fundamentals"
            )).scalar()
        if count and count > 400:
            state._fund_backfill_status.update({"state": "done", "msg": f"Already have {count} tickers"})
            print(f"[fund-backfill] Already have {count} tickers — skipping")
            return
    except Exception as e:
        print(f"[fund-backfill] Pre-check failed ({e}), proceeding")

    tickers = [str(t) for t in state.df.index]
    total   = len(tickers)
    state._fund_backfill_status.update({"state": "running", "done": 0, "total": total,
                                        "msg": f"0/{total} tickers"})
    print(f"[fund-backfill] Starting 2-year backfill for {total} tickers…")

    INSERT_FUND = _t("""
        INSERT INTO sp500_fundamentals
          (ticker, period_date, total_revenue, gross_profit, operating_income,
           net_income, ebitda, gross_margin, operating_margin, net_margin,
           revenue_growth, earnings_growth, book_value, total_assets, total_debt, roe)
        VALUES
          (:ticker, :period_date, :total_revenue, :gross_profit, :operating_income,
           :net_income, :ebitda, :gross_margin, :operating_margin, :net_margin,
           :revenue_growth, :earnings_growth, :book_value, :total_assets, :total_debt, :roe)
        ON DUPLICATE KEY UPDATE
          gross_margin=VALUES(gross_margin), operating_margin=VALUES(operating_margin),
          net_margin=VALUES(net_margin), revenue_growth=VALUES(revenue_growth),
          earnings_growth=VALUES(earnings_growth), roe=VALUES(roe),
          total_revenue=VALUES(total_revenue), net_income=VALUES(net_income),
          book_value=VALUES(book_value), fetched_at=NOW()
    """)
    INSERT_REC = _t("""
        INSERT INTO sp500_analyst_recs (ticker, rec_date, firm, rec_to, rec_from, action)
        VALUES (:ticker, :rec_date, :firm, :rec_to, :rec_from, :action)
    """)

    for i, ticker in enumerate(tickers, 1):
        try:
            fund_rows, analyst_rows = _fetch_ticker_fundamentals(ticker, years=2)
            with engine.connect() as conn:
                if fund_rows:
                    conn.execute(INSERT_FUND, fund_rows)
                if analyst_rows:
                    conn.execute(INSERT_REC, analyst_rows)
                conn.commit()
        except Exception as e:
            print(f"[fund-backfill] {ticker}: {e}")
        state._fund_backfill_status.update({"done": i, "msg": f"{i}/{total} tickers"})
        if i % 50 == 0:
            print(f"[fund-backfill] {i}/{total} tickers done")
        time.sleep(0.6)

    state._fund_backfill_status.update({"state": "done", "msg": f"Complete — {total} tickers"})
    print("[fund-backfill] Complete")


def _fundamentals_daily_update():
    """Fetch new quarterly data for tickers whose latest period is > 85 days old."""
    from sqlalchemy import text as _t

    engine = get_engine()
    try:
        with engine.connect() as conn:
            stale = conn.execute(_t("""
                SELECT ticker FROM (
                    SELECT ticker, MAX(period_date) AS latest
                    FROM sp500_fundamentals GROUP BY ticker
                ) t WHERE latest < DATE_SUB(CURDATE(), INTERVAL 85 DAY)
            """)).fetchall()
        tickers_to_update = [r.ticker for r in stale]
        if not tickers_to_update:
            return
        print(f"[fund-update] Refreshing {len(tickers_to_update)} stale tickers")
        for ticker in tickers_to_update:
            try:
                fund_rows, _ = _fetch_ticker_fundamentals(ticker, years=1)
                if fund_rows:
                    with engine.connect() as conn:
                        conn.execute(_t("""
                            INSERT INTO sp500_fundamentals
                              (ticker, period_date, total_revenue, gross_profit, operating_income,
                               net_income, ebitda, gross_margin, operating_margin, net_margin,
                               revenue_growth, earnings_growth, book_value, total_assets,
                               total_debt, roe)
                            VALUES
                              (:ticker, :period_date, :total_revenue, :gross_profit, :operating_income,
                               :net_income, :ebitda, :gross_margin, :operating_margin, :net_margin,
                               :revenue_growth, :earnings_growth, :book_value, :total_assets,
                               :total_debt, :roe)
                            ON DUPLICATE KEY UPDATE
                              gross_margin=VALUES(gross_margin), net_income=VALUES(net_income),
                              revenue_growth=VALUES(revenue_growth), roe=VALUES(roe),
                              fetched_at=NOW()
                        """), fund_rows)
                        conn.commit()
            except Exception as e:
                print(f"[fund-update] {ticker}: {e}")
            time.sleep(0.5)
        print(f"[fund-update] Done — refreshed {len(tickers_to_update)} tickers")
    except Exception as e:
        print(f"[fund-update] error: {e}")


# ── Thread startup ─────────────────────────────────────────────────────────────
def start_workers():
    import threading
    state._refresh_thread = threading.Thread(
        target=_price_refresh_loop, daemon=True, name="price-refresh"
    )
    state._refresh_thread.start()

    state._backfill_thread = threading.Thread(
        target=_delayed_backfill, daemon=True, name="backfill"
    )
    state._backfill_thread.start()
