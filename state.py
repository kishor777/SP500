"""Shared mutable application state.

All modules access these objects via `import state` and reference them as
`state.df`, `state._reco_cache`, etc.  Never do `from state import df; df = new`
— that only rebinds the local name.  For reassignment always use `state.df = new`.
"""
import pandas as pd

# ── Core data (populated at startup by sp500_viewer) ─────────────────────────
df: pd.DataFrame = pd.DataFrame()
hist_by_ticker: dict = {}
_name_to_ticker: dict = {}

# ── Thread status (mutated in-place by workers) ───────────────────────────────
_last_refresh: dict = {"ts": None, "count": 0, "status": "pending", "epoch": 0.0}
_hist_backfill_status: dict = {"state": "pending", "done": 0, "total": 0,
                               "rows_added": 0, "msg": ""}
_vol_trade_buy_date:  str | None = None
_vol_trade_sell_date: str | None = None

# ── Thread handles (set at startup) ──────────────────────────────────────────
_refresh_thread = None
_backfill_thread = None

# ── Caches (mutated in-place by their respective modules) ────────────────────
_holdings_cache: dict = {}          # slug → (list, filing_date, fetch_time)
_optimized_rules_cache: dict = {}   # {"rules": {...}, "ts": float}
_reco_cache: dict = {"data": [], "ts": 0.0}

# ── Screener metadata (populated at startup) ──────────────────────────────────
_s_num_meta: dict = {}
_s_cat_meta: dict = {}
_s_group_order: list = []

# ── Guru display helpers (populated at startup) ───────────────────────────────
_GURU_SHORT: dict = {}
_GURU_COLOR: dict = {}

# ── Fundamentals backfill status ─────────────────────────────────────────────
_fund_backfill_status: dict = {"state": "idle", "done": 0, "total": 0, "msg": ""}

# ── Admin settings (DB-backed overrides, keyed by setting key_name) ──────────
settings: dict = {}         # raw string values; use get_setting() to read
