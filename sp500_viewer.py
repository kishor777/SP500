"""S&P 500 app — slim entry point. All logic lives in blueprints/ and helpers."""
import logging
import os

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

for _lib in ("yfinance", "urllib3", "requests", "peewee", "apscheduler",
             "PIL", "transformers", "torch", "werkzeug"):
    logging.getLogger(_lib).setLevel(logging.WARNING)
logging.getLogger("sqlalchemy").setLevel(logging.ERROR)

from flask import Flask

import state
from extensions import limiter, init_auth
from helpers import load_info, load_history, build_screener_meta
from edgar import build_name_to_ticker, save_guru_rules_to_db, load_guru_rules_from_db
from workers import start_workers
from db_config import (get_engine, create_guru_tables, create_guru_rules_table,
                       migrate_guru_tables, create_intraday_table,
                       create_screener_filters_table, create_vol_trades_table,
                       create_sentiment_table, create_sp500_history_table,
                       create_portfolio_table, migrate_portfolio_table,
                       create_admin_settings_table)
from helpers import load_admin_settings
from config import GURUS, SCREENER_NUM_GROUPS

from blueprints.admin           import bp as admin_bp
from blueprints.market_data     import bp as market_data_bp
from blueprints.screener        import bp as screener_bp
from blueprints.guru            import bp as guru_bp
from blueprints.recommendations import bp as recommendations_bp
from blueprints.portfolio       import bp as portfolio_bp
from blueprints.admin_settings  import bp as admin_settings_bp

# ── App factory ───────────────────────────────────────────────────────────────
app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("FLASK_SECRET_KEY") or os.urandom(24)

limiter.init_app(app)
init_auth(app)

app.register_blueprint(admin_bp)
app.register_blueprint(market_data_bp)
app.register_blueprint(screener_bp)
app.register_blueprint(guru_bp)
app.register_blueprint(recommendations_bp)
app.register_blueprint(portfolio_bp)
app.register_blueprint(admin_settings_bp)

# ── Startup: tables → data → state → workers ──────────────────────────────────
try:
    create_sp500_history_table()
    create_guru_tables()
    migrate_guru_tables()
    create_intraday_table()
    create_screener_filters_table()
    create_vol_trades_table()
    create_sentiment_table()
    create_portfolio_table()
    migrate_portfolio_table()
    create_admin_settings_table()
except Exception as _e:
    print(f"Could not create/migrate tables: {_e}")

# Load core data into shared state
state.df              = load_info()
state.hist_by_ticker  = load_history()
state._name_to_ticker = build_name_to_ticker()

# Build screener metadata
state._s_num_meta, state._s_cat_meta = build_screener_meta()
state._s_group_order = list(SCREENER_NUM_GROUPS.keys())

# Guru display helpers
state._GURU_SHORT = {slug: g["name"].split()[-1] for slug, g in GURUS.items()}
state._GURU_COLOR = {slug: g["color"] for slug, g in GURUS.items()}

# Seed / load guru rules from DB
try:
    create_guru_rules_table()
    _db_rules = load_guru_rules_from_db()
    if not _db_rules:
        print("Seeding guru rules to DB…")
        save_guru_rules_to_db()
        _db_rules = load_guru_rules_from_db()
    for _slug, _rules in _db_rules.items():
        if _slug in GURUS:
            GURUS[_slug]["rules"] = _rules
except Exception as _e:
    print(f"Could not initialize guru rules DB: {_e}")

# Load DB-backed settings into state before workers start
state.settings = load_admin_settings()
print(f"[startup] Admin settings: {len(state.settings)} override(s) loaded from DB")

# Diagnostic: confirm ANTHROPIC_API_KEY is loaded
_ak = os.environ.get("ANTHROPIC_API_KEY", "")
print(f"[startup] ANTHROPIC_API_KEY: {'SET (' + str(len(_ak)) + ' chars)' if _ak else 'NOT SET — Claude Recs will not work'}")

# Start background threads
start_workers()

if __name__ == "__main__":
    app.run(debug=True, port=5050, threaded=True, use_reloader=False)
