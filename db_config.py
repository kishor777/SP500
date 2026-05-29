"""MySQL connection config — credentials from .env (local) or Railway env vars (cloud)."""
import os
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool

# Load .env if present (local dev); Railway sets env vars directly
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Support both local .env names and Railway's auto-injected MySQL plugin names
def _env(*keys, default=""):
    for k in keys:
        v = os.getenv(k)
        if v:
            return v
    return default

_ON_RAILWAY = bool(os.getenv("RAILWAY_ENVIRONMENT"))

DB = {
    "host":     _env("DB_HOST", "MYSQLHOST",     default="localhost"),
    "port":     int(_env("DB_PORT", "MYSQLPORT", default="3306")),
    "user":     _env("DB_USER", "MYSQLUSER",     default="root"),
    "password": _env("DB_PASSWORD", "MYSQLPASSWORD", default=""),
    "database": _env("DB_NAME", "MYSQLDATABASE", default="sp500"),
}

_engine = None


def get_engine():
    global _engine
    if _engine is None:
        url = (
            f"mysql+pymysql://{DB['user']}:{quote_plus(DB['password'])}"
            f"@{DB['host']}:{DB['port']}/{DB['database']}"
            f"?charset=utf8mb4"
        )
        # Railway MySQL requires SSL; local dev skips it
        connect_args = {"ssl": {"verify_cert": False}} if _ON_RAILWAY else {}
        _engine = create_engine(
            url,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            connect_args=connect_args,
        )
    return _engine


def create_guru_rules_table():
    """Create guru_rules table for storing editable investment rules."""
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS guru_rules (
                id          INT AUTO_INCREMENT PRIMARY KEY,
                slug        VARCHAR(50)                          NOT NULL,
                field_id    VARCHAR(100)                         NOT NULL,
                rule_type   ENUM('min','max','categorical')      NOT NULL,
                num_value   DOUBLE,
                cat_values  TEXT COMMENT 'JSON array for categorical rules',
                is_active   TINYINT(1) NOT NULL DEFAULT 1,
                updated_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                            ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY  uk_slug_field_type (slug, field_id, rule_type),
                INDEX       idx_slug (slug)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        conn.commit()


def create_guru_tables():
    """Create guru_filing_meta and guru_holdings tables if they don't exist."""
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS guru_filing_meta (
                slug        VARCHAR(50)  NOT NULL,
                filing_date DATE         NOT NULL,
                fetched_at  DATETIME     NOT NULL,
                source      VARCHAR(20)  NOT NULL DEFAULT '13f',
                PRIMARY KEY (slug, filing_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS guru_holdings (
                id          INT AUTO_INCREMENT PRIMARY KEY,
                slug        VARCHAR(50)  NOT NULL,
                filing_date DATE         NOT NULL,
                name        VARCHAR(500) NOT NULL,
                cusip       VARCHAR(20),
                ticker      VARCHAR(20),
                put_call    VARCHAR(10)  NOT NULL DEFAULT '',
                value       BIGINT       NOT NULL,
                shares      BIGINT       NOT NULL DEFAULT 0,
                weight      FLOAT,
                change_tag  VARCHAR(20),
                INDEX idx_slug_date (slug, filing_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        conn.commit()


def migrate_guru_tables():
    """Migrate existing single-filing tables to multi-filing schema (idempotent)."""
    engine = get_engine()
    with engine.connect() as conn:
        # Migrate guru_filing_meta: single-column PK(slug) → composite PK(slug, filing_date)
        try:
            pk_cols = [r[0] for r in conn.execute(text("""
                SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'guru_filing_meta'
                AND CONSTRAINT_NAME = 'PRIMARY' ORDER BY ORDINAL_POSITION
            """)).fetchall()]
            if pk_cols == ['slug']:
                conn.execute(text(
                    "ALTER TABLE guru_filing_meta "
                    "DROP PRIMARY KEY, ADD PRIMARY KEY (slug, filing_date)"
                ))
                conn.commit()
                print("Migrated guru_filing_meta → composite PK (slug, filing_date)")
        except Exception as e:
            print(f"guru_filing_meta migration skipped: {e}")

        # Migrate guru_holdings: add idx_slug_date if missing
        try:
            has_idx = conn.execute(text("""
                SELECT 1 FROM INFORMATION_SCHEMA.STATISTICS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'guru_holdings'
                AND INDEX_NAME = 'idx_slug_date'
            """)).fetchone()
            if not has_idx:
                conn.execute(text(
                    "ALTER TABLE guru_holdings ADD INDEX idx_slug_date (slug, filing_date)"
                ))
                conn.commit()
                print("Added idx_slug_date to guru_holdings")
        except Exception as e:
            print(f"guru_holdings index migration skipped: {e}")


def create_intraday_table():
    """Create sp500_intraday table for 5-minute OHLCV bars (rolling 60-day window)."""
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS sp500_intraday (
                ticker  VARCHAR(20) NOT NULL,
                dt      DATETIME    NOT NULL,
                open    FLOAT,
                high    FLOAT,
                low     FLOAT,
                close   FLOAT,
                volume  BIGINT,
                PRIMARY KEY (ticker, dt),
                INDEX idx_ticker (ticker)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        conn.commit()


def create_screener_filters_table():
    """Create screener_saved_filters table for named filter presets."""
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS screener_saved_filters (
                id         INT AUTO_INCREMENT PRIMARY KEY,
                name       VARCHAR(100) NOT NULL UNIQUE,
                filter_json TEXT NOT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                           ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        conn.commit()


def create_vol_trades_table():
    """Create vol_trades table for simulated paper trades."""
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS vol_trades (
                id          INT AUTO_INCREMENT PRIMARY KEY,
                trade_date  DATE NOT NULL,
                ticker      VARCHAR(20) NOT NULL,
                buy_time    DATETIME,
                buy_price   FLOAT,
                amount_usd  FLOAT NOT NULL DEFAULT 1000,
                sell_time   DATETIME,
                sell_price  FLOAT,
                pnl_dollar  FLOAT,
                pnl_pct     FLOAT,
                status      ENUM('open','sold') NOT NULL DEFAULT 'open',
                INDEX idx_date   (trade_date),
                INDEX idx_status (status)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        conn.commit()


def create_sentiment_table():
    """Create sentiment_scores table — one row per ticker per day, upserted on refresh."""
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS sentiment_scores (
                id                INT AUTO_INCREMENT PRIMARY KEY,
                ticker            VARCHAR(20)  NOT NULL,
                score_date        DATE         NOT NULL,
                score             FLOAT        NOT NULL,
                stocktwits_score  FLOAT,
                reddit_score      FLOAT,
                volume_spike      FLOAT,
                momentum          FLOAT,
                post_count        INT          NOT NULL DEFAULT 0,
                classification    VARCHAR(20),
                computed_at       DATETIME     NOT NULL,
                UNIQUE KEY uq_ticker_date (ticker, score_date),
                INDEX      idx_score_date (score_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        conn.commit()


def ensure_database():
    """Create the database if it doesn't exist yet (run once on first use)."""
    url_no_db = (
        f"mysql+pymysql://{DB['user']}:{quote_plus(DB['password'])}"
        f"@{DB['host']}:{DB['port']}?charset=utf8mb4"
    )
    tmp = create_engine(url_no_db)
    with tmp.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{DB['database']}` "
                          f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"))
    tmp.dispose()
