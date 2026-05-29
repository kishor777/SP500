"""MySQL connection config — copy this file to db_config.py and fill in your values.

    cp db_config.example.py db_config.py
"""
import os
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool

DB = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     int(os.getenv("DB_PORT", "3306")),
    "user":     os.getenv("DB_USER",     "your_db_user"),
    "password": os.getenv("DB_PASSWORD", "your_db_password"),
    "database": os.getenv("DB_NAME",     "sp500"),
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
        _engine = create_engine(
            url,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
        )
    return _engine


# --- paste the rest of db_config.py below (all create_*_table functions) ---
