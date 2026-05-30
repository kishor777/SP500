"""Backup irreplaceable MySQL tables to backups/backup_YYYY-MM-DD.sql

Usage:
    python backup.py              # creates backups/backup_2026-05-29.sql
    python backup.py --restore backups/backup_2026-05-29.sql
"""
import logging
import sys
import os
from datetime import date
from pathlib import Path
from sqlalchemy import text

# Suppress SQLAlchemy WARNING-level noise (duplicate-key errors during restore, etc.)
logging.getLogger("sqlalchemy").setLevel(logging.ERROR)

# Tables backed up (sp500_history/intraday excluded — large, auto-backfilled on startup)
BACKUP_TABLES = [
    "sp500_info",
    "guru_rules",
    "screener_saved_filters",
    "vol_trades",
    "guru_filing_meta",
    "guru_holdings",
    "sentiment_scores",
]

BACKUP_DIR = Path(__file__).parent / "backups"


def _quote(val) -> str:
    """Safely quote a value for SQL INSERT."""
    if val is None:
        return "NULL"
    if isinstance(val, (int, float)):
        return str(val)
    return "'" + str(val).replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n").replace("\r", "\\r") + "'"


def backup(output_path: Path | None = None):
    from db_config import get_engine
    engine = get_engine()

    if output_path is None:
        BACKUP_DIR.mkdir(exist_ok=True)
        output_path = BACKUP_DIR / f"backup_{date.today().isoformat()}.sql"

    lines = [
        "-- SP500 App — MySQL backup",
        f"-- Date: {date.today().isoformat()}",
        f"-- Tables: {', '.join(BACKUP_TABLES)}",
        "-- Restore: python backup.py --restore <this_file>",
        "",
        "SET FOREIGN_KEY_CHECKS=0;",
        "SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO';",
        "",
    ]

    total_rows = 0
    with engine.connect() as conn:
        for table in BACKUP_TABLES:
            # Check table exists
            exists = conn.execute(text(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name = :t"
            ), {"t": table}).scalar()
            if not exists:
                lines.append(f"-- SKIP {table} (table does not exist)\n")
                continue

            rows = conn.execute(text(f"SELECT * FROM `{table}`")).fetchall()
            cols = conn.execute(text(f"DESCRIBE `{table}`")).fetchall()
            col_names = [c[0] for c in cols]

            # Get CREATE TABLE DDL so restore works on an empty database
            create_row = conn.execute(text(f"SHOW CREATE TABLE `{table}`")).fetchone()
            create_ddl = create_row[1] if create_row else None

            lines.append(f"-- ── {table} ({len(rows)} rows) " + "-" * 40)
            if create_ddl:
                # Convert to CREATE TABLE IF NOT EXISTS
                create_ddl = create_ddl.replace(
                    "CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ", 1
                )
                lines.append(create_ddl + ";")
            lines.append(f"TRUNCATE TABLE `{table}`;")

            for row in rows:
                vals = ", ".join(_quote(v) for v in row)
                col_list = ", ".join(f"`{c}`" for c in col_names)
                lines.append(f"INSERT IGNORE INTO `{table}` ({col_list}) VALUES ({vals});")

            lines.append("")
            total_rows += len(rows)
            print(f"  {table:<30} {len(rows):>6} rows")

    lines.append("SET FOREIGN_KEY_CHECKS=1;")
    output_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nBackup saved: {output_path}  ({total_rows} total rows)")
    return output_path


def restore(sql_path: str):
    from db_config import get_engine
    engine = get_engine()
    content = Path(sql_path).read_text(encoding="utf-8")
    statements = [s.strip() for s in content.split(";\n") if s.strip() and not s.strip().startswith("--")]
    restored = 0
    skipped = 0
    with engine.connect() as conn:
        for stmt in statements:
            # Force INSERT IGNORE so old backup files (with INSERT INTO) never raise IntegrityError
            upper = stmt.upper().lstrip()
            if upper.startswith("INSERT ") and "IGNORE" not in upper[:20]:
                stmt = "INSERT IGNORE" + stmt[stmt.upper().index("INSERT") + 6:]
            try:
                conn.execute(text(stmt))
                if upper.startswith("INSERT"):
                    restored += 1
            except Exception as e:
                skipped += 1
                if skipped <= 5:
                    print(f"  WARN: {e} — {stmt[:80]}")
                elif skipped == 6:
                    print("  (further WARN messages suppressed)")
        conn.commit()
    print(f"Restore complete — {restored} rows inserted, {skipped} skipped from {sql_path}")


if __name__ == "__main__":
    if len(sys.argv) >= 3 and sys.argv[1] == "--restore":
        restore(sys.argv[2])
    else:
        backup()
