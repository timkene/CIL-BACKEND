#!/usr/bin/env python3
"""
Push "AI DRIVEN DATA" schema from local DuckDB to MotherDuck.
No dlt required. Run from DLT root with MOTHERDUCK_TOKEN set.

  MOTHERDUCK_TOKEN=xxx python3 scripts/push_ai_driven_data_to_motherduck.py
"""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = ROOT / "ai_driven_data.duckdb"
MOTHERDUCK_DB = "ai_driven_data"
SCHEMA = "AI DRIVEN DATA"


def main():
    token = os.getenv("MOTHERDUCK_TOKEN") or os.getenv("MOTHERDUCK_PAT")
    if not token:
        print("Set MOTHERDUCK_TOKEN or MOTHERDUCK_PAT in the environment.")
        sys.exit(1)

    db_path = os.getenv("DUCKDB_PATH", str(DEFAULT_DB))
    if not Path(db_path).exists():
        print(f"Local database not found: {db_path}")
        sys.exit(1)

    import duckdb

    local_conn = duckdb.connect(db_path, read_only=True)
    tables = local_conn.execute(f"""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = '{SCHEMA.replace("'", "''")}'
        ORDER BY table_name
    """).fetchall()
    table_names = [r[0] for r in tables]
    local_conn.close()

    md_conn = duckdb.connect(f"md:?motherduck_token={token}")
    md_conn.execute(f"CREATE DATABASE IF NOT EXISTS {MOTHERDUCK_DB}")
    md_conn.execute(f"USE {MOTHERDUCK_DB}")
    md_conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"')
    md_conn.execute(f"ATTACH '{db_path}' AS local_db (READ_ONLY)")

    print("=" * 60)
    print(f"Pushing schema \"{SCHEMA}\" to MotherDuck")
    print("=" * 60)
    print(f"Local DB: {db_path}")
    print(f"MotherDuck DB: {MOTHERDUCK_DB}")
    print(f"Tables: {len(table_names)}\n")

    success = 0
    for table in table_names:
        try:
            md_conn.execute(f'DROP TABLE IF EXISTS "{SCHEMA}"."{table}"')
            md_conn.execute(
                f'CREATE TABLE "{SCHEMA}"."{table}" AS SELECT * FROM local_db."{SCHEMA}"."{table}"'
            )
            n = md_conn.execute(f'SELECT COUNT(*) FROM "{SCHEMA}"."{table}"').fetchone()[0]
            print(f"  {table}: {n:,} rows")
            success += 1
        except Exception as e:
            print(f"  {table}: FAILED - {e}")

    md_conn.execute("DETACH local_db")
    md_conn.close()

    print("=" * 60)
    print(f"Done. {success}/{len(table_names)} tables pushed to MotherDuck.")
    print("=" * 60)


if __name__ == "__main__":
    main()
