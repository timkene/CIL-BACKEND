#!/usr/bin/env python3
"""
Inspect FOOTBALL schema in local DuckDB (used by Eko React app).
Lists all tables in FOOTBALL schema and row counts.
Run from DLT root: python scripts/inspect_football_schema.py
"""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = ROOT / "ai_driven_data.duckdb"


def main():
    db_path = os.getenv("DUCKDB_PATH", str(DEFAULT_DB))
    if not Path(db_path).exists():
        print(f"Database not found: {db_path}")
        print("Set DUCKDB_PATH to your local DuckDB file if it's elsewhere.")
        sys.exit(1)

    import duckdb
    conn = duckdb.connect(db_path, read_only=True)

    # List tables in FOOTBALL schema (DuckDB stores unquoted identifiers in info_schema)
    try:
        tables = conn.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema = 'FOOTBALL'
            ORDER BY table_name
        """).fetchall()
    except Exception as e:
        print(f"Error reading schema: {e}")
        conn.close()
        sys.exit(1)

    if not tables:
        print("No FOOTBALL schema or tables found in this database.")
        conn.close()
        return

    print("=" * 60)
    print("FOOTBALL schema (local DuckDB)")
    print("=" * 60)
    print(f"Database: {db_path}\n")

    for schema, table in tables:
        try:
            n = conn.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"').fetchone()[0]
            print(f"  {schema}.{table}: {n:,} rows")
        except Exception as e:
            print(f"  {schema}.{table}: error - {e}")

    print("=" * 60)
    conn.close()


if __name__ == "__main__":
    main()
