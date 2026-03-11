#!/usr/bin/env python3
"""
Push NHIA and PROCEDURE_DIAGNOSIS schemas from local DuckDB to MotherDuck.
Views are materialized as tables (CREATE TABLE ... AS SELECT * FROM view).
Run from DLT root with MOTHERDUCK_TOKEN set.
"""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = ROOT / "ai_driven_data.duckdb"
MOTHERDUCK_DB = "ai_driven_data"
SCHEMAS = ["NHIA", "PROCEDURE_DIAGNOSIS"]


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
    md_conn = duckdb.connect(f"md:?motherduck_token={token}")
    md_conn.execute(f"CREATE DATABASE IF NOT EXISTS {MOTHERDUCK_DB}")
    md_conn.execute(f"USE {MOTHERDUCK_DB}")
    md_conn.execute(f"ATTACH '{db_path}' AS local_db (READ_ONLY)")

    print("=" * 60)
    print("Pushing NHIA and PROCEDURE_DIAGNOSIS to MotherDuck")
    print("=" * 60)
    print(f"Local DB: {db_path}\n")

    total_ok = 0
    total_all = 0
    for schema in SCHEMAS:
        items = local_conn.execute(f"""
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = '{schema}'
            ORDER BY table_name
        """).fetchall()
        if not items:
            print(f"  [{schema}] — no tables/views in local, skip\n")
            continue
        md_conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        print(f"  [{schema}] — {len(items)} objects")
        for table, tbl_type in items:
            total_all += 1
            try:
                md_conn.execute(f'DROP TABLE IF EXISTS "{schema}"."{table}"')
                md_conn.execute(
                    f'CREATE TABLE "{schema}"."{table}" AS SELECT * FROM local_db."{schema}"."{table}"'
                )
                n = md_conn.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"').fetchone()[0]
                kind = " (view→table)" if tbl_type == "VIEW" else ""
                print(f"    {table}: {n:,} rows{kind}")
                total_ok += 1
            except Exception as e:
                print(f"    {table}: FAILED - {e}")
        print()

    md_conn.execute("DETACH local_db")
    local_conn.close()
    md_conn.close()

    print("=" * 60)
    print(f"Done. {total_ok}/{total_all} tables/views pushed.")
    print("=" * 60)


if __name__ == "__main__":
    main()
