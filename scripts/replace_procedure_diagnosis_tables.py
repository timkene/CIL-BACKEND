#!/usr/bin/env python3
"""
Replace PROCEDURE_DIAGNOSIS.PROCEDURE_DIAGNOSIS_COMP and PROCEDURE_DIAGNOSIS.PROCEDURE_MASTER
in local DuckDB with data from the given CSV files.

Run from DLT root:
  python scripts/replace_procedure_diagnosis_tables.py

Uses DUCKDB_PATH env or default ai_driven_data.duckdb in project root.
"""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CSV_COMP = ROOT / "NO_AUTH SETTINGS_PROCEDURE-DIAGNOSIS-COMP.csv"
CSV_PROCEDURE = ROOT / "NO_AUTH SETTINGS_PROCEDURE.csv"
DEFAULT_DB = ROOT / "ai_driven_data.duckdb"
SCHEMA = "PROCEDURE_DIAGNOSIS"
TABLE_COMP = "PROCEDURE_DIAGNOSIS_COMP"
TABLE_MASTER = "PROCEDURE_MASTER"


def main():
    db_path = os.getenv("DUCKDB_PATH", str(DEFAULT_DB))
    if not Path(db_path).exists():
        print(f"Database not found: {db_path}")
        sys.exit(1)
    if not CSV_COMP.exists():
        print(f"CSV not found: {CSV_COMP}")
        sys.exit(1)
    if not CSV_PROCEDURE.exists():
        print(f"CSV not found: {CSV_PROCEDURE}")
        sys.exit(1)

    import duckdb
    conn = duckdb.connect(db_path, read_only=False)

    # Ensure schema exists
    conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"')

    # 1) Replace PROCEDURE_DIAGNOSIS_COMP
    print(f"Loading {CSV_COMP.name} into {SCHEMA}.{TABLE_COMP}...")
    conn.execute(f'DROP TABLE IF EXISTS "{SCHEMA}"."{TABLE_COMP}"')
    conn.execute(f"""
        CREATE TABLE "{SCHEMA}"."{TABLE_COMP}" AS
        SELECT * FROM read_csv_auto(?, header=true, ignore_errors=false)
    """, [str(CSV_COMP)])
    n_comp = conn.execute(f'SELECT COUNT(*) FROM "{SCHEMA}"."{TABLE_COMP}"').fetchone()[0]
    print(f"  -> {n_comp} rows in {SCHEMA}.{TABLE_COMP}")

    # 2) Replace PROCEDURE_MASTER (includes Quantity_limit)
    print(f"Loading {CSV_PROCEDURE.name} into {SCHEMA}.{TABLE_MASTER}...")
    conn.execute(f'DROP TABLE IF EXISTS "{SCHEMA}"."{TABLE_MASTER}"')
    conn.execute(f"""
        CREATE TABLE "{SCHEMA}"."{TABLE_MASTER}" AS
        SELECT * FROM read_csv_auto(?, header=true, ignore_errors=false)
    """, [str(CSV_PROCEDURE)])
    n_master = conn.execute(f'SELECT COUNT(*) FROM "{SCHEMA}"."{TABLE_MASTER}"').fetchone()[0]
    print(f"  -> {n_master} rows in {SCHEMA}.{TABLE_MASTER}")

    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
