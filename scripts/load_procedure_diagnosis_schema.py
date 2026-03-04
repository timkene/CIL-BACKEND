#!/usr/bin/env python3
"""
Replace PROCEDURE_DIAGNOSIS.DIAGNOSIS_MASTER and PROCEDURE_MASTER with new CSV data,
and create PROCEDURE_DIAGNOSIS.PROCEDURE_DIAGNOSIS_COMP.

CSVs (in project root):
  - NO_AUTH SETTINGS_DIAGNOSIS .csv  -> DIAGNOSIS_MASTER
  - NO_AUTH SETTINGS_PROCEDURE.csv   -> PROCEDURE_MASTER
  - NO_AUTH SETTINGS_PROCEDURE-DIAGNOSIS-COMP.csv -> PROCEDURE_DIAGNOSIS_COMP

DB: ai_driven_data.duckdb
"""
import os
import sys

import duckdb

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(ROOT, "ai_driven_data.duckdb")
DIAG_CSV = os.path.join(ROOT, "NO_AUTH SETTINGS_DIAGNOSIS .csv")
PROC_CSV = os.path.join(ROOT, "NO_AUTH SETTINGS_PROCEDURE.csv")
COMP_CSV = os.path.join(ROOT, "NO_AUTH SETTINGS_PROCEDURE-DIAGNOSIS-COMP.csv")


def main():
    for path in (DIAG_CSV, PROC_CSV, COMP_CSV):
        if not os.path.isfile(path):
            print(f"CSV not found: {path}")
            sys.exit(1)
    if not os.path.isfile(DB_PATH):
        print(f"Database not found: {DB_PATH}")
        sys.exit(1)

    # Use absolute paths for DuckDB read_csv
    diag_csv_abs = os.path.abspath(DIAG_CSV)
    proc_csv_abs = os.path.abspath(PROC_CSV)
    comp_csv_abs = os.path.abspath(COMP_CSV)

    conn = duckdb.connect(DB_PATH, read_only=False)
    conn.execute('CREATE SCHEMA IF NOT EXISTS "PROCEDURE_DIAGNOSIS"')

    # --- DIAGNOSIS_MASTER (CSV: diagnosis_code, diagnosis_name, diagnosis_class, age_range, gender_applicable, notes; add implied_symptoms) ---
    conn.execute('DROP TABLE IF EXISTS "PROCEDURE_DIAGNOSIS"."DIAGNOSIS_MASTER"')
    conn.execute("""
        CREATE TABLE "PROCEDURE_DIAGNOSIS"."DIAGNOSIS_MASTER" AS
        SELECT diagnosis_code, diagnosis_name, diagnosis_class, age_range, gender_applicable, notes,
               '' AS implied_symptoms
        FROM read_csv_auto(?, header=true, ignore_errors=true)
    """, [diag_csv_abs])
    n_diag = conn.execute('SELECT COUNT(*) FROM "PROCEDURE_DIAGNOSIS"."DIAGNOSIS_MASTER"').fetchone()[0]
    print(f"DIAGNOSIS_MASTER: {n_diag} rows")

    # --- PROCEDURE_MASTER (CSV has no typical_diagnoses/typical_symptoms; add as empty) ---
    conn.execute('DROP TABLE IF EXISTS "PROCEDURE_DIAGNOSIS"."PROCEDURE_MASTER"')
    conn.execute("""
        CREATE TABLE "PROCEDURE_DIAGNOSIS"."PROCEDURE_MASTER" AS
        SELECT procedure_code, procedure_name, procedure_class, age_range, gender_applicable,
               formulation, frequency_category, clinical_notes,
               '' AS typical_diagnoses, '' AS typical_symptoms
        FROM read_csv_auto(?, header=true, ignore_errors=true)
    """, [proc_csv_abs])
    n_proc = conn.execute('SELECT COUNT(*) FROM "PROCEDURE_DIAGNOSIS"."PROCEDURE_MASTER"').fetchone()[0]
    print(f"PROCEDURE_MASTER: {n_proc} rows")

    # --- PROCEDURE_DIAGNOSIS_COMP ---
    conn.execute('DROP TABLE IF EXISTS "PROCEDURE_DIAGNOSIS"."PROCEDURE_DIAGNOSIS_COMP"')
    conn.execute("""
        CREATE TABLE "PROCEDURE_DIAGNOSIS"."PROCEDURE_DIAGNOSIS_COMP" AS
        SELECT * FROM read_csv_auto(?, header=true, ignore_errors=true)
    """, [comp_csv_abs])
    n_comp = conn.execute('SELECT COUNT(*) FROM "PROCEDURE_DIAGNOSIS"."PROCEDURE_DIAGNOSIS_COMP"').fetchone()[0]
    print(f"PROCEDURE_DIAGNOSIS_COMP: {n_comp} rows")

    # --- First 3 rows of each table ---
    def show_first_3(name, query):
        print(f"\n--- First 3 rows: {name} ---")
        result = conn.execute(query)
        rows = result.fetchall()
        col_names = [d[0] for d in result.description]
        print("  " + " | ".join(col_names))
        for row in rows:
            print("  " + " | ".join(str(x) for x in row))

    show_first_3("DIAGNOSIS_MASTER", 'SELECT * FROM "PROCEDURE_DIAGNOSIS"."DIAGNOSIS_MASTER" LIMIT 3')
    show_first_3("PROCEDURE_MASTER", 'SELECT * FROM "PROCEDURE_DIAGNOSIS"."PROCEDURE_MASTER" LIMIT 3')
    show_first_3("PROCEDURE_DIAGNOSIS_COMP", 'SELECT * FROM "PROCEDURE_DIAGNOSIS"."PROCEDURE_DIAGNOSIS_COMP" LIMIT 3')

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
