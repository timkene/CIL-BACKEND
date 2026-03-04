#!/usr/bin/env python3
"""
Replace PROCEDURE_DIAGNOSIS.PROCEDURE_MASTER with data from procedure_master.csv.

Usage (from project root):
    PYTHONPATH=. python scripts/load_procedure_master_from_csv.py

Uses: procedure_master.csv in project root
DB: ai_driven_data.duckdb
"""
import os
import sys

import pandas as pd
import duckdb

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(ROOT, "ai_driven_data.duckdb")
CSV_PATH = os.path.join(ROOT, "procedure_master.csv")


def main():
    if not os.path.isfile(CSV_PATH):
        print(f"CSV not found: {CSV_PATH}")
        sys.exit(1)
    if not os.path.isfile(DB_PATH):
        print(f"Database not found: {DB_PATH}")
        sys.exit(1)

    df = pd.read_csv(CSV_PATH)
    df.columns = [c.strip() for c in df.columns]

    required = ["procedure_code", "procedure_name", "procedure_class", "typical_diagnoses", "typical_symptoms"]
    for col in required:
        if col not in df.columns:
            print(f"Missing column: {col}. Found: {list(df.columns)}")
            sys.exit(1)

    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].fillna("").astype(str)

    conn = duckdb.connect(DB_PATH, read_only=False)
    conn.execute('CREATE SCHEMA IF NOT EXISTS "PROCEDURE_DIAGNOSIS"')
    conn.execute('DROP TABLE IF EXISTS "PROCEDURE_DIAGNOSIS"."PROCEDURE_MASTER"')

    conn.execute("""
        CREATE TABLE "PROCEDURE_DIAGNOSIS"."PROCEDURE_MASTER" (
            procedure_code VARCHAR,
            procedure_name VARCHAR,
            procedure_class VARCHAR,
            typical_diagnoses VARCHAR,
            typical_symptoms VARCHAR,
            age_range VARCHAR,
            gender_applicable VARCHAR,
            formulation VARCHAR,
            frequency_category VARCHAR,
            clinical_notes VARCHAR
        )
    """)

    conn.register("_pm_df", df)
    conn.execute("""
        INSERT INTO "PROCEDURE_DIAGNOSIS"."PROCEDURE_MASTER"
        (procedure_code, procedure_name, procedure_class, typical_diagnoses, typical_symptoms,
         age_range, gender_applicable, formulation, frequency_category, clinical_notes)
        SELECT
            procedure_code, procedure_name, procedure_class, typical_diagnoses, typical_symptoms,
            COALESCE(age_range, ''),
            COALESCE(gender_applicable, ''),
            COALESCE(formulation, ''),
            COALESCE(frequency_category, ''),
            COALESCE(clinical_notes, '')
        FROM _pm_df
    """)
    conn.unregister("_pm_df")

    n = conn.execute('SELECT COUNT(*) FROM "PROCEDURE_DIAGNOSIS"."PROCEDURE_MASTER"').fetchone()[0]
    conn.close()
    print(f"Done. PROCEDURE_DIAGNOSIS.PROCEDURE_MASTER replaced with {n} rows from procedure_master.csv")


if __name__ == "__main__":
    main()
