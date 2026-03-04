#!/usr/bin/env python3
"""
Export claims (last 3 months by datesubmitted) whose procedure codes come from
PROCEDURE_DIAGNOSIS.PROCEDURE_DIAGNOSIS_COMP, excluding CONS021 and CONS022.
Output: Excel with columns procedure code, procedure name, diagnosis code, diagnosis name.
"""
import os
import sys
from datetime import date, timedelta

import duckdb
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(ROOT, "ai_driven_data.duckdb")
OUTPUT_EXCEL = os.path.join(ROOT, "claims_procedure_diagnosis_comp_last_3months.xlsx")

# Exclude these procedure codes
EXCLUDE_CODES = ("CONS021", "CONS022")


def main():
    if not os.path.isfile(DB_PATH):
        print(f"Database not found: {DB_PATH}")
        sys.exit(1)

    # Use a copy to avoid lock if main DB is in use
    db_copy = os.path.join(ROOT, "ai_driven_data_export_copy.duckdb")
    try:
        import shutil
        shutil.copy2(DB_PATH, db_copy)
    except Exception as e:
        print(f"Could not copy DB: {e}. Using original.")
        db_copy = DB_PATH

    conn = duckdb.connect(db_copy, read_only=True)

    # Last 3 months by datesubmitted
    end_date = date.today()
    start_date = end_date - timedelta(days=90)

    # 1) Unique procedure codes from PROCEDURE_DIAGNOSIS.PROCEDURE_DIAGNOSIS_COMP (exclude CONS021, CONS022)
    comp_codes_df = conn.execute("""
        SELECT DISTINCT TRIM(UPPER(procedure_code)) AS procedure_code
        FROM "PROCEDURE_DIAGNOSIS"."PROCEDURE_DIAGNOSIS_COMP"
        WHERE TRIM(UPPER(procedure_code)) NOT IN (?, ?)
    """, [c.upper() for c in EXCLUDE_CODES]).fetchdf()

    if comp_codes_df.empty:
        print("No procedure codes found in PROCEDURE_DIAGNOSIS.PROCEDURE_DIAGNOSIS_COMP (after exclusions).")
        conn.close()
        if db_copy != DB_PATH and os.path.isfile(db_copy):
            os.remove(db_copy)
        sys.exit(1)

    comp_codes_list = comp_codes_df["procedure_code"].astype(str).tolist()
    placeholders = ", ".join(["?" for _ in comp_codes_list])

    # 2) Claims from last 3 months with those procedure codes; join procedure name and diagnosis name
    query = f"""
        SELECT
            c.code AS procedure_code,
            COALESCE(pd.proceduredesc, '') AS procedure_name,
            COALESCE(c.diagnosiscode, '') AS diagnosis_code,
            COALESCE(d.diagnosisdesc, '') AS diagnosis_name
        FROM "AI DRIVEN DATA"."CLAIMS DATA" c
        LEFT JOIN "AI DRIVEN DATA"."PROCEDURE DATA" pd
            ON TRIM(UPPER(c.code)) = TRIM(UPPER(pd.procedurecode))
        LEFT JOIN "AI DRIVEN DATA"."DIAGNOSIS" d
            ON TRIM(UPPER(c.diagnosiscode)) = TRIM(UPPER(d.diagnosiscode))
        WHERE c.datesubmitted >= ?
          AND c.datesubmitted <= ?
          AND TRIM(UPPER(c.code)) IN ({placeholders})
    """
    params = [start_date.isoformat(), end_date.isoformat()] + comp_codes_list
    df = conn.execute(query, params).fetchdf()

    conn.close()
    if db_copy != DB_PATH and os.path.isfile(db_copy):
        os.remove(db_copy)

    if df.empty:
        print(f"No claims found for last 3 months ({start_date} to {end_date}) with procedure codes from PROCEDURE_DIAGNOSIS_COMP (excl. CONS021, CONS022).")
        df = pd.DataFrame(columns=["procedure_code", "procedure_name", "diagnosis_code", "diagnosis_name"])

    # Normalize column names for output
    df = df.rename(columns={
        "procedure_code": "Procedure Code",
        "procedure_name": "Procedure Name",
        "diagnosis_code": "Diagnosis Code",
        "diagnosis_name": "Diagnosis Name",
    })

    df.to_excel(OUTPUT_EXCEL, index=False, sheet_name="Claims Proc-Diag Comp")
    print(f"Written {len(df)} rows to {OUTPUT_EXCEL}")
    print(f"Date range: {start_date} to {end_date} (datesubmitted)")
    print(f"Procedure codes: from PROCEDURE_DIAGNOSIS.PROCEDURE_DIAGNOSIS_COMP, excluding {EXCLUDE_CODES}")


if __name__ == "__main__":
    main()
