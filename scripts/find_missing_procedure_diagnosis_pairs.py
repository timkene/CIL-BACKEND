#!/usr/bin/env python3
"""
Find procedure–diagnosis mappings seen in recent claims that are NOT present
in PROCEDURE_DIAGNOSIS.PROCEDURE_DIAGNOSIS_COMP.

Input:
  - DuckDB: ai_driven_data.duckdb
  - Excel:  claims_procedure_diagnosis_comp_last_3months.xlsx

Output:
  - Excel:  claims_procedure_diagnosis_missing_in_schema.xlsx
    (same columns as input, filtered to only pairs missing in the schema)
"""
import os
import sys
import shutil

import duckdb
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(ROOT, "ai_driven_data.duckdb")
INPUT_XLSX = os.path.join(ROOT, "claims_procedure_diagnosis_comp_last_3months.xlsx")
OUTPUT_XLSX = os.path.join(ROOT, "claims_procedure_diagnosis_missing_in_schema.xlsx")


def main():
  # Basic existence checks
  if not os.path.isfile(DB_PATH):
    print(f"Database not found: {DB_PATH}")
    sys.exit(1)
  if not os.path.isfile(INPUT_XLSX):
    print(f"Input Excel not found: {INPUT_XLSX}")
    sys.exit(1)

  # Use a copy of the DB to avoid locking issues
  db_copy = os.path.join(ROOT, "ai_driven_data_pd_copy.duckdb")
  try:
    shutil.copy2(DB_PATH, db_copy)
  except Exception as e:
    print(f"Could not copy DB ({e}), using original.")
    db_copy = DB_PATH

  conn = duckdb.connect(db_copy, read_only=True)

  try:
    # 1) Load all procedure–diagnosis pairs from PROCEDURE_DIAGNOSIS_COMP
    schema_df = conn.execute(
      """
      SELECT DISTINCT
        TRIM(UPPER(procedure_code))  AS procedure_code,
        TRIM(UPPER(diagnosis_code))  AS diagnosis_code
      FROM "PROCEDURE_DIAGNOSIS"."PROCEDURE_DIAGNOSIS_COMP"
      """
    ).fetchdf()

    if schema_df.empty:
      print("No rows found in PROCEDURE_DIAGNOSIS.PROCEDURE_DIAGNOSIS_COMP.")
      return

    schema_pairs = {
      (row["procedure_code"], row["diagnosis_code"])
      for _, row in schema_df.iterrows()
      if pd.notna(row["procedure_code"]) and pd.notna(row["diagnosis_code"])
    }

    # 2) Load the recent claims Excel
    claims_df = pd.read_excel(INPUT_XLSX)

    # Normalise column names if needed
    cols = {c.lower().strip(): c for c in claims_df.columns}
    proc_col = cols.get("procedure code".lower(), None)
    diag_col = cols.get("diagnosis code".lower(), None)

    if proc_col is None or diag_col is None:
      print(f"Could not find expected 'Procedure Code' / 'Diagnosis Code' columns in {INPUT_XLSX}")
      print(f"Columns found: {list(claims_df.columns)}")
      return

    # Normalise codes
    claims_df["__proc_norm"] = (
      claims_df[proc_col].astype(str).str.strip().str.upper()
    )
    claims_df["__diag_norm"] = (
      claims_df[diag_col].astype(str).str.strip().str.upper()
    )

    # Drop rows with missing diagnosis or procedure code (optional)
    claims_df = claims_df[
      (claims_df["__proc_norm"] != "") & (claims_df["__diag_norm"] != "")
    ].copy()

    # 3) Keep only pairs NOT present in the schema
    mask_missing = [
      (p, d) not in schema_pairs
      for p, d in zip(claims_df["__proc_norm"], claims_df["__diag_norm"])
    ]
    missing_df = claims_df[mask_missing].copy()

    # Drop helper columns
    missing_df = missing_df.drop(columns=["__proc_norm", "__diag_norm"])

    # 4) Write output Excel
    missing_df.to_excel(OUTPUT_XLSX, index=False)
    print(f"Total input rows:      {len(claims_df)}")
    print(f"Missing schema pairs:  {len(missing_df)}")
    print(f"Output written to:     {OUTPUT_XLSX}")

  finally:
    conn.close()
    if db_copy != DB_PATH and os.path.isfile(db_copy):
      try:
        os.remove(db_copy)
      except Exception:
        pass


if __name__ == "__main__":
  main()

