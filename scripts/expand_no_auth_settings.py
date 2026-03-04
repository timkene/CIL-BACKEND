#!/usr/bin/env python3
"""
Expand NO_AUTH SETTINGS_add.csv so each diagnosis code is one row, with diagnosis_name.

Input CSV columns: procedure_code, procedure_name, typical_diagnoses (comma-separated codes)
Output: procedure_code, procedure_name, diagnosis_code, diagnosis_name (one row per diagnosis)

Optionally resolves diagnosis_name from PROCEDURE_DIAGNOSIS.DIAGNOSIS_MASTER in ai_driven_data.duckdb.
"""

import csv
import os
import sys

# Project root
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DB_PATH = os.path.join(PROJECT_ROOT, "ai_driven_data.duckdb")


def get_diagnosis_name_lookup():
    """Build dict of diagnosis_code -> diagnosis_name from DuckDB if available."""
    lookup = {}
    if not os.path.isfile(DB_PATH):
        return lookup
    try:
        import duckdb
        conn = duckdb.connect(DB_PATH, read_only=True)
        try:
            rows = conn.execute("""
                SELECT diagnosis_code, diagnosis_name
                FROM "PROCEDURE_DIAGNOSIS"."DIAGNOSIS_MASTER"
            """).fetchall()
            for code, name in rows:
                if code:
                    lookup[str(code).strip().upper()] = (name or code).strip()
        finally:
            conn.close()
    except Exception:
        pass
    return lookup


def main():
    input_path = os.path.join(PROJECT_ROOT, "NO_AUTH SETTINGS_add.csv")
    if len(sys.argv) > 1:
        input_path = sys.argv[1]
    if not os.path.isfile(input_path):
        print(f"Error: File not found: {input_path}")
        sys.exit(1)

    name_lookup = get_diagnosis_name_lookup()
    if name_lookup:
        print(f"Resolved {len(name_lookup)} diagnosis names from DB.")
    else:
        print("No DB lookup; diagnosis_name will be the code.")

    out_rows = []
    with open(input_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            proc_code = (row.get("procedure_code") or "").strip()
            proc_name = (row.get("procedure_name") or "").strip()
            typical = (row.get("typical_diagnoses") or "").strip()
            if not typical:
                out_rows.append({
                    "procedure_code": proc_code,
                    "procedure_name": proc_name,
                    "diagnosis_code": "",
                    "diagnosis_name": "",
                })
                continue
            codes = [c.strip() for c in typical.split(",") if c.strip()]
            for code in codes:
                key = code.upper()
                diag_name = name_lookup.get(key, code)
                out_rows.append({
                    "procedure_code": proc_code,
                    "procedure_name": proc_name,
                    "diagnosis_code": code,
                    "diagnosis_name": diag_name,
                })

    out_path = input_path.replace(".csv", "_expanded.csv")
    if out_path == input_path:
        out_path = input_path + "_expanded.csv"
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["procedure_code", "procedure_name", "diagnosis_code", "diagnosis_name"])
        w.writeheader()
        w.writerows(out_rows)
    print(f"Wrote {len(out_rows)} rows to {out_path}")


if __name__ == "__main__":
    main()
