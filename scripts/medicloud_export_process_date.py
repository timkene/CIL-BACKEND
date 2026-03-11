#!/usr/bin/env python3
"""Connect to Medicloud, export dbo.claims where dateadded from 2025-03-01 to today to Excel."""
import os
import sys
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parent.parent
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

def main():
    import toml
    import pyodbc
    import pandas as pd

    secrets_path = ROOT / "secrets.toml"
    if not secrets_path.exists():
        print("secrets.toml not found")
        sys.exit(1)
    secrets = toml.load(secrets_path)
    creds = secrets.get("credentials")
    if not creds:
        print("secrets.toml missing [credentials]")
        sys.exit(1)

    drivers = [x for x in pyodbc.drivers()]
    preferred = ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server", "SQL Server Native Client 11.0", "SQL Server"]
    driver = next((d for d in preferred if d in drivers), None)
    if not driver:
        print("No SQL Server ODBC driver found")
        sys.exit(1)

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={creds['server']},{creds['port']};"
        f"DATABASE={creds['database']};"
        f"UID={creds['username']};"
        f"PWD={creds['password']};"
        "Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30;"
    )
    conn = pyodbc.connect(conn_str)

    start_date = "2025-03-01"
    end_date = datetime.now().strftime("%Y-%m-%d")
    sql = "SELECT * FROM dbo.claims WHERE [dateadded] >= ? AND [dateadded] <= ?"
    df = pd.read_sql(sql, conn, params=[start_date, end_date])
    conn.close()

    out_path = ROOT / "outputs" / "medicloud_process_date_export.xlsx"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(out_path, index=False, engine="openpyxl")
    print(f"Exported {len(df)} rows from claims to {out_path} (dateadded {start_date} to {end_date})")

if __name__ == "__main__":
    main()
