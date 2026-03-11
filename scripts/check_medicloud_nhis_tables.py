#!/usr/bin/env python3
"""
Connect to Medicloud (SQL Server) using credentials from secrets.toml (same as dlt_sources)
and list all tables whose name contains 'nhis'.

Run from DLT project root so secrets.toml is found:
  python scripts/check_medicloud_nhis_tables.py
"""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
os.chdir(ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

def main():
    try:
        import toml
    except ImportError:
        print("Install toml: pip install toml")
        sys.exit(1)
    try:
        import pyodbc
    except ImportError:
        print("Install pyodbc: pip install pyodbc")
        sys.exit(1)

    secrets_path = ROOT / "secrets.toml"
    if not secrets_path.exists():
        print(f"Secrets file not found: {secrets_path}")
        sys.exit(1)
    secrets = toml.load(secrets_path)
    creds = secrets.get("credentials")
    if not creds:
        print("secrets.toml must contain a [credentials] section (server, port, database, username, password)")
        sys.exit(1)

    drivers = [x for x in pyodbc.drivers()]
    preferred = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server",
    ]
    driver = None
    for d in preferred:
        if d in drivers:
            driver = d
            break
    if not driver:
        print("No compatible SQL Server ODBC driver found. Available:", drivers)
        sys.exit(1)

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={creds['server']},{creds['port']};"
        f"DATABASE={creds['database']};"
        f"UID={creds['username']};"
        f"PWD={creds['password']};"
        "Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30;"
    )
    print("Connecting to Medicloud...")
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Tables with 'nhis' in the name (any schema)
    cursor.execute("""
        SELECT s.name AS schema_name, t.name AS table_name
        FROM sys.tables t
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name LIKE '%nhis%'
        ORDER BY s.name, t.name
    """)
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        print("No tables found with 'nhis' in the name.")
        return
    print(f"Tables containing 'nhis' ({len(rows)}):")
    print("-" * 60)
    for schema, table in rows:
        print(f"  {schema}.{table}")

if __name__ == "__main__":
    main()
