#!/usr/bin/env python3
"""
Connect to Medicloud database using credentials from dlt_sources (secrets.toml)
and list all tables whose name contains 'nhia' (case insensitive).
Run from DLT root: python scripts/list_medicloud_nhia_tables.py
"""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

def load_secrets():
    import toml
    secrets_path = ROOT / "secrets.toml"
    if not secrets_path.exists():
        raise FileNotFoundError(f"Secrets file not found: {secrets_path}")
    return toml.load(secrets_path)

def get_sql_driver():
    import pyodbc
    drivers = [x for x in pyodbc.drivers()]
    preferred = [
        'ODBC Driver 18 for SQL Server',
        'ODBC Driver 17 for SQL Server',
        'SQL Server Native Client 11.0',
        'SQL Server'
    ]
    for d in preferred:
        if d in drivers:
            return d
    raise RuntimeError("No compatible SQL Server driver found.")

def create_medicloud_connection():
    import pyodbc
    secrets = load_secrets()
    creds = secrets['credentials']
    driver = get_sql_driver()
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={creds['server']},{creds['port']};"
        f"DATABASE={creds['database']};"
        f"UID={creds['username']};"
        f"PWD={creds['password']};"
        f"Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)

def main():
    conn = create_medicloud_connection()
    cursor = conn.cursor()
    # SQL Server: list tables where name contains 'nhia' (case insensitive by default)
    cursor.execute("""
        SELECT s.name AS schema_name, t.name AS table_name
        FROM sys.tables t
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE LOWER(t.name) LIKE '%nhia%'
        ORDER BY s.name, t.name
    """)
    rows = cursor.fetchall()
    conn.close()
    if not rows:
        print("No tables found with 'nhia' in the name.")
        return
    print("Tables in Medicloud with 'nhia' in name (case insensitive):")
    print("-" * 60)
    for schema, table in rows:
        print(f"  {schema}.{table}")
    print("-" * 60)
    print(f"Total: {len(rows)} table(s)")

if __name__ == "__main__":
    main()
