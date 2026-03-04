"""
Create FOOTBALL schema and players table in DuckDB for the Eko React app.
Run once: python scripts/init_football_schema.py
"""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "ai_driven_data.duckdb"


def main():
    if not DB_PATH.exists():
        print(f"Database not found: {DB_PATH}")
        sys.exit(1)

    import duckdb
    conn = duckdb.connect(str(DB_PATH), read_only=False)

    conn.execute('CREATE SCHEMA IF NOT EXISTS FOOTBALL')

    conn.execute("""
        CREATE TABLE IF NOT EXISTS FOOTBALL.players (
            id INTEGER PRIMARY KEY,
            first_name VARCHAR NOT NULL,
            surname VARCHAR NOT NULL,
            baller_name VARCHAR NOT NULL UNIQUE,
            jersey_number INTEGER NOT NULL CHECK (jersey_number >= 1 AND jersey_number <= 100),
            email VARCHAR NOT NULL,
            whatsapp_phone VARCHAR NOT NULL,
            status VARCHAR NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected')),
            password_hash VARCHAR,
            year_registered INTEGER,
            created_at TIMESTAMP DEFAULT current_timestamp,
            approved_at TIMESTAMP
        )
    """)

    print("FOOTBALL schema and FOOTBALL.players table ready.")
    conn.close()


if __name__ == "__main__":
    main()
