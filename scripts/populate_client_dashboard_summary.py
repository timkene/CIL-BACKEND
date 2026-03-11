#!/usr/bin/env python3
"""
Populate CLIENT_DASHBOARD_SUMMARY table for the client dashboard (nightly batch).
Run with the same env as the API (USE_LOCAL_DB, MOTHERDUCK_TOKEN) so the table
is written to the DB the API reads from.

  USE_LOCAL_DB=false MOTHERDUCK_TOKEN=xxx python scripts/populate_client_dashboard_summary.py
"""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Load .env if present
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

def main():
    from api.routes.clients import populate_client_dashboard_summary_table
    success, rows, err = populate_client_dashboard_summary_table()
    if success:
        print(f"OK: CLIENT_DASHBOARD_SUMMARY populated with {rows} rows")
        return 0
    print(f"ERROR: {err}", file=sys.stderr)
    return 1

if __name__ == "__main__":
    sys.exit(main())
