"""
seed_staff_permissions.py
==========================
Reads the two permission CSVs and seeds Supabase:
  - staff table  (from permission_EMAILS AND NAME.csv)
  - department_permissions table  (from permission_PERMISSION.csv)

Run once:
    python scripts/seed_staff_permissions.py

Supabase setup — run this SQL in Supabase SQL Editor first:

    CREATE TABLE staff (
        id              BIGSERIAL PRIMARY KEY,
        first_name      TEXT NOT NULL,
        last_name       TEXT NOT NULL DEFAULT '',
        email           TEXT NOT NULL UNIQUE,
        department      TEXT NOT NULL,
        password        TEXT NOT NULL,
        status          TEXT NOT NULL DEFAULT 'ACTIVE',
        session_version INT  NOT NULL DEFAULT 1,
        created_at      TIMESTAMPTZ DEFAULT NOW(),
        updated_at      TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE department_permissions (
        id          BIGSERIAL PRIMARY KEY,
        department  TEXT NOT NULL UNIQUE,
        modules     TEXT[] NOT NULL
    );

    ALTER TABLE staff ENABLE ROW LEVEL SECURITY;
    ALTER TABLE department_permissions ENABLE ROW LEVEL SECURITY;
    CREATE POLICY "Allow all" ON staff FOR ALL USING (true) WITH CHECK (true);
    CREATE POLICY "Allow all" ON department_permissions FOR ALL USING (true) WITH CHECK (true);
"""

import os
import csv
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

STAFF_CSV = ROOT / "permission_EMAILS AND NAME.csv"
PERMS_CSV = ROOT / "permission_PERMISSION.csv"


def seed_departments():
    print("\n[1/2] Seeding department_permissions...")
    rows = []
    with open(PERMS_CSV, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            dept = row["DEPARTMENTS"].strip()
            mods_raw = row["MODULES"].strip()
            if not dept:
                continue
            if mods_raw.upper() == "ALL":
                modules = ["ALL"]
            else:
                modules = [m.strip() for m in mods_raw.split(",") if m.strip()]
            rows.append({"department": dept, "modules": modules})

    for r in rows:
        sb.table("department_permissions").upsert(r, on_conflict="department").execute()
        print(f"  {r['department']:30s} → {r['modules']}")

    print(f"  Done: {len(rows)} departments")


def seed_staff():
    print("\n[2/2] Seeding staff...")
    rows = []
    with open(STAFF_CSV, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            email = row["Email address"].strip()
            if not email:
                continue
            status = row["Status"].strip().upper()
            if status not in ("ACTIVE", "INACTIVE"):
                status = "ACTIVE"
            rows.append({
                "first_name":  row["First Name"].strip(),
                "last_name":   row["Last Name"].strip(),
                "email":       email,
                "department":  row["Department"].strip().upper(),
                "password":    row["Password"].strip(),
                "status":      status,
                "session_version": 1,
            })

    for r in rows:
        sb.table("staff").upsert(r, on_conflict="email").execute()

    print(f"  Done: {len(rows)} staff members")


def main():
    print("=" * 50)
    print("  Staff & Permissions Seed")
    print("=" * 50)
    seed_departments()
    seed_staff()
    print("\nAll done.\n")


if __name__ == "__main__":
    main()
