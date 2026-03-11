#!/usr/bin/env python3
"""
Daily Provider Band Mapping Scan
==================================
Scans all active members to detect:
  1. Inappropriate Provider Mapping  — member is mapped to a provider whose band
     is higher than their plan price allows.
  2. Inappropriate Provider Visits   — member visited a higher-band provider more
     than 2 times in the last 3 months.

Band price tiers (individual price takes priority; fall back to family price):
  Individual 0–81,779        / Family 0–409,399        → Band D only
  Individual 81,800–117,244  / Family 409,400–586,224  → Band C–D
  Individual 117,245–344,999 / Family 586,225–1,724,999→ Band B–D
  Individual 345,000–678,500 / Family 1,725,000–3,392,500 → Band A–D
  Special/None band          → flagged for ALL price tiers

Supabase tables required (run once in Supabase SQL Editor):
─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS enrollee_provider_mapping_flags (
    id                    BIGSERIAL PRIMARY KEY,
    enrollee_id           TEXT        NOT NULL,
    full_name             TEXT,
    plan_name             TEXT,
    plan_code             TEXT,
    individual_price      NUMERIC(18,2),
    family_price          NUMERIC(18,2),
    allowed_bands         TEXT,
    mapped_provider_id    TEXT,
    mapped_provider_name  TEXT,
    mapped_provider_band  TEXT,
    flag_reason           TEXT,
    scanned_at            TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (enrollee_id)
);

CREATE TABLE IF NOT EXISTS enrollee_provider_visit_flags (
    id                       BIGSERIAL PRIMARY KEY,
    enrollee_id              TEXT        NOT NULL,
    full_name                TEXT,
    plan_name                TEXT,
    individual_price         NUMERIC(18,2),
    family_price             NUMERIC(18,2),
    allowed_bands            TEXT,
    visit_count_higher_band  INTEGER,
    last_visit_date          DATE,
    higher_band_providers    TEXT,   -- JSON array of provider names
    scanned_at               TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (enrollee_id)
);
─────────────────────────────────────────────────────────────

Usage:
    MOTHERDUCK_TOKEN=xxx SUPABASE_URL=xxx SUPABASE_KEY=xxx \\
        python scripts/daily_provider_band_mapping.py
"""

import os
import sys
import json
import traceback
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

import duckdb
from supabase import create_client, Client

# ── Config ────────────────────────────────────────────────────────────────────
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN", "")
MOTHERDUCK_DSN   = f"md:ai_driven_data?motherduck_token={MOTHERDUCK_TOKEN}"
SUPABASE_URL     = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY     = os.getenv("SUPABASE_KEY", "")
SCHEMA           = "AI DRIVEN DATA"

# ── Band rules ────────────────────────────────────────────────────────────────
# Rank: higher number = higher (more expensive) band
BAND_RANK: dict[str, int] = {"D": 1, "C": 2, "B": 3, "A": 4}


def _normalise_band(raw: str) -> str:
    """
    Normalise raw band string to a single letter.
    Handles 'Band A', 'BAND A', 'A', 'band_a', etc.
    Returns '' if unrecognisable (treated as special/unknown).
    """
    b = (raw or "").strip().upper()
    if b.startswith("BAND "):
        b = b[5:].strip()
    if b.startswith("BAND_"):
        b = b[5:].strip()
    # Keep only the first letter if it's a known band letter
    if b and b[0] in BAND_RANK:
        return b[0]
    return b  # returns 'SPECIAL', 'NONE', '' etc.


def get_allowed_bands(individual_price, family_price) -> set[str] | None:
    """
    Return the set of allowed band letters for a plan.
    Returns None when no price is available (member skipped).
    Individual price takes priority; family price used only when individual is None or 0
    (0 means price not set, not a free plan).
    """
    if individual_price is not None and float(individual_price) > 0:
        p = float(individual_price)
        if p <= 81_779:
            return {"D"}
        elif p <= 117_244:
            return {"C", "D"}
        elif p <= 344_999:
            return {"B", "C", "D"}
        elif p <= 678_500:
            return {"A", "B", "C", "D"}
        else:
            return {"A", "B", "C", "D"}

    if family_price is not None:
        p = float(family_price)
        if p <= 409_399:
            return {"D"}
        elif p <= 586_224:
            return {"C", "D"}
        elif p <= 1_724_999:
            return {"B", "C", "D"}
        elif p <= 3_392_500:
            return {"A", "B", "C", "D"}
        else:
            return {"A", "B", "C", "D"}

    return None


def is_higher_band(band_letter: str, allowed: set[str]) -> bool:
    """
    Returns True if band_letter is higher than every band in allowed,
    OR if it is a special/unknown band (SPECIAL, NONE, etc.).
    """
    if not band_letter or band_letter not in BAND_RANK:
        # Unknown / special band → always flag
        return True
    max_allowed_rank = max(BAND_RANK[b] for b in allowed if b in BAND_RANK)
    return BAND_RANK[band_letter] > max_allowed_rank


# ── SQL ───────────────────────────────────────────────────────────────────────
MEMBER_MAPPING_SQL = f"""
SELECT
    m.enrollee_id,
    mem.firstname || ' ' || mem.lastname   AS fullname,
    mp.providerid                          AS mapped_providerid,
    p.providername                         AS mapped_provider_name,
    p.bands                                AS mapped_provider_band,
    pl.planname,
    pl.plancode,
    gp.individualprice,
    gp.familyprice,
    CAST(gc.enddate AS DATE)               AS contract_end_date
FROM "{SCHEMA}"."MEMBERS" m
JOIN "{SCHEMA}"."MEMBER"          mem ON TRY_CAST(m.memberid AS BIGINT) = mem.memberid
JOIN "{SCHEMA}"."MEMBER_PROVIDER" mp  ON CAST(mp.memberid AS VARCHAR) = m.memberid
                                      AND mp.iscurrent = TRUE
JOIN "{SCHEMA}"."MEMBER_PLANS"    mpl ON mp.memberid = mpl.memberid
                                      AND mpl.iscurrent = TRUE
JOIN "{SCHEMA}"."PLANS"           pl  ON CAST(mpl.planid AS BIGINT) = pl.planid
LEFT JOIN "{SCHEMA}"."GROUP_PLANS"   gp ON pl.planid = gp.planid
                                       AND gp.iscurrent = TRUE
                                       AND gp.groupid = mem.groupid
LEFT JOIN "{SCHEMA}"."PROVIDERS"     p  ON TRY_CAST(mp.providerid AS BIGINT) = TRY_CAST(p.protariffid AS BIGINT)
LEFT JOIN "{SCHEMA}"."GROUP_CONTRACT" gc ON mem.groupid = gc.groupid
                                        AND gc.iscurrent = 1
WHERE m.iscurrent = TRUE
"""

CLAIMS_3M_SQL = f"""
SELECT
    cd.enrollee_id,
    cd.nhisproviderid                           AS claim_providerid,
    p.providername                              AS claim_provider_name,
    p.bands                                     AS claim_provider_band,
    CAST(cd.encounterdatefrom AS DATE)          AS encounter_date,
    CAST(cd.panumber AS VARCHAR)                 AS pa_number
FROM "{SCHEMA}"."CLAIMS DATA" cd
JOIN "{SCHEMA}"."PROVIDERS" p
    ON TRY_CAST(cd.nhisproviderid AS BIGINT) = TRY_CAST(p.providerid AS BIGINT)
WHERE CAST(cd.encounterdatefrom AS DATE) >= CURRENT_DATE - INTERVAL '3 months'
"""


# ── Core scan ─────────────────────────────────────────────────────────────────
def run_scan() -> tuple[int, int]:
    print(f"[{datetime.now(timezone.utc).isoformat()}] Starting provider band mapping scan…")

    sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    conn = duckdb.connect(MOTHERDUCK_DSN)
    try:
        # Step 1 – active member plan + provider mapping
        print("  Fetching active member provider mappings…")
        member_cols = [
            "enrollee_id", "fullname", "mapped_providerid",
            "mapped_provider_name", "mapped_provider_band",
            "planname", "plancode", "individualprice", "familyprice",
            "contract_end_date",
        ]
        members = [
            dict(zip(member_cols, row))
            for row in conn.execute(MEMBER_MAPPING_SQL).fetchall()
        ]
        print(f"  → {len(members)} active members")

        # Step 2 – claims in last 3 months
        print("  Fetching last-3-months claims…")
        claim_cols = [
            "enrollee_id", "claim_providerid",
            "claim_provider_name", "claim_provider_band", "encounter_date", "pa_number",
        ]
        claims_by_enrollee: dict[str, list] = defaultdict(list)
        for row in conn.execute(CLAIMS_3M_SQL).fetchall():
            rec = dict(zip(claim_cols, row))
            claims_by_enrollee[rec["enrollee_id"].upper()].append(rec)
        total_claims = sum(len(v) for v in claims_by_enrollee.values())
        print(f"  → {total_claims} claims across {len(claims_by_enrollee)} enrollees")
    finally:
        conn.close()

    scanned_at = datetime.now(timezone.utc).isoformat()
    mapping_flags: list[dict] = []
    visit_flags:   list[dict] = []

    for m in members:
        eid       = m["enrollee_id"]
        ind_price = float(m["individualprice"]) if m["individualprice"] is not None else None
        fam_price = float(m["familyprice"])     if m["familyprice"]     is not None else None
        allowed   = get_allowed_bands(ind_price, fam_price)

        if allowed is None:
            continue  # no price data — skip

        allowed_str  = ",".join(sorted(allowed, key=lambda b: BAND_RANK.get(b, 0)))
        mapped_band  = _normalise_band(m["mapped_provider_band"] or "")

        # ── Tab 1: Inappropriate Mapping ──────────────────────────────────────
        if mapped_band and is_higher_band(mapped_band, allowed):
            mapping_flags.append({
                "enrollee_id":          eid,
                "full_name":            (m["fullname"] or "").strip(),
                "plan_name":            m["planname"],
                "plan_code":            m["plancode"],
                "individual_price":     ind_price,
                "family_price":         fam_price,
                "allowed_bands":        allowed_str,
                "mapped_provider_id":   str(m["mapped_providerid"]) if m["mapped_providerid"] else None,
                "mapped_provider_name": m["mapped_provider_name"],
                "mapped_provider_band": mapped_band,
                "flag_reason":          (
                    f"Mapped to Band {mapped_band} but plan allows only Band {allowed_str}"
                ),
                "contract_end_date":    str(m["contract_end_date"]) if m["contract_end_date"] else None,
                "scanned_at":           scanned_at,
            })

        # ── Tab 2: Inappropriate Visits ───────────────────────────────────────
        enrollee_claims = claims_by_enrollee.get(eid.upper(), [])
        higher_rows = [
            c for c in enrollee_claims
            if is_higher_band(_normalise_band(c["claim_provider_band"] or ""), allowed)
        ]
        # Deduplicate by PA number — one PA = one visit regardless of procedure rows
        seen_pa: dict[str, dict] = {}
        for c in higher_rows:
            pa = c["pa_number"] or f"no_pa_{c['encounter_date']}_{c['claim_providerid']}"
            if pa not in seen_pa:
                seen_pa[pa] = c
        higher_visits = list(seen_pa.values())
        if len(higher_visits) > 2:
            providers_visited = sorted({
                c["claim_provider_name"]
                for c in higher_visits
                if c["claim_provider_name"]
            })
            last_visit = max(
                (c["encounter_date"] for c in higher_visits if c["encounter_date"]),
                default=None,
            )
            visit_flags.append({
                "enrollee_id":             eid,
                "full_name":               (m["fullname"] or "").strip(),
                "plan_name":               m["planname"],
                "individual_price":        ind_price,
                "family_price":            fam_price,
                "allowed_bands":           allowed_str,
                "visit_count_higher_band": len(higher_visits),
                "last_visit_date":         str(last_visit) if last_visit else None,
                "higher_band_providers":   json.dumps(providers_visited),
                "contract_end_date":       str(m["contract_end_date"]) if m["contract_end_date"] else None,
                "scanned_at":              scanned_at,
            })

    # Deduplicate by enrollee_id (keep last seen — handles members with multiple plans)
    mapping_flags = list({r["enrollee_id"]: r for r in mapping_flags}.values())
    visit_flags   = list({r["enrollee_id"]: r for r in visit_flags}.values())

    print(f"  → {len(mapping_flags)} inappropriate mapping flags")
    print(f"  → {len(visit_flags)} inappropriate visit flags")

    # ── Upsert to Supabase (truncate + re-insert for daily refresh) ────────────
    print("  Upserting to Supabase…")
    BATCH = 500

    sb.table("enrollee_provider_mapping_flags").delete().neq("id", 0).execute()
    for i in range(0, len(mapping_flags), BATCH):
        sb.table("enrollee_provider_mapping_flags").insert(mapping_flags[i:i + BATCH]).execute()

    sb.table("enrollee_provider_visit_flags").delete().neq("id", 0).execute()
    for i in range(0, len(visit_flags), BATCH):
        sb.table("enrollee_provider_visit_flags").insert(visit_flags[i:i + BATCH]).execute()

    print(f"  ✓ Done.")
    return len(mapping_flags), len(visit_flags)


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    missing = [v for v in ("MOTHERDUCK_TOKEN", "SUPABASE_URL", "SUPABASE_KEY") if not os.getenv(v)]
    if missing:
        print(f"ERROR: Missing env vars: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)

    try:
        mapping_count, visit_count = run_scan()
        print(f"\nScan complete — Mapping flags: {mapping_count} | Visit flags: {visit_count}")
    except Exception:
        traceback.print_exc()
        sys.exit(1)
