"""
Monthly AI Medical Analytics Report — Clearline HMO
======================================================
Runs on the last day of every month (or on-demand).

ANALYTICAL FRAMEWORK
--------------------
This script is built around the core financial objective of an HMO:
    MLR = Total Medical Cost / Premium Revenue
    Target MLR < 75%  →  Profit margin ≥ 25%

Every section is framed around one of three questions:
    1. WHAT IS THE TRUE COST (and is it controllable)?
    2. WHAT ARE WE ACTUALLY PAYING OUT NOW (cash flow)?
    3. WHAT LIABILITY IS STILL COMING (outstanding/IBNR)?

KEY DATE DIMENSION RULES:
    encounterdatefrom = when care was delivered  → disease burden, utilisation
    datesubmitted     = when provider billed us  → cash outflow, payment pressure
    requestdate (PA)  = when PA was requested    → committed liability

Run locally:
  MOTHERDUCK_TOKEN=... SUPABASE_URL=... SUPABASE_KEY=... ANTHROPIC_API_KEY=... \
  python scripts/monthly_ai_report.py
"""

import os, json
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from collections import defaultdict

import duckdb
import anthropic
from supabase import create_client

# ── Config ─────────────────────────────────────────────────────────────────────
MOTHERDUCK_TOKEN  = os.environ["MOTHERDUCK_TOKEN"]
SUPABASE_URL      = os.environ["SUPABASE_URL"]
SUPABASE_KEY      = os.environ["SUPABASE_KEY"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
S = "AI DRIVEN DATA"

TODAY        = date.today()
CY           = TODAY.year
PY           = TODAY.year - 1
PPY          = TODAY.year - 2

# Report covers the just-completed month
REPORT_MONTH = TODAY.month if TODAY.day > 20 else (TODAY.month - 1 or 12)
REPORT_YEAR  = CY if REPORT_MONTH <= TODAY.month else PY
REPORT_LABEL = f"{REPORT_YEAR}-{REPORT_MONTH:02d}"

# Rolling 14-month window (gives 2 months of YoY overlap)
ROLLING_START = (TODAY - relativedelta(months=14)).replace(day=1)

MONTH_LABELS = ["Jan","Feb","Mar","Apr","May","Jun",
                "Jul","Aug","Sep","Oct","Nov","Dec"]

def fmt_n(n):  return f"₦{n:,.0f}"
def fmt_m(n):  return f"₦{n/1_000_000:.1f}M"
def pct(a, b): return f"{a/b*100:.1f}%" if b else "—"
def safe_div(a, b): return round(a/b, 4) if b else 0.0


# ══════════════════════════════════════════════════════════════════════════════
# DATA COLLECTION
# ══════════════════════════════════════════════════════════════════════════════

def collect_data(con) -> dict:
    data = {}

    # ── PART 0: ENROLLMENT ────────────────────────────────────────────────────
    print("  [0/17] Enrolled headcount by month...")
    rows = con.execute(f"""
        SELECT
            YEAR(CAST(gc.startdate AS DATE))   AS yr,
            MONTH(CAST(gc.startdate AS DATE))  AS mo,
            COUNT(DISTINCT m.memberid)          AS enrolled_members,
            COUNT(DISTINCT gc.groupid)          AS active_groups
        FROM "{S}"."MEMBERS" m
        JOIN "{S}"."GROUP_CONTRACT" gc ON m.groupid = gc.groupid
        WHERE gc.iscurrent = TRUE
        GROUP BY 1, 2
        ORDER BY 1, 2
    """).fetchall()
    data["enrollment_monthly"] = [
        {"year": r[0], "month": MONTH_LABELS[r[1]-1], "month_num": r[1],
         "enrolled": r[2], "groups": r[3]}
        for r in rows
    ]

    current_enrolled = con.execute(f"""
        SELECT COUNT(DISTINCT memberid) FROM "{S}"."MEMBERS" WHERE iscurrent = TRUE
    """).fetchone()[0] or 1
    data["current_enrolled"] = current_enrolled

    # ── PART 1A: PAYMENT OUTFLOW ──────────────────────────────────────────────
    print("  [1a/17] Payment outflow by submission date (actual cash flow)...")
    rows = con.execute(f"""
        SELECT
            YEAR(CAST(datesubmitted AS DATE))     AS submit_yr,
            MONTH(CAST(datesubmitted AS DATE))    AS submit_mo,
            YEAR(CAST(encounterdatefrom AS DATE)) AS encounter_yr,
            COUNT(DISTINCT claimnumber)           AS claim_count,
            ROUND(SUM(approvedamount), 0)          AS approved,
            COUNT(DISTINCT enrollee_id)            AS enrollees
        FROM "{S}"."CLAIMS DATA"
        WHERE CAST(datesubmitted AS DATE) >= DATE '{ROLLING_START}'
          AND approvedamount > 0
          AND datesubmitted IS NOT NULL
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3
    """).fetchall()
    data["payment_outflow"] = [
        {
            "submit_year": r[0], "submit_month": MONTH_LABELS[r[1]-1], "submit_month_num": r[1],
            "encounter_year": r[2],
            "is_current_year_service": r[2] == r[0],
            "is_prior_year_tail": r[2] < r[0],
            "claim_count": r[3], "approved": int(r[4] or 0), "enrollees": r[5]
        }
        for r in rows
    ]

    # ── PART 1B: DISEASE BURDEN ───────────────────────────────────────────────
    print("  [1b/17] Disease burden by encounter date (utilisation picture)...")
    rows = con.execute(f"""
        SELECT
            YEAR(CAST(encounterdatefrom AS DATE))   AS enc_yr,
            MONTH(CAST(encounterdatefrom AS DATE))  AS enc_mo,
            COUNT(DISTINCT claimnumber)             AS claim_count,
            ROUND(SUM(approvedamount), 0)            AS approved,
            ROUND(SUM(chargeamount), 0)              AS charged,
            ROUND(SUM(deniedamount), 0)              AS denied,
            COUNT(DISTINCT enrollee_id)              AS claimants,
            SUM(CASE WHEN isinpatient THEN 1 ELSE 0 END) AS inpatient_claims,
            ROUND(SUM(CASE WHEN isinpatient THEN approvedamount ELSE 0 END), 0) AS inpatient_cost
        FROM "{S}"."CLAIMS DATA"
        WHERE CAST(encounterdatefrom AS DATE) >= DATE '{ROLLING_START}'
          AND approvedamount > 0
        GROUP BY 1, 2 ORDER BY 1, 2
    """).fetchall()
    data["disease_burden_monthly"] = [
        {
            "year": r[0], "month": MONTH_LABELS[r[1]-1], "month_num": r[1],
            "claim_count": r[2], "approved": int(r[3] or 0),
            "charged": int(r[4] or 0), "denied": int(r[5] or 0),
            "claimants": r[6],
            "denial_rate_pct": round((r[5] or 0) / (r[4] or 1) * 100, 2),
            "inpatient_claims": r[7], "inpatient_cost": int(r[8] or 0),
            "inpatient_cost_pct": round((r[8] or 0) / (r[3] or 1) * 100, 1),
        }
        for r in rows
    ]

    # ── PART 2: IBNR ──────────────────────────────────────────────────────────
    print("  [2/17] IBNR & outstanding liability analysis...")
    rows = con.execute(f"""
        SELECT
            YEAR(CAST(encounterdatefrom AS DATE))   AS enc_yr,
            MONTH(CAST(encounterdatefrom AS DATE))  AS enc_mo,
            DATEDIFF('day',
                CAST(encounterdatefrom AS DATE),
                CAST(datesubmitted AS DATE))         AS lag_days,
            COUNT(DISTINCT claimnumber)             AS claims,
            ROUND(SUM(approvedamount), 0)            AS approved
        FROM "{S}"."CLAIMS DATA"
        WHERE CAST(encounterdatefrom AS DATE) >= DATE '{date(CY-2, 1, 1)}'
          AND datesubmitted IS NOT NULL
          AND approvedamount > 0
          AND DATEDIFF('day',
                CAST(encounterdatefrom AS DATE),
                CAST(datesubmitted AS DATE)) >= 0
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3
    """).fetchall()

    ibnr_map = defaultdict(lambda: {"<30d": 0, "30-60d": 0, "61-90d": 0,
                                     "91-180d": 0, "181-365d": 0, "365d+": 0,
                                     "total_submitted": 0})
    for (ey, em, lag, cnt, amt) in rows:
        k = (ey, em)
        ibnr_map[k]["total_submitted"] += (amt or 0)
        if lag < 30:    ibnr_map[k]["<30d"]      += (amt or 0)
        elif lag < 60:  ibnr_map[k]["30-60d"]    += (amt or 0)
        elif lag < 90:  ibnr_map[k]["61-90d"]    += (amt or 0)
        elif lag < 180: ibnr_map[k]["91-180d"]   += (amt or 0)
        elif lag < 365: ibnr_map[k]["181-365d"]  += (amt or 0)
        else:           ibnr_map[k]["365d+"]     += (amt or 0)

    data["submission_runout"] = [
        {"enc_year": k[0], "enc_month": MONTH_LABELS[k[1]-1], "enc_month_num": k[1],
         **{bkt: int(v) for bkt, v in m.items()}}
        for k, m in sorted(ibnr_map.items())
    ]

    rows2 = con.execute(f"""
        SELECT
            YEAR(CAST(datesubmitted     AS DATE)) AS submit_yr,
            YEAR(CAST(encounterdatefrom AS DATE)) AS enc_yr,
            COUNT(DISTINCT claimnumber)           AS claims,
            ROUND(SUM(approvedamount), 0)          AS approved
        FROM "{S}"."CLAIMS DATA"
        WHERE approvedamount > 0 AND datesubmitted IS NOT NULL
        GROUP BY 1, 2 ORDER BY 1, 2
    """).fetchall()
    data["cross_year_payments"] = [
        {"submit_year": r[0], "encounter_year": r[1],
         "claims": r[2], "approved": int(r[3] or 0),
         "is_tail_payment": r[1] < r[0]}
        for r in rows2
    ]

    # ── PART 3: PMPM ──────────────────────────────────────────────────────────
    print("  [3/17] PMPM analysis (real cash collected ÷ enrolled members)...")
    rows = con.execute(f"""
        WITH monthly_cash AS (
            SELECT
                YEAR(Date)  AS yr,
                MONTH(Date) AS mo,
                ROUND(SUM(Amount), 0) AS cash_received
            FROM "{S}"."CLIENT_CASH_RECEIVED"
            WHERE Date >= DATE '{ROLLING_START}'
            GROUP BY 1, 2
        ),
        monthly_claims AS (
            SELECT
                YEAR(CAST(encounterdatefrom AS DATE))  AS yr,
                MONTH(CAST(encounterdatefrom AS DATE)) AS mo,
                ROUND(SUM(approvedamount), 0)           AS claims_approved,
                COUNT(DISTINCT enrollee_id)             AS claimants,
                COUNT(DISTINCT claimnumber)             AS claim_count
            FROM "{S}"."CLAIMS DATA"
            WHERE approvedamount > 0
              AND CAST(encounterdatefrom AS DATE) >= DATE '{ROLLING_START}'
            GROUP BY 1, 2
        ),
        monthly_pa AS (
            SELECT
                YEAR(CAST(requestdate AS DATE))  AS yr,
                MONTH(CAST(requestdate AS DATE)) AS mo,
                COUNT(DISTINCT panumber)          AS pa_count,
                ROUND(SUM(granted), 0)            AS pa_granted,
                ROUND(SUM(requested), 0)          AS pa_requested
            FROM "{S}"."PA DATA"
            WHERE CAST(requestdate AS DATE) >= DATE '{ROLLING_START}'
            GROUP BY 1, 2
        )
        SELECT
            mc.yr, mc.mo,
            mc.claims_approved, mc.claimants, mc.claim_count,
            cash.cash_received,
            pa.pa_count, pa.pa_granted, pa.pa_requested
        FROM monthly_claims mc
        LEFT JOIN monthly_cash cash ON mc.yr = cash.yr AND mc.mo = cash.mo
        LEFT JOIN monthly_pa   pa   ON mc.yr = pa.yr   AND mc.mo = pa.mo
        ORDER BY mc.yr, mc.mo
    """).fetchall()

    annual_cash = con.execute(f"""
        SELECT YEAR(Date) AS yr, ROUND(SUM(Amount), 0) AS total_cash
        FROM "{S}"."CLIENT_CASH_RECEIVED"
        WHERE YEAR(Date) IN ({CY}, {PY})
        GROUP BY 1
    """).fetchall()
    annual_cash_map = {r[0]: int(r[1] or 0) for r in annual_cash}

    data["pmpm_monthly"] = [
        {
            "year": r[0], "month": MONTH_LABELS[r[1]-1], "month_num": r[1],
            "claims_approved": int(r[2] or 0),
            "claimants": r[3], "claim_count": r[4],
            "cash_received": int(r[5] or 0),
            "pa_count": r[6] or 0,
            "pa_granted": int(r[7] or 0),
            "pa_requested": int(r[8] or 0),
            "pmpm_cash":   round((r[5] or 0) / max(current_enrolled, 1), 2),
            "pmpm_claims": round((r[2] or 0) / max(current_enrolled, 1), 2),
        }
        for r in rows
    ]
    data["annualised_pmpm"] = {
        yr: round(annual_cash_map.get(yr, 0) / 12 / max(current_enrolled, 1), 2)
        for yr in [PY, CY]
    }

    # ── PART 4: CLAIM FREQUENCY ───────────────────────────────────────────────
    print("  [4/17] Claim frequency & utilisation rate...")
    rows = con.execute(f"""
        SELECT
            YEAR(CAST(encounterdatefrom AS DATE)) AS yr,
            COUNT(DISTINCT claimnumber)           AS total_claims,
            COUNT(DISTINCT enrollee_id)           AS claimants,
            COUNT(DISTINCT (enrollee_id || '|' ||
                CAST(encounterdatefrom AS VARCHAR))) AS unique_encounters
        FROM "{S}"."CLAIMS DATA"
        WHERE approvedamount > 0
          AND CAST(encounterdatefrom AS DATE) >= DATE '{date(CY-2, 1, 1)}'
        GROUP BY 1 ORDER BY 1
    """).fetchall()

    enrolled_by_yr = con.execute(f"""
        SELECT
            YEAR(CAST(gc.startdate AS DATE)) AS yr,
            COUNT(DISTINCT m.memberid)        AS enrolled
        FROM "{S}"."MEMBERS" m
        JOIN "{S}"."GROUP_CONTRACT" gc ON m.groupid = gc.groupid
        WHERE gc.iscurrent = TRUE
        GROUP BY 1
    """).fetchall()
    enrolled_map = {r[0]: r[1] for r in enrolled_by_yr}

    data["claim_frequency"] = [
        {
            "year": r[0],
            "total_claims": r[1], "claimants": r[2], "unique_encounters": r[3],
            "enrolled": enrolled_map.get(r[0], current_enrolled),
            "claims_per_1000_members": round(r[1] / enrolled_map.get(r[0], current_enrolled) * 1000, 1),
            "utilisation_rate_pct": round(r[2] / enrolled_map.get(r[0], current_enrolled) * 100, 1),
            "avg_claims_per_claimant": round(r[1] / r[2], 1) if r[2] else 0,
        }
        for r in rows
    ]

    # ── PART 5: PA MONTHLY ────────────────────────────────────────────────────
    print("  [5/17] PA volume & committed liability...")
    rows = con.execute(f"""
        SELECT
            YEAR(CAST(requestdate AS DATE))  AS yr,
            MONTH(CAST(requestdate AS DATE)) AS mo,
            COUNT(DISTINCT panumber)          AS pa_count,
            ROUND(SUM(requested), 0)          AS total_requested,
            ROUND(SUM(granted), 0)            AS total_granted,
            ROUND(SUM(requested) - SUM(granted), 0) AS pa_reduction,
            COUNT(DISTINCT groupname)         AS groups,
            COUNT(DISTINCT providerid)        AS providers,
            COUNT(DISTINCT IID)               AS unique_enrollees
        FROM "{S}"."PA DATA"
        WHERE CAST(requestdate AS DATE) >= DATE '{ROLLING_START}'
        GROUP BY 1, 2 ORDER BY 1, 2
    """).fetchall()
    data["pa_monthly"] = [
        {
            "year": r[0], "month": MONTH_LABELS[r[1]-1], "month_num": r[1],
            "pa_count": r[2],
            "requested": int(r[3] or 0), "granted": int(r[4] or 0),
            "pa_reduction": int(r[5] or 0),
            "pa_gate_rate_pct": round((r[4] or 0) / (r[3] or 1) * 100, 1),
            "avg_granted_per_pa": round((r[4] or 0) / (r[2] or 1), 0),
            "groups": r[6], "providers": r[7], "enrollees": r[8]
        }
        for r in rows
    ]

    # ── PART 6: CLAIMS VETTING ────────────────────────────────────────────────
    print("  [6/17] Claims vetting effectiveness (PA granted vs claims approved)...")
    rows = con.execute(f"""
        WITH pa_norm AS (
            SELECT
                CAST(CAST(panumber AS BIGINT) AS VARCHAR) AS pa_num,
                YEAR(CAST(requestdate AS DATE))            AS yr,
                MONTH(CAST(requestdate AS DATE))           AS mo,
                groupname, providerid,
                ROUND(SUM(granted), 0)                     AS pa_granted,
                ROUND(SUM(requested), 0)                   AS pa_requested
            FROM "{S}"."PA DATA"
            WHERE panumber IS NOT NULL
              AND CAST(requestdate AS DATE) >= DATE '{ROLLING_START}'
            GROUP BY 1, 2, 3, 4, 5
        ),
        cl_norm AS (
            SELECT
                CAST(CAST(panumber AS BIGINT) AS VARCHAR) AS pa_num,
                ROUND(SUM(approvedamount), 0)              AS cl_approved,
                ROUND(SUM(deniedamount), 0)                AS cl_denied,
                ROUND(SUM(chargeamount), 0)                AS cl_charged
            FROM "{S}"."CLAIMS DATA"
            WHERE panumber IS NOT NULL AND approvedamount > 0
            GROUP BY 1
        )
        SELECT
            pa.yr, pa.mo,
            COUNT(DISTINCT pa.pa_num)     AS matched_panumbers,
            ROUND(SUM(pa.pa_granted), 0)  AS pa_granted_matched,
            ROUND(SUM(cl.cl_approved), 0) AS cl_approved,
            ROUND(SUM(cl.cl_denied), 0)   AS cl_denied,
            ROUND(SUM(cl.cl_charged), 0)  AS cl_charged,
            ROUND((SUM(pa.pa_granted) - SUM(cl.cl_approved))
                  / NULLIF(SUM(pa.pa_granted), 0) * 100, 2) AS vetting_reduction_pct
        FROM pa_norm pa
        JOIN cl_norm cl ON pa.pa_num = cl.pa_num
        GROUP BY 1, 2 ORDER BY 1, 2
    """).fetchall()
    data["vetting_monthly"] = [
        {
            "year": r[0], "month": MONTH_LABELS[r[1]-1], "month_num": r[1],
            "matched_panumbers": r[2],
            "pa_granted": int(r[3] or 0), "cl_approved": int(r[4] or 0),
            "cl_denied": int(r[5] or 0), "cl_charged": int(r[6] or 0),
            "vetting_reduction_pct": float(r[7] or 0),
            "amount_recovered_by_vetting": int((r[3] or 0) - (r[4] or 0)),
        }
        for r in rows
    ]

    # ── PART 6B: THREE COST POOLS ─────────────────────────────────────────────
    print("  [6b/17] Pre-computing cost pool separation...")
    cost_pools_pre = {}
    for year in [PY, CY]:
        vm  = [r for r in data["vetting_monthly"] if r["year"] == year]
        pam = [r for r in data["pa_monthly"]       if r["year"] == year]
        pool_a_pa  = sum(r["pa_granted"]                  for r in vm)
        pool_a_cl  = sum(r["cl_approved"]                 for r in vm)
        pool_a_rec = sum(r["amount_recovered_by_vetting"] for r in vm)
        total_pa   = sum(r["granted"]                     for r in pam)
        cost_pools_pre[year] = {
            "pool_a_pa_granted":      int(pool_a_pa),
            "pool_a_cl_approved":     int(pool_a_cl),
            "pool_a_recovered":       int(pool_a_rec),
            "pool_a_vetting_pct":     round(pool_a_rec / pool_a_pa * 100, 2) if pool_a_pa else 0,
            "pool_b_noauth":          0,   # filled after noauth query
            "pool_c_outstanding_pa":  int(max(0, total_pa - pool_a_pa)),
            "total_pa_committed":     int(total_pa),
        }
    data["cost_pools"] = cost_pools_pre

    # ── PART 7: NO-AUTH CLAIMS ────────────────────────────────────────────────
    print("  [7/17] No-auth (NI Auth) claims analysis...")
    rows = con.execute(f"""
        SELECT
            YEAR(CAST(encounterdatefrom AS DATE))   AS yr,
            MONTH(CAST(encounterdatefrom AS DATE))  AS mo,
            COUNT(DISTINCT claimnumber)             AS claim_count,
            ROUND(SUM(approvedamount), 0)            AS approved,
            ROUND(SUM(chargeamount), 0)              AS charged,
            ROUND(SUM(deniedamount), 0)              AS denied,
            COUNT(DISTINCT enrollee_id)              AS enrollees,
            SUM(CASE WHEN isinpatient THEN 1 ELSE 0 END) AS inpatient_count
        FROM "{S}"."CLAIMS DATA"
        WHERE panumber IS NULL
          AND approvedamount > 0
          AND CAST(encounterdatefrom AS DATE) >= DATE '{ROLLING_START}'
        GROUP BY 1, 2 ORDER BY 1, 2
    """).fetchall()
    data["noauth_monthly"] = [
        {"year": r[0], "month": MONTH_LABELS[r[1]-1], "month_num": r[1],
         "claim_count": r[2], "approved": int(r[3] or 0),
         "charged": int(r[4] or 0), "denied": int(r[5] or 0),
         "enrollees": r[6], "inpatient": r[7],
         "denial_rate_pct": round((r[5] or 0) / (r[4] or 1) * 100, 1)}
        for r in rows
    ]

    # Backfill Pool B
    for year in [PY, CY]:
        noauth_total = sum(r["approved"] for r in data["noauth_monthly"] if r["year"] == year)
        data["cost_pools"][year]["pool_b_noauth"] = int(noauth_total)
        data["cost_pools"][year]["total_actual_claims_paid"] = (
            data["cost_pools"][year]["pool_a_cl_approved"] + noauth_total
        )

    rows = con.execute(f"""
        SELECT
            cd.nhisproviderid,
            COALESCE(pr.providername, '(Unknown)') AS provider_name,
            pr.bands,
            COUNT(DISTINCT cd.claimnumber)         AS claim_count,
            ROUND(SUM(cd.approvedamount), 0)        AS approved,
            COUNT(DISTINCT cd.enrollee_id)          AS enrollees,
            ROUND(SUM(cd.deniedamount), 0)          AS denied
        FROM "{S}"."CLAIMS DATA" cd
        LEFT JOIN "{S}"."PROVIDERS" pr
            ON TRY_CAST(cd.nhisproviderid AS BIGINT) = TRY_CAST(pr.providerid AS BIGINT)
        WHERE cd.panumber IS NULL
          AND cd.approvedamount > 0
          AND CAST(cd.encounterdatefrom AS DATE) >= DATE '{date(CY-1, 1, 1)}'
        GROUP BY 1, 2, 3
        ORDER BY approved DESC LIMIT 25
    """).fetchall()
    data["noauth_by_provider"] = [
        {"provider_id": str(r[0]), "name": r[1], "band": r[2] or "—",
         "claim_count": r[3], "approved": int(r[4] or 0),
         "enrollees": r[5], "denied": int(r[6] or 0)}
        for r in rows
    ]

    # ── PART 8 & 9: DISEASE BURDEN ────────────────────────────────────────────
    print("  [8/17] Disease burden by diagnosis (claims)...")
    rows = con.execute(f"""
        SELECT
            cd.diagnosiscode,
            COALESCE(d.diagnosisdesc, cd.diagnosiscode) AS diagnosis_name,
            YEAR(CAST(cd.encounterdatefrom AS DATE))     AS yr,
            COUNT(DISTINCT cd.claimnumber)               AS claim_count,
            ROUND(SUM(cd.approvedamount), 0)             AS approved,
            COUNT(DISTINCT cd.enrollee_id)               AS enrollees,
            ROUND(AVG(cd.approvedamount), 0)             AS avg_cost_per_claim
        FROM "{S}"."CLAIMS DATA" cd
        LEFT JOIN "{S}"."DIAGNOSIS" d ON cd.diagnosiscode = d.diagnosiscode
        WHERE CAST(cd.encounterdatefrom AS DATE) >= DATE '{date(CY-2, 1, 1)}'
          AND cd.approvedamount > 0
          AND cd.diagnosiscode IS NOT NULL AND cd.diagnosiscode != ''
        GROUP BY 1, 2, 3
        ORDER BY 5 DESC
    """).fetchall()
    dx_map = defaultdict(lambda: {"name": "", "years": {}})
    for r in rows:
        dx_map[r[0]]["name"] = r[1]
        dx_map[r[0]]["years"][str(r[2])] = {
            "claims": r[3], "approved": int(r[4] or 0),
            "enrollees": r[5], "avg_cost": int(r[6] or 0)
        }
    sorted_dx = sorted(dx_map.items(),
                       key=lambda x: sum(v["approved"] for v in x[1]["years"].values()),
                       reverse=True)[:40]
    data["claims_by_diagnosis"] = [
        {"code": c, "name": v["name"], "years": v["years"]}
        for c, v in sorted_dx
    ]

    print("  [9/17] Disease burden by diagnosis (PA)...")
    rows = con.execute(f"""
        SELECT
            td.code,
            td.desc,
            COUNT(DISTINCT CAST(CAST(td.panumber AS BIGINT) AS VARCHAR)) AS pa_count,
            ROUND(SUM(pa.granted), 0)   AS total_granted,
            ROUND(AVG(pa.granted), 0)   AS avg_granted,
            COUNT(DISTINCT pa.IID)      AS enrollees
        FROM "{S}"."TBPADIAGNOSIS" td
        JOIN "{S}"."PA DATA" pa
            ON CAST(CAST(td.panumber AS BIGINT) AS VARCHAR)
             = CAST(CAST(pa.panumber AS BIGINT) AS VARCHAR)
        WHERE CAST(pa.requestdate AS DATE) >= DATE '{date(CY-1, 1, 1)}'
          AND td.code IS NOT NULL AND td.code != ''
        GROUP BY 1, 2 ORDER BY 4 DESC LIMIT 50
    """).fetchall()
    data["pa_top_diagnoses"] = [
        {"code": r[0], "description": r[1], "pa_count": r[2],
         "granted": int(r[3] or 0), "avg_granted": int(r[4] or 0),
         "enrollees": r[5]}
        for r in rows
    ]

    # ── PART 10: TOP PROVIDERS ────────────────────────────────────────────────
    print("  [10/17] Provider analysis (PA + claims, YoY)...")
    rows = con.execute(f"""
        SELECT
            pa.providerid,
            COALESCE(pr.providername, '(Unknown)') AS provider_name,
            pr.bands,
            YEAR(CAST(pa.requestdate AS DATE))     AS yr,
            COUNT(DISTINCT pa.panumber)            AS pa_count,
            ROUND(SUM(pa.granted), 0)              AS pa_granted,
            COUNT(DISTINCT pa.IID)                 AS pa_enrollees,
            ROUND(SUM(pa.granted) / NULLIF(COUNT(DISTINCT pa.IID), 0), 0) AS pa_cpe
        FROM "{S}"."PA DATA" pa
        LEFT JOIN "{S}"."PROVIDERS" pr
            ON TRY_CAST(pa.providerid AS BIGINT) = TRY_CAST(pr.providerid AS BIGINT)
        WHERE CAST(pa.requestdate AS DATE) >= DATE '{date(CY-1, 1, 1)}'
        GROUP BY 1, 2, 3, 4 ORDER BY 4, 6 DESC
    """).fetchall()
    prov_map = defaultdict(lambda: {"name": "", "band": "", "by_year": {}})
    for r in rows:
        prov_map[r[0]]["name"] = r[1]
        prov_map[r[0]]["band"] = r[2] or "—"
        prov_map[r[0]]["by_year"][str(r[3])] = {
            "pa_count": r[4], "pa_granted": int(r[5] or 0),
            "enrollees": r[6], "cpe": int(r[7] or 0)
        }
    sorted_prov = sorted(prov_map.items(),
                         key=lambda x: sum(v["pa_granted"] for v in x[1]["by_year"].values()),
                         reverse=True)[:30]
    data["pa_by_provider"] = [
        {"provider_id": pid, "name": v["name"], "band": v["band"], "by_year": v["by_year"]}
        for pid, v in sorted_prov
    ]

    rows = con.execute(f"""
        SELECT
            cd.nhisproviderid,
            COALESCE(pr.providername, '(Unknown)') AS provider_name,
            pr.bands,
            YEAR(CAST(cd.encounterdatefrom AS DATE)) AS yr,
            COUNT(DISTINCT cd.claimnumber)           AS claim_count,
            ROUND(SUM(cd.approvedamount), 0)          AS approved,
            COUNT(DISTINCT cd.enrollee_id)            AS enrollees,
            SUM(CASE WHEN cd.isinpatient THEN 1 ELSE 0 END) AS inpatient,
            ROUND(SUM(CASE WHEN cd.isinpatient THEN cd.approvedamount ELSE 0 END), 0) AS inpatient_cost
        FROM "{S}"."CLAIMS DATA" cd
        LEFT JOIN "{S}"."PROVIDERS" pr
            ON TRY_CAST(cd.nhisproviderid AS BIGINT) = TRY_CAST(pr.providerid AS BIGINT)
        WHERE CAST(cd.encounterdatefrom AS DATE) >= DATE '{date(CY-2, 1, 1)}'
          AND cd.approvedamount > 0
        GROUP BY 1, 2, 3, 4 ORDER BY 4 DESC, 6 DESC
    """).fetchall()
    cprov_map = defaultdict(lambda: {"name": "", "band": "", "by_year": {}})
    for r in rows:
        cprov_map[r[0]]["name"] = r[1]
        cprov_map[r[0]]["band"] = r[2] or "—"
        cprov_map[r[0]]["by_year"][str(r[3])] = {
            "claims": r[4], "approved": int(r[5] or 0),
            "enrollees": r[6], "inpatient": r[7],
            "inpatient_cost": int(r[8] or 0)
        }
    sorted_cprov = sorted(cprov_map.items(),
                          key=lambda x: sum(v["approved"] for v in x[1]["by_year"].values()),
                          reverse=True)[:30]
    data["claims_by_provider"] = [
        {"provider_id": pid, "name": v["name"], "band": v["band"], "by_year": v["by_year"]}
        for pid, v in sorted_cprov
    ]

    # ── PART 11: CLIENT GROUPS ────────────────────────────────────────────────
    print("  [11/17] Client group analysis (PA YoY)...")
    rows = con.execute(f"""
        SELECT groupname,
               YEAR(CAST(requestdate AS DATE)) AS yr,
               COUNT(DISTINCT panumber)         AS pa_count,
               ROUND(SUM(granted), 0)           AS pa_granted,
               COUNT(DISTINCT IID)              AS enrollees,
               ROUND(SUM(granted)/NULLIF(COUNT(DISTINCT IID),0),0) AS cpe
        FROM "{S}"."PA DATA"
        WHERE CAST(requestdate AS DATE) >= DATE '{date(CY-1, 1, 1)}'
          AND groupname IS NOT NULL AND TRIM(groupname) != ''
        GROUP BY 1, 2 ORDER BY 4 DESC
    """).fetchall()
    grp_map = defaultdict(lambda: {"by_year": {}})
    for r in rows:
        grp_map[r[0]]["by_year"][str(r[1])] = {
            "pa_count": r[2], "granted": int(r[3] or 0),
            "enrollees": r[4], "cpe": int(r[5] or 0)
        }
    sorted_grps = sorted(grp_map.items(),
                         key=lambda x: sum(v["granted"] for v in x[1]["by_year"].values()),
                         reverse=True)[:30]
    data["pa_by_group"] = [
        {"group": g, "by_year": v["by_year"]}
        for g, v in sorted_grps
    ]

    # ── PART 12: INPATIENT ────────────────────────────────────────────────────
    print("  [12/17] Inpatient admissions analysis...")
    rows = con.execute(f"""
        SELECT
            YEAR(CAST(cd.encounterdatefrom AS DATE)) AS yr,
            COALESCE(d.diagnosisdesc, cd.diagnosiscode) AS diagnosis,
            COUNT(DISTINCT cd.claimnumber)   AS admissions,
            ROUND(SUM(cd.approvedamount), 0) AS total_cost,
            ROUND(AVG(cd.approvedamount), 0) AS avg_cost_per_claim,
            COUNT(DISTINCT cd.enrollee_id)   AS patients
        FROM "{S}"."CLAIMS DATA" cd
        LEFT JOIN "{S}"."DIAGNOSIS" d ON cd.diagnosiscode = d.diagnosiscode
        WHERE cd.isinpatient = TRUE
          AND cd.approvedamount > 0
          AND CAST(cd.encounterdatefrom AS DATE) >= DATE '{date(CY-2, 1, 1)}'
        GROUP BY 1, 2 ORDER BY 1 DESC, 4 DESC
    """).fetchall()
    ip_map = defaultdict(lambda: {"by_year": {}})
    for r in rows:
        ip_map[r[1]]["by_year"][str(r[0])] = {
            "admissions": r[2], "cost": int(r[3] or 0),
            "avg_cost": int(r[4] or 0), "patients": r[5]
        }
    sorted_ip = sorted(ip_map.items(),
                       key=lambda x: sum(v["cost"] for v in x[1]["by_year"].values()),
                       reverse=True)[:25]
    data["inpatient_by_diagnosis"] = [
        {"diagnosis": dx, "by_year": v["by_year"]}
        for dx, v in sorted_ip
    ]

    # ── PART 13: HIGH UTILISERS ───────────────────────────────────────────────
    print("  [13/17] High utiliser enrollees...")
    rows = con.execute(f"""
        SELECT
            cd.enrollee_id,
            COALESCE(mem.firstname || ' ' || mem.lastname, cd.enrollee_id) AS name,
            g.groupname,
            YEAR(CAST(cd.encounterdatefrom AS DATE)) AS yr,
            COUNT(DISTINCT cd.claimnumber)   AS claim_count,
            ROUND(SUM(cd.approvedamount), 0) AS total_approved,
            COUNT(DISTINCT cd.diagnosiscode) AS distinct_diagnoses,
            SUM(CASE WHEN cd.isinpatient THEN 1 ELSE 0 END) AS inpatient_episodes,
            ROUND(AVG(cd.approvedamount), 0) AS avg_cost_per_claim
        FROM "{S}"."CLAIMS DATA" cd
        LEFT JOIN "{S}"."MEMBERS" ms ON cd.enrollee_id = ms.enrollee_id
        LEFT JOIN "{S}"."MEMBER"  mem ON CAST(ms.memberid AS BIGINT) = mem.memberid
        LEFT JOIN "{S}"."GROUPS"  g   ON ms.groupid = g.groupid
        WHERE CAST(cd.encounterdatefrom AS DATE) >= DATE '{date(CY-1, 1, 1)}'
          AND cd.approvedamount > 0
        GROUP BY 1, 2, 3, 4
        HAVING SUM(cd.approvedamount) > 500000
        ORDER BY 4 DESC, 6 DESC
        LIMIT 40
    """).fetchall()
    data["high_utilisers"] = [
        {"enrollee_id": r[0], "name": r[1], "group": r[2] or "—",
         "year": r[3], "claims": r[4], "approved": int(r[5] or 0),
         "diagnoses": r[6], "inpatient_episodes": r[7],
         "avg_cost_per_claim": int(r[8] or 0)}
        for r in rows
    ]

    # ── PART 14: PROCEDURES ───────────────────────────────────────────────────
    print("  [14/17] Top procedures by cost...")
    rows = con.execute(f"""
        SELECT
            cd.code,
            COALESCE(pd.proceduredesc, cd.code) AS procedure_name,
            YEAR(CAST(cd.encounterdatefrom AS DATE)) AS yr,
            COUNT(DISTINCT cd.claimnumber) AS claim_count,
            ROUND(SUM(cd.approvedamount), 0) AS approved,
            ROUND(AVG(cd.approvedamount), 0) AS avg_cost
        FROM "{S}"."CLAIMS DATA" cd
        LEFT JOIN "{S}"."PROCEDURE DATA" pd ON cd.code = pd.procedurecode
        WHERE CAST(cd.encounterdatefrom AS DATE) >= DATE '{date(CY-2, 1, 1)}'
          AND cd.approvedamount > 0
          AND cd.code IS NOT NULL AND cd.code != ''
        GROUP BY 1, 2, 3 ORDER BY 3 DESC, 5 DESC
    """).fetchall()
    proc_map = defaultdict(lambda: {"name": "", "by_year": {}})
    for r in rows:
        proc_map[r[0]]["name"] = r[1]
        proc_map[r[0]]["by_year"][str(r[2])] = {
            "claims": r[3], "approved": int(r[4] or 0), "avg_cost": int(r[5] or 0)
        }
    sorted_proc = sorted(proc_map.items(),
                         key=lambda x: sum(v["approved"] for v in x[1]["by_year"].values()),
                         reverse=True)[:30]
    data["claims_by_procedure"] = [
        {"code": c, "name": v["name"], "by_year": v["by_year"]}
        for c, v in sorted_proc
    ]

    # ── PART 15: DATA QUALITY ─────────────────────────────────────────────────
    print("  [15/17] Data quality checks...")
    dq = {}

    # Auto-detect quantity column to correctly check approved > total charged
    qty_col_row = con.execute(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{S}'
          AND table_name = 'CLAIMS DATA'
          AND LOWER(column_name) IN ('quantity','qty','units','unit','unitquantity',
                                     'noofunits','numberofunits','unit_quantity')
        LIMIT 1
    """).fetchone()
    qty_col = qty_col_row[0] if qty_col_row else None

    if qty_col:
        r = con.execute(f"""
            SELECT COUNT(*),
                   ROUND(SUM(approvedamount - (chargeamount * CAST("{qty_col}" AS DOUBLE))), 0)
            FROM "{S}"."CLAIMS DATA"
            WHERE approvedamount > (chargeamount * CAST("{qty_col}" AS DOUBLE))
              AND approvedamount > 0
              AND CAST("{qty_col}" AS DOUBLE) > 0
              AND CAST(encounterdatefrom AS DATE) >= DATE '{date(CY-1, 1, 1)}'
        """).fetchone()
        dq_note = f"Correct check: approved > (chargeamount × {qty_col})."
        flag = "INVESTIGATE" if (r[0] or 0) > 100 else "OK"
    else:
        r = (0, 0)
        dq_note = ("No quantity column found. chargeamount meaning is ambiguous — "
                   "may be a unit rate OR total charge. Verify with operations before raising alert.")
        flag = "VERIFY_CHARGEAMOUNT_MEANING"

    dq["approved_exceeds_charged"] = {
        "count": int(r[0] or 0), "excess_amount": int(r[1] or 0),
        "qty_column_detected": qty_col or "none",
        "note": dq_note, "flag": flag
    }

    r = con.execute(f"""
        SELECT COUNT(*) FROM "{S}"."CLAIMS DATA"
        WHERE CAST(encounterdatefrom AS DATE) > DATE '{TODAY}'
    """).fetchone()
    dq["future_encounter_dates"] = {"count": r[0] or 0, "flag": "ERROR" if (r[0] or 0) > 0 else "OK"}

    r = con.execute(f"""
        SELECT COUNT(*) FROM "{S}"."CLAIMS DATA"
        WHERE CAST(datesubmitted AS DATE) < CAST(encounterdatefrom AS DATE)
          AND datesubmitted IS NOT NULL
    """).fetchone()
    dq["submitted_before_encounter"] = {"count": r[0] or 0, "flag": "ERROR" if (r[0] or 0) > 0 else "OK"}

    r = con.execute(f"""
        SELECT COUNT(DISTINCT cd.claimnumber), ROUND(SUM(cd.approvedamount), 0)
        FROM "{S}"."CLAIMS DATA" cd
        LEFT JOIN "{S}"."PROVIDERS" p
            ON TRY_CAST(cd.nhisproviderid AS BIGINT) = TRY_CAST(p.providerid AS BIGINT)
        WHERE p.providerid IS NULL AND cd.approvedamount > 0
          AND CAST(cd.encounterdatefrom AS DATE) >= DATE '{date(CY-1, 1, 1)}'
    """).fetchone()
    dq["claims_unmapped_provider"] = {
        "claim_count": r[0] or 0, "amount": int(r[1] or 0),
        "flag": "WARN" if (r[0] or 0) > 0 else "OK"
    }

    r = con.execute(f"""
        SELECT COUNT(*), ROUND(SUM(cd.approvedamount),0)
        FROM "{S}"."CLAIMS DATA" cd WHERE cd.diagnosiscode = 'A227'
    """).fetchone()
    dq["anthrax_coding_check"] = {
        "claim_count": r[0] or 0, "amount": int(r[1] or 0),
        "flag": "CODING_ERROR" if (r[0] or 0) > 5 else "OK",
        "note": "A227=anthrax sepsis — clinically implausible at scale, likely miscoded"
    }

    data["data_quality"] = dq

    # ── PART 16: MULTI-YEAR HISTORY ───────────────────────────────────────────
    print("  [16/17] Multi-year claims history...")
    rows = con.execute(f"""
        SELECT
            YEAR(CAST(encounterdatefrom AS DATE))  AS yr,
            COUNT(DISTINCT claimnumber)            AS claim_count,
            COUNT(DISTINCT enrollee_id)            AS claimants,
            ROUND(SUM(approvedamount), 0)           AS approved,
            ROUND(SUM(chargeamount), 0)             AS charged,
            ROUND(SUM(deniedamount), 0)             AS denied,
            SUM(CASE WHEN isinpatient THEN 1 ELSE 0 END) AS inpatient_count,
            ROUND(SUM(CASE WHEN isinpatient THEN approvedamount ELSE 0 END), 0) AS inpatient_cost
        FROM "{S}"."CLAIMS DATA"
        WHERE CAST(encounterdatefrom AS DATE) >= DATE '2020-01-01'
          AND approvedamount > 0
        GROUP BY 1 ORDER BY 1
    """).fetchall()
    data["claims_yearly"] = [
        {"year": r[0], "claim_count": r[1], "claimants": r[2],
         "approved": int(r[3] or 0), "charged": int(r[4] or 0), "denied": int(r[5] or 0),
         "inpatient_count": r[6], "inpatient_cost": int(r[7] or 0),
         "denial_rate_pct": round((r[5] or 0) / (r[4] or 1) * 100, 1),
         "inpatient_pct": round(r[6] / (r[1] or 1) * 100, 1)}
        for r in rows
    ]

    # ── PART 17: PA BY BENEFIT CODE ───────────────────────────────────────────
    print("  [17/17] PA by benefit code...")
    rows = con.execute(f"""
        SELECT
            p.benefitcode,
            COALESCE(bc.benefitcodedesc, p.benefitcode) AS benefit_name,
            COUNT(DISTINCT p.panumber)   AS pa_count,
            ROUND(SUM(p.granted), 0)     AS granted,
            ROUND(AVG(p.granted), 0)     AS avg_granted
        FROM "{S}"."PA DATA" p
        LEFT JOIN "{S}"."BENEFITCODES" bc
            ON CAST(p.benefitcode AS VARCHAR) = CAST(bc.benefitcodeid AS VARCHAR)
        WHERE CAST(p.requestdate AS DATE) >= DATE '{date(CY-1, 1, 1)}'
          AND p.benefitcode IS NOT NULL
        GROUP BY 1, 2 ORDER BY 4 DESC LIMIT 20
    """).fetchall()
    data["pa_by_benefit"] = [
        {"benefit_code": r[0], "benefit_name": r[1],
         "pa_count": r[2], "granted": int(r[3] or 0), "avg_granted": int(r[4] or 0)}
        for r in rows
    ]

    return data


# ══════════════════════════════════════════════════════════════════════════════
# PROMPT BUILDER
# ══════════════════════════════════════════════════════════════════════════════

def build_prompt(data: dict) -> str:

    # ── Pre-compute summary figures ────────────────────────────────────────────
    cy_enc       = [r for r in data["disease_burden_monthly"] if r["year"] == CY]
    py_enc       = [r for r in data["disease_burden_monthly"] if r["year"] == PY]
    cy_approved  = sum(r["approved"]       for r in cy_enc)
    py_approved  = sum(r["approved"]       for r in py_enc)
    cy_inpatient = sum(r["inpatient_cost"] for r in cy_enc)

    cy_outflow_all    = [r for r in data["payment_outflow"] if r["submit_year"] == CY]
    cy_outflow_total  = sum(r["approved"] for r in cy_outflow_all)
    cy_outflow_cy_svc = sum(r["approved"] for r in cy_outflow_all if r["encounter_year"] == CY)
    cy_outflow_py_svc = sum(r["approved"] for r in cy_outflow_all if r["encounter_year"] < CY)
    cy_tail_pct       = round(cy_outflow_py_svc / cy_outflow_total * 100, 1) if cy_outflow_total else 0

    pmpm_cy = data["annualised_pmpm"].get(CY, 0)
    pmpm_py = data["annualised_pmpm"].get(PY, 0)
    recent_pmpm_rows = sorted(data["pmpm_monthly"],
                              key=lambda r: (r["year"], r["month_num"]), reverse=True)[:3]
    pmpm_rolling_3m = round(sum(r["pmpm_cash"] for r in recent_pmpm_rows) / 3, 2) if recent_pmpm_rows else 0

    cp_cy = data["cost_pools"].get(CY, {})
    cp_py = data["cost_pools"].get(PY, {})
    total_pa_cy     = cp_cy.get("pool_a_pa_granted", 0)
    total_cl_cy     = cp_cy.get("pool_a_cl_approved", 0)
    total_vetted_cy = cp_cy.get("pool_a_recovered", 0)
    vetting_pct_cy  = cp_cy.get("pool_a_vetting_pct", 0)
    noauth_cy       = cp_cy.get("pool_b_noauth", 0)
    noauth_py       = cp_py.get("pool_b_noauth", 0)
    outstanding_pa  = cp_cy.get("pool_c_outstanding_pa", 0)
    total_paid_cy   = cp_cy.get("total_actual_claims_paid", 0)

    pa_cy       = [r for r in data["pa_monthly"] if r["year"] == CY]
    pa_py       = [r for r in data["pa_monthly"] if r["year"] == PY]
    pa_total_cy = sum(r["granted"] for r in pa_cy)
    pa_total_py = sum(r["granted"] for r in pa_py)
    pa_count_cy = sum(r["pa_count"] for r in pa_cy)

    enrolled = data["current_enrolled"]

    prompt = f"""You are a senior actuary and medical analytics director specialising in health insurance in Nigeria, with deep expertise in HMO operations and Nigerian NHIS regulations.

You are analysing data for **Clearline International Limited**, a Health Maintenance Organisation in Lagos, Nigeria.

Today is {TODAY}. This report covers **{REPORT_LABEL}**.

---
# FINANCIAL OBJECTIVE — READ THIS FIRST

Clearline's profitability depends on keeping MLR below 75%.

    MLR = Total Medical Cost / Premium Revenue × 100
    MLR < 65%  = Healthy    |    MLR 65–75% = Watch    |    MLR > 75% = Loss-making

**Every finding must link to: what is driving costs, is it controllable, and what should management do?**

---
# THREE COST POOLS — CRITICAL: NEVER MIX THESE

The total medical cost has three mutually exclusive components:

**Pool A — PA-Authorised & Vetted Claims** (panumber NOT NULL)
- PA granted = commitment ceiling
- Claims approved = what was actually paid after vetting
- Vetting recovery = PA granted − claims approved
- Source: vetting_monthly data

**Pool B — No-Auth Claims** (panumber IS NULL)
- No prior gate was applied
- Approved = what was paid with no ceiling
- Source: noauth_monthly data

**Pool C — Outstanding PA (IBNR)** (PA issued, no claim received yet)
- = Total PA granted − PA already matched to claims
- This is future liability still incoming
- Source: cost_pools data

**TOTAL TRUE COST = Pool A (claims approved) + Pool B (no-auth) + Pool C (IBNR estimate)**
**NEVER compare disease_burden_monthly totals to pa_monthly totals — they use different date columns and cover different populations.**

---
# DATA MODEL

- **encounterdatefrom** = when care was delivered → disease burden, utilisation
- **datesubmitted** = when provider billed Clearline → cash flow, payment pressure
- **requestdate (PA)** = when PA was requested → committed liability
- A 2025 visit can be billed in 2026 — this creates tail liability that inflates CY MLR
- panumber = one visit authorisation (one patient, one facility, one day)

---
# DATASET OVERVIEW

**Enrolled population:** {enrolled:,} members
**PMPM (annualised, cash-based):** {PY}: {fmt_n(pmpm_py)} | {CY}: {fmt_n(pmpm_cy)} | Rolling 3-month: {fmt_n(pmpm_rolling_3m)}

## PAYMENT OUTFLOW ({CY} cash by datesubmitted):
- Total paid in {CY} YTD: {fmt_m(cy_outflow_total)}
- For {CY} services: {fmt_m(cy_outflow_cy_svc)} ({round(cy_outflow_cy_svc/cy_outflow_total*100,1) if cy_outflow_total else 0}%)
- For PRIOR YEAR services (tail): {fmt_m(cy_outflow_py_svc)} ({cy_tail_pct}%)
{'⚠️ Tail is SIGNIFICANT — prior-year claims are inflating CY cash MLR.' if cy_tail_pct > 20 else ''}

## DISEASE BURDEN (by encounterdatefrom):
- {CY} YTD (care in {CY}): {fmt_m(cy_approved)} | Inpatient: {round(cy_inpatient/cy_approved*100,1) if cy_approved else 0}% of cost
- {PY} full year: {fmt_m(py_approved)}

## THREE COST POOLS — {CY} YTD:
- Pool A (PA-vetted claims paid): {fmt_m(total_cl_cy)} [PA authorised: {fmt_m(total_pa_cy)}, recovered by vetting: {fmt_m(total_vetted_cy)} ({vetting_pct_cy}%)]
- Pool B (no-auth, uncontrolled): {fmt_m(noauth_cy)}
- Pool C (PA issued, not yet claimed): {fmt_m(outstanding_pa)} [outstanding IBNR estimate]
- Total actual paid ({CY}): {fmt_m(total_paid_cy)}

## PA COMMITTED LIABILITY:
- {PY} PA granted: {fmt_m(pa_total_py)} | {CY} YTD PA granted: {fmt_m(pa_total_cy)}

---
# SECTION 1 — PAYMENT OUTFLOW (Cash MLR by datesubmitted)

{json.dumps(data["payment_outflow"], indent=2)}

---
# SECTION 2 — DISEASE BURDEN (Utilisation by encounterdatefrom)

{json.dumps(data["disease_burden_monthly"], indent=2)}

---
# SECTION 3 — CROSS-YEAR PAYMENT SPLIT

{json.dumps(data["cross_year_payments"], indent=2)}

---
# SECTION 4 — SUBMISSION RUNOUT (IBNR Pattern)

{json.dumps(data["submission_runout"], indent=2)}

---
# SECTION 5 — PA MONTHLY TRENDS

{json.dumps(data["pa_monthly"], indent=2)}

---
# SECTION 6 — CLAIMS VETTING EFFECTIVENESS (Pool A detail)

{json.dumps(data["vetting_monthly"], indent=2)}

---
# SECTION 6B — COST POOL SUMMARY (pre-computed — use these figures, do not recalculate)

{json.dumps(data["cost_pools"], indent=2)}

---
# SECTION 7 — NO-AUTH CLAIMS (Pool B)

Monthly trend:
{json.dumps(data["noauth_monthly"], indent=2)}

Top providers:
{json.dumps(data["noauth_by_provider"], indent=2)}

---
# SECTION 8 — DISEASE BURDEN FROM CLAIMS (top 40, multi-year)

Group ICD codes into clinical clusters (malaria variants → Malaria; hypertension/heart failure → Cardiovascular; UTI variants → Urinary Tract; etc.)
{json.dumps(data["claims_by_diagnosis"], indent=2)}

---
# SECTION 9 — DISEASE BURDEN FROM PA (top 50)

{json.dumps(data["pa_top_diagnoses"], indent=2)}

---
# SECTION 10A — TOP PROVIDERS BY PA (top 30, YoY)

{json.dumps(data["pa_by_provider"], indent=2)}

---
# SECTION 10B — TOP PROVIDERS BY CLAIMS (top 30, multi-year)

{json.dumps(data["claims_by_provider"], indent=2)}

---
# SECTION 11 — CLIENT GROUP ANALYSIS (top 30, YoY)

{json.dumps(data["pa_by_group"], indent=2)}

---
# SECTION 12 — INPATIENT DEEP DIVE (top 25)

{json.dumps(data["inpatient_by_diagnosis"], indent=2)}

---
# SECTION 13 — HIGH UTILISERS (>₦500K)

{json.dumps(data["high_utilisers"], indent=2)}

---
# SECTION 14 — TOP PROCEDURES (top 30)

{json.dumps(data["claims_by_procedure"], indent=2)}

---
# SECTION 15 — PA BY BENEFIT CODE (top 20)

{json.dumps(data["pa_by_benefit"], indent=2)}

---
# SECTION 16 — CLAIM FREQUENCY & UTILISATION

{json.dumps(data["claim_frequency"], indent=2)}

---
# SECTION 17 — MULTI-YEAR CLAIMS HISTORY (2020–{CY})

{json.dumps(data["claims_yearly"], indent=2)}

---
# SECTION 18 — DATA QUALITY FLAGS

{json.dumps(data["data_quality"], indent=2)}

---
# YOUR TASK

Produce a professional management report. Every finding must cite actual numbers. DO NOT be generic.

## 1. EXECUTIVE SUMMARY (6–10 bullets)
CEO-level findings with specific numbers and implications for MLR.

## 2. MLR & FINANCIAL POSITION

### 2a. Cash MLR (datesubmitted basis)
What is the cash MLR? What % of payments are prior-year tail? Adjusted MLR for current-year services only?

### 2b. Incurred MLR (encounterdatefrom basis)
True disease burden cost this year. YoY trend. Cost per member trend.

### 2c. IBNR & Outstanding Liability (Pool C)
Estimate outstanding liability. Which encounter months have worst submission runout?

## 3. COST CONTROL GATES

### 3a. Vetting Effectiveness (Pool A)
Monthly vetting recovery trend. Which months are weakest? Providers with suspiciously low vetting reduction?

### 3b. No-Auth Spend (Pool B)
Scale and trend. Top providers. Which diagnoses dominate? Any provider concentrating in no-auth (abuse signal)?

### 3c. PA Gate & PMPM Trend
PA volume trend month-by-month. Benefit codes driving admissions. PMPM trend interpretation.

## 4. DISEASE BURDEN

### 4a. Clinical Clusters
Group all diagnoses into clusters. Top 10 clusters by cost. What is rising? What is seasonal?

### 4b. PA vs Claims Disease Cross-Reference
Same diseases in both? Major discrepancies = no-auth or data gap.

### 4c. Inpatient Analysis
Top admission diagnoses, average cost, inpatient as % of total.

## 5. PROVIDER ANALYSIS

### 5a. Top 15 Providers (combined PA + claims)
YoY growth. Band distribution. Over-referral to high-band providers?

### 5b. Anomalies
High claims volume vs low PA (no-auth abuse?). High PA vs low claims (IBNR or data issue?).

## 6. CLIENT GROUP ANALYSIS
Top utilisers, highest CPE, fastest-growing groups, renewal repricing candidates.

## 7. HIGH UTILISER PROFILE
Profile the cohort. Key diagnoses. Group concentration. % of total cost. Case management recommendation.

## 8. SEASONALITY & 3-MONTH FORECAST
Seasonal patterns from multi-year data. Forecast PA and claims for next 3 months with risk factors.

## 9. COST REDUCTION RECOMMENDATIONS (priority-ordered, minimum 8)
For each:
- **Finding:** specific data point
- **Action:** named owner, specific step
- **Estimated impact:** ₦ or MLR points
- **Timeline:** immediate / this quarter / next renewal

## 10. DATA QUALITY REPORT
Explain each flag. Financial impact. What must be fixed before next report.

Write as a professional internal management report. Use ₦ for all monetary values. Name specific providers, groups, diagnoses — do not anonymise.
"""
    return prompt


# ══════════════════════════════════════════════════════════════════════════════
# CLAUDE API CALL
# ══════════════════════════════════════════════════════════════════════════════

def call_claude(prompt: str) -> str:
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    print("  Calling Claude Opus 4.6 (this may take 60–120 seconds)...")
    message = client.messages.create(
        model="claude-opus-4-6",
        max_tokens=8192,
        messages=[{"role": "user", "content": prompt}],
    )
    return message.content[0].text


# ══════════════════════════════════════════════════════════════════════════════
# KEY METRICS (frontend strip)
# ══════════════════════════════════════════════════════════════════════════════

def extract_key_metrics(data: dict) -> dict:
    enrolled = data["current_enrolled"]

    cy_enc       = [r for r in data["disease_burden_monthly"] if r["year"] == CY]
    py_enc       = [r for r in data["disease_burden_monthly"] if r["year"] == PY]
    cy_approved  = sum(r["approved"]       for r in cy_enc)
    py_approved  = sum(r["approved"]       for r in py_enc)
    cy_inpatient = sum(r["inpatient_cost"] for r in cy_enc)
    cy_claims    = sum(r["claim_count"]    for r in cy_enc)

    cp_cy = data["cost_pools"].get(CY, {})
    cp_py = data["cost_pools"].get(PY, {})

    pa_cy       = [r for r in data["pa_monthly"] if r["year"] == CY]
    pa_py       = [r for r in data["pa_monthly"] if r["year"] == PY]
    pa_total_cy = sum(r["granted"]  for r in pa_cy)
    pa_total_py = sum(r["granted"]  for r in pa_py)
    pa_count_cy = sum(r["pa_count"] for r in pa_cy)

    cy_yearly = next((r for r in data["claims_yearly"] if r["year"] == CY), {})

    top_dx   = data["pa_top_diagnoses"][:5]
    top_prov = data["pa_by_provider"][:5]
    top_grp  = data["pa_by_group"][:5]

    return {
        "report_period":             REPORT_LABEL,
        "enrolled":                  enrolled,
        "pmpm_cy":                   data["annualised_pmpm"].get(CY, 0),
        "pmpm_py":                   data["annualised_pmpm"].get(PY, 0),
        # PA
        "pa_total_granted_2025":     int(pa_total_py),
        "pa_total_granted_2026_ytd": int(pa_total_cy),
        "pa_count_2026_ytd":         int(pa_count_cy),
        # Claims (disease burden view)
        "claims_approved_2025":      int(py_approved),
        "claims_approved_2026_ytd":  int(cy_approved),
        "claims_count_2026_ytd":     int(cy_claims),
        "inpatient_pct_2026":        cy_yearly.get("inpatient_pct", 0),
        "denial_rate_2026":          cy_yearly.get("denial_rate_pct", 0),
        # Vetting (Pool A)
        "vetting_pa_granted_cy":     cp_cy.get("pool_a_pa_granted", 0),
        "vetting_cl_approved_cy":    cp_cy.get("pool_a_cl_approved", 0),
        "vetting_recovered_cy":      cp_cy.get("pool_a_recovered", 0),
        "vetting_reduction_pct":     cp_cy.get("pool_a_vetting_pct", 0),
        # No-auth (Pool B)
        "noauth_approved_cy":        cp_cy.get("pool_b_noauth", 0),
        "noauth_approved_py":        cp_py.get("pool_b_noauth", 0),
        # IBNR (Pool C)
        "outstanding_pa_cy":         cp_cy.get("pool_c_outstanding_pa", 0),
        # Top items
        "top_5_pa_diagnoses":        [d["description"] for d in top_dx],
        "top_5_pa_providers":        [p["name"] for p in top_prov],
        "top_5_pa_groups":           [g["group"] for g in top_grp],
    }


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    print(f"[{TODAY}] Monthly AI Report — {REPORT_LABEL}")
    con = duckdb.connect(f"md:?motherduck_token={MOTHERDUCK_TOKEN}")
    sb  = create_client(SUPABASE_URL, SUPABASE_KEY)

    sb.table("ai_monthly_reports").upsert([{
        "report_period": REPORT_LABEL,
        "status":        "running",
        "triggered_at":  str(TODAY),
    }], on_conflict="report_period").execute()

    try:
        print("Collecting data from MotherDuck...")
        data = collect_data(con)
        con.close()

        print("Building prompt...")
        prompt = build_prompt(data)
        print(f"  Prompt length: {len(prompt):,} characters")

        print("Generating AI report...")
        report_md = call_claude(prompt)
        print(f"  Report length: {len(report_md):,} characters")

        metrics = extract_key_metrics(data)

        print("Saving to Supabase...")
        sb.table("ai_monthly_reports").upsert([{
            "report_period":   REPORT_LABEL,
            "status":          "complete",
            "report_markdown": report_md,
            "key_metrics":     json.dumps(metrics),
            "triggered_at":    str(TODAY),
            "generated_at":    str(TODAY),
        }], on_conflict="report_period").execute()

        print(f"[{TODAY}] Report complete — {len(report_md):,} characters saved.")

    except Exception as e:
        sb.table("ai_monthly_reports").upsert([{
            "report_period": REPORT_LABEL,
            "status":        f"error: {str(e)[:500]}",
            "triggered_at":  str(TODAY),
        }], on_conflict="report_period").execute()
        raise


if __name__ == "__main__":
    main()
