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
from datetime import date
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

def fmt_n(n):   return f"₦{n:,.0f}"
def fmt_m(n):   return f"₦{n/1_000_000:.1f}M"
def pct(a, b):  return f"{a/b*100:.1f}%" if b else "—"
def safe_div(a, b): return round(a/b, 4) if b else 0.0


# ══════════════════════════════════════════════════════════════════════════════
# DATA COLLECTION
# ══════════════════════════════════════════════════════════════════════════════

def collect_data(con) -> dict:
    """
    Run all analytical queries and return a structured dict.

    Guiding principle: every query must answer a specific business question,
    not just produce a number. The question is documented in each section.
    """
    data = {}

    # ──────────────────────────────────────────────────────────────────────────
    # PART 0: ENROLLMENT — The Denominator for Everything
    # Business question: How many lives are we responsible for each month?
    # ──────────────────────────────────────────────────────────────────────────
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

    # ──────────────────────────────────────────────────────────────────────────
    # PART 1A: PAYMENT OUTFLOW — Actual cash leaving Clearline by datesubmitted
    # ──────────────────────────────────────────────────────────────────────────
    print("  [1a/17] Payment outflow by submission date (actual cash flow)...")
    rows = con.execute(f"""
        SELECT
            YEAR(CAST(datesubmitted AS DATE))    AS submit_yr,
            MONTH(CAST(datesubmitted AS DATE))   AS submit_mo,
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

    # ──────────────────────────────────────────────────────────────────────────
    # PART 1B: DISEASE BURDEN — Care delivered each month by encounter date
    # ──────────────────────────────────────────────────────────────────────────
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

    # ──────────────────────────────────────────────────────────────────────────
    # PART 2: IBNR — Submission runout and cross-year payment split
    # ──────────────────────────────────────────────────────────────────────────
    print("  [2/17] IBNR & outstanding liability analysis...")
    rows = con.execute(f"""
        SELECT
            YEAR(CAST(encounterdatefrom AS DATE))    AS enc_yr,
            MONTH(CAST(encounterdatefrom AS DATE))   AS enc_mo,
            DATEDIFF('day',
                CAST(encounterdatefrom AS DATE),
                CAST(datesubmitted AS DATE))          AS lag_days,
            COUNT(DISTINCT claimnumber)              AS claims,
            ROUND(SUM(approvedamount), 0)             AS approved
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
            YEAR(CAST(datesubmitted    AS DATE)) AS submit_yr,
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

    # ──────────────────────────────────────────────────────────────────────────
    # PART 3: PMPM — Per Member Per Month
    # ──────────────────────────────────────────────────────────────────────────
    print("  [3/17] PMPM analysis (Per Member Per Month)...")
    rows = con.execute(f"""
        WITH monthly_claims AS (
            SELECT
                YEAR(CAST(encounterdatefrom AS DATE))  AS yr,
                MONTH(CAST(encounterdatefrom AS DATE)) AS mo,
                ROUND(SUM(approvedamount), 0)           AS total_approved,
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
            mc.total_approved, mc.claimants, mc.claim_count,
            pa.pa_count, pa.pa_granted, pa.pa_requested
        FROM monthly_claims mc
        LEFT JOIN monthly_pa pa ON mc.yr = pa.yr AND mc.mo = pa.mo
        ORDER BY mc.yr, mc.mo
    """).fetchall()
    data["pmpm_monthly"] = [
        {
            "year": r[0], "month": MONTH_LABELS[r[1]-1], "month_num": r[1],
            "claims_approved": int(r[2] or 0),
            "claimants": r[3], "claim_count": r[4],
            "pa_count": r[5] or 0,
            "pa_granted": int(r[6] or 0),
            "pa_requested": int(r[7] or 0),
            "pa_approval_pct": round((r[6] or 0) / (r[7] or 1) * 100, 1),
        }
        for r in rows
    ]

    # ──────────────────────────────────────────────────────────────────────────
    # PART 4: CLAIM FREQUENCY & UTILISATION RATE
    # ──────────────────────────────────────────────────────────────────────────
    print("  [4/17] Claim frequency & utilisation rate...")
    rows = con.execute(f"""
        SELECT
            YEAR(CAST(encounterdatefrom AS DATE)) AS yr,
            COUNT(DISTINCT claimnumber)           AS total_claims,
            COUNT(DISTINCT enrollee_id)           AS claimants
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
            "total_claims": r[1], "claimants": r[2],
            "enrolled": enrolled_map.get(r[0], current_enrolled),
            "claims_per_1000_members": round(r[1] / enrolled_map.get(r[0], current_enrolled) * 1000, 1),
            "utilisation_rate_pct": round(r[2] / enrolled_map.get(r[0], current_enrolled) * 100, 1),
            "avg_claims_per_claimant": round(r[1] / r[2], 1) if r[2] else 0,
        }
        for r in rows
    ]

    # ──────────────────────────────────────────────────────────────────────────
    # PART 5: PA ANALYSIS — Committed Liability
    # ──────────────────────────────────────────────────────────────────────────
    print("  [5/17] PA volume & committed liability...")
    rows = con.execute(f"""
        SELECT
            YEAR(CAST(requestdate AS DATE))   AS yr,
            MONTH(CAST(requestdate AS DATE))  AS mo,
            COUNT(DISTINCT panumber)           AS pa_count,
            ROUND(SUM(requested), 0)           AS total_requested,
            ROUND(SUM(granted), 0)             AS total_granted,
            ROUND(SUM(requested) - SUM(granted), 0) AS pa_reduction,
            COUNT(DISTINCT groupname)          AS groups,
            COUNT(DISTINCT providerid)         AS providers,
            COUNT(DISTINCT IID)                AS unique_enrollees
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

    # ──────────────────────────────────────────────────────────────────────────
    # PART 6: CLAIMS VETTING EFFECTIVENESS
    # ──────────────────────────────────────────────────────────────────────────
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

    # ──────────────────────────────────────────────────────────────────────────
    # PART 7: NO-AUTH CLAIMS (Uncontrolled Spend)
    # ──────────────────────────────────────────────────────────────────────────
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

    # ──────────────────────────────────────────────────────────────────────────
    # PART 8 & 9: DISEASE BURDEN — Claims and PA diagnoses
    # ──────────────────────────────────────────────────────────────────────────
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

    # ──────────────────────────────────────────────────────────────────────────
    # PART 10: TOP PROVIDERS — PA and Claims
    # ──────────────────────────────────────────────────────────────────────────
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

    # ──────────────────────────────────────────────────────────────────────────
    # PART 11: CLIENT GROUP ANALYSIS
    # ──────────────────────────────────────────────────────────────────────────
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

    # ──────────────────────────────────────────────────────────────────────────
    # PART 12: INPATIENT DEEP DIVE
    # ──────────────────────────────────────────────────────────────────────────
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

    # ──────────────────────────────────────────────────────────────────────────
    # PART 13: HIGH UTILISERS (>₦500K)
    # ──────────────────────────────────────────────────────────────────────────
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

    # ──────────────────────────────────────────────────────────────────────────
    # PART 14: PROCEDURE ANALYSIS
    # ──────────────────────────────────────────────────────────────────────────
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

    # ──────────────────────────────────────────────────────────────────────────
    # PART 15: DATA QUALITY FLAGS
    # ──────────────────────────────────────────────────────────────────────────
    print("  [15/17] Data quality checks...")
    dq = {}

    r = con.execute(f"""
        SELECT COUNT(*), SUM(approvedamount - chargeamount)
        FROM "{S}"."CLAIMS DATA"
        WHERE approvedamount > chargeamount AND approvedamount > 0
          AND CAST(encounterdatefrom AS DATE) >= DATE '{date(CY-1, 1, 1)}'
    """).fetchone()
    dq["approved_exceeds_charged"] = {
        "count": r[0] or 0, "excess_amount": int(r[1] or 0),
        "flag": "INVESTIGATE" if (r[0] or 0) > 100 else "OK"
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
        WHERE p.providerid IS NULL
          AND cd.approvedamount > 0
          AND CAST(cd.encounterdatefrom AS DATE) >= DATE '{date(CY-1, 1, 1)}'
    """).fetchone()
    dq["claims_unmapped_provider"] = {
        "claim_count": r[0] or 0, "amount": int(r[1] or 0),
        "flag": "WARN" if (r[0] or 0) > 0 else "OK"
    }

    data["data_quality"] = dq

    # ──────────────────────────────────────────────────────────────────────────
    # PART 16: MULTI-YEAR CLAIMS HISTORY
    # ──────────────────────────────────────────────────────────────────────────
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

    # ──────────────────────────────────────────────────────────────────────────
    # PART 17: PA BY BENEFIT CODE
    # ──────────────────────────────────────────────────────────────────────────
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
    cy_enc  = [r for r in data["disease_burden_monthly"] if r["year"] == CY]
    py_enc  = [r for r in data["disease_burden_monthly"] if r["year"] == PY]
    cy_approved  = sum(r["approved"]      for r in cy_enc)
    py_approved  = sum(r["approved"]      for r in py_enc)
    cy_claimants = sum(r["claimants"]     for r in cy_enc)
    cy_inpatient = sum(r["inpatient_cost"] for r in cy_enc)

    cy_outflow_all    = [r for r in data["payment_outflow"] if r["submit_year"] == CY]
    cy_outflow_total  = sum(r["approved"] for r in cy_outflow_all)
    cy_outflow_cy_svc = sum(r["approved"] for r in cy_outflow_all if r["encounter_year"] == CY)
    cy_outflow_py_svc = sum(r["approved"] for r in cy_outflow_all if r["encounter_year"] < CY)
    cy_tail_pct       = round(cy_outflow_py_svc / cy_outflow_total * 100, 1) if cy_outflow_total else 0

    vetting_cy      = [r for r in data["vetting_monthly"] if r["year"] == CY]
    total_pa_cy     = sum(r["pa_granted"]               for r in vetting_cy)
    total_cl_cy     = sum(r["cl_approved"]              for r in vetting_cy)
    total_vetted_cy = sum(r["amount_recovered_by_vetting"] for r in vetting_cy)

    noauth_cy = sum(r["approved"] for r in data["noauth_monthly"] if r["year"] == CY)
    noauth_py = sum(r["approved"] for r in data["noauth_monthly"] if r["year"] == PY)

    pa_cy       = [r for r in data["pa_monthly"] if r["year"] == CY]
    pa_py       = [r for r in data["pa_monthly"] if r["year"] == PY]
    pa_total_cy = sum(r["granted"] for r in pa_cy)
    pa_total_py = sum(r["granted"] for r in pa_py)

    enrolled = data["current_enrolled"]
    recent_months = sorted(data["disease_burden_monthly"],
                           key=lambda r: (r["year"], r["month_num"]), reverse=True)[:3]
    pmpm_recent = sum(r["approved"] for r in recent_months) / 3 / max(enrolled, 1)

    prompt = f"""You are a senior actuary and medical analytics director specialising in health insurance in Nigeria, with deep knowledge of HMO operations, Nigerian NHIS regulations, and international actuarial standards.

You are analysing data for **Clearline International Limited**, a Health Maintenance Organisation in Lagos, Nigeria.

Today is {TODAY}. This report covers **{REPORT_LABEL}**.

---
# FINANCIAL OBJECTIVE — READ THIS FIRST

Clearline's profitability depends entirely on keeping the Medical Loss Ratio (MLR) below 75%.

    MLR = Total Medical Cost (Claims Paid + Unclaimed PA) / Premium Revenue × 100
    MLR < 65%  = Healthy
    MLR 65–75% = Watch closely
    MLR 75–85% = Loss-making — intervention required
    MLR > 85%  = Critical — immediate action

**Your entire analysis must be framed around this objective: identifying what is driving costs, what is controllable, what is structural, and what specific actions Clearline can take.**

---
# DATA MODEL — READ CAREFULLY

## Two date columns serve completely different purposes:

### encounterdatefrom → "What care did our members receive?" (CLINICAL picture)
- When the patient visited the hospital → use for disease burden, utilisation, provider behaviour

### datesubmitted → "What cash are we paying out?" (FINANCIAL picture)
- When the provider billed Clearline → use for cash flow, outstanding liability, financial planning

**A 2025 visit can be billed in 2026 — this creates "tail liability" that inflates the apparent CY MLR.**

## PA System (Pre-Authorisation)
- panumber = one visit authorisation (one patient, one facility, one day)
- PA granted = the ceiling Clearline committed to pay — NOT actual payment
- PA data starts 2025. Claims data starts 2020.
- Two cost control gates: (1) PA Gate at authorisation, (2) Claims Vetting Gate at billing

## No-Auth Claims (NI Auth)
- panumber IS NULL → care delivered without prior authorisation
- Legitimate for emergencies; represents UNCONTROLLED spend with no pre-gate

---
# DATASET OVERVIEW

**Enrolled population (current):** {enrolled:,} members
**PMPM (last 3 months avg):** {fmt_n(pmpm_recent)}

## PAYMENT OUTFLOW ({CY} cash payments by datesubmitted):
- Total paid in {CY} YTD: {fmt_m(cy_outflow_total)}
- For {CY} services: {fmt_m(cy_outflow_cy_svc)} ({round(cy_outflow_cy_svc/cy_outflow_total*100,1) if cy_outflow_total else 0}%)
- For PRIOR YEAR services (tail): {fmt_m(cy_outflow_py_svc)} ({cy_tail_pct}%)
{'⚠️ This tail is SIGNIFICANT — prior-year liability is inflating the apparent ' + str(CY) + ' MLR.' if cy_tail_pct > 20 else ''}

## DISEASE BURDEN (by encounterdatefrom):
- {CY} YTD claims (care in {CY}): {fmt_m(cy_approved)} | {cy_claimants:,} claimants
- {PY} full year (care in {PY}): {fmt_m(py_approved)}
- Inpatient as % of {CY} cost: {round(cy_inpatient/cy_approved*100,1) if cy_approved else 0}%

## PA COMMITTED LIABILITY:
- {PY} PA granted: {fmt_m(pa_total_py)}
- {CY} YTD PA granted: {fmt_m(pa_total_cy)}

## CLAIMS VETTING (matched PA→Claims):
- {CY} YTD PA granted (matched): {fmt_m(total_pa_cy)}
- {CY} YTD claims approved (matched): {fmt_m(total_cl_cy)}
- Recovered by vetting: {fmt_m(total_vetted_cy)} ({round(total_vetted_cy/total_pa_cy*100,1) if total_pa_cy else 0}% reduction)

## NO-AUTH SPEND (no PA gate applied):
- {PY}: {fmt_m(noauth_py)}
- {CY} YTD: {fmt_m(noauth_cy)}

---
# SECTION 1 — PAYMENT OUTFLOW (Cash MLR by datesubmitted)

What cash actually left Clearline accounts each month, broken down by which year the care happened.
{json.dumps(data["payment_outflow"], indent=2)}

---
# SECTION 2 — DISEASE BURDEN (Utilisation by encounterdatefrom)

What care was actually delivered each month — the clinical picture.
{json.dumps(data["disease_burden_monthly"], indent=2)}

---
# SECTION 3 — CROSS-YEAR PAYMENT SPLIT (Tail Liability)

How much of each submission year's payments are for prior-year services.
{json.dumps(data["cross_year_payments"], indent=2)}

---
# SECTION 4 — SUBMISSION RUNOUT (IBNR Pattern)

For each encounter month, how quickly are claims being submitted. This reveals outstanding liability.
{json.dumps(data["submission_runout"], indent=2)}

---
# SECTION 5 — PA MONTHLY TRENDS (Committed Liability)

PA granted is the forward indicator — this month's PA becomes next month's claims pressure.
{json.dumps(data["pa_monthly"], indent=2)}

---
# SECTION 6 — CLAIMS VETTING EFFECTIVENESS

For matched PA→Claims panumbers: how much of authorised liability did the claims team recover?
{json.dumps(data["vetting_monthly"], indent=2)}

---
# SECTION 7 — NO-AUTH (NI AUTH) CLAIMS

Spend with no prior gate applied. Growing no-auth = weakening utilisation management.
Monthly trend:
{json.dumps(data["noauth_monthly"], indent=2)}

Top providers (no-auth):
{json.dumps(data["noauth_by_provider"], indent=2)}

---
# SECTION 8 — DISEASE BURDEN FROM CLAIMS (top 40 diagnoses, multi-year)

Apply ICD medical clustering — group related diagnoses into clinical categories.
{json.dumps(data["claims_by_diagnosis"], indent=2)}

---
# SECTION 9 — DISEASE BURDEN FROM PA (top 50 diagnoses)

Diagnoses attached to PA requests — the authorised disease burden picture.
{json.dumps(data["pa_top_diagnoses"], indent=2)}

---
# SECTION 10A — TOP PROVIDERS BY PA (top 30, YoY)

{json.dumps(data["pa_by_provider"], indent=2)}

---
# SECTION 10B — TOP PROVIDERS BY CLAIMS (top 30, multi-year)

{json.dumps(data["claims_by_provider"], indent=2)}

---
# SECTION 11 — CLIENT GROUP ANALYSIS (top 30 by PA granted, YoY)

{json.dumps(data["pa_by_group"], indent=2)}

---
# SECTION 12 — INPATIENT DEEP DIVE (top 25 diagnoses, multi-year)

{json.dumps(data["inpatient_by_diagnosis"], indent=2)}

---
# SECTION 13 — HIGH UTILISERS (>₦500K annual claims)

{json.dumps(data["high_utilisers"], indent=2)}

---
# SECTION 14 — TOP PROCEDURES BY COST (top 30, multi-year)

{json.dumps(data["claims_by_procedure"], indent=2)}

---
# SECTION 15 — PA BY BENEFIT CODE (top 20)

{json.dumps(data["pa_by_benefit"], indent=2)}

---
# SECTION 16 — CLAIM FREQUENCY & UTILISATION RATE

{json.dumps(data["claim_frequency"], indent=2)}

---
# SECTION 17 — MULTI-YEAR CLAIMS HISTORY (2020–{CY})

{json.dumps(data["claims_yearly"], indent=2)}

---
# SECTION 18 — DATA QUALITY FLAGS

{json.dumps(data["data_quality"], indent=2)}

---
# YOUR TASK

Produce a comprehensive medical analytics management report. Be specific — every finding must cite actual numbers from the data. DO NOT be generic.

## 1. EXECUTIVE SUMMARY (6–10 bullet points)
Key headline findings the CEO needs to know immediately, each with a specific number and implication.

## 2. MLR & FINANCIAL POSITION

### 2a. Cash MLR Analysis (by datesubmitted)
What is the cash MLR? What proportion of current year payments are tail claims from prior years? What does this mean for true profitability?

### 2b. True Incurred MLR (by encounterdatefrom)
What is the actual disease burden cost for care delivered this year? How does it compare to the prior year? Is cost per member rising or falling?

### 2c. IBNR & Outstanding Liability
Based on the submission runout pattern, how much {CY} liability has NOT yet been billed? Estimate the IBNR tail. Which months have the worst submission lag?

## 3. COST CONTROL GATE ANALYSIS

### 3a. Claims Vetting Effectiveness
Is the claims team recovering meaningful amounts vs PA grants? Which months show the weakest vetting? Flag providers where PA-to-claims gap is suspiciously low (rubber-stamping risk).

### 3b. No-Auth (NI Auth) Analysis
What is the scale and trend of uncontrolled spend? Which providers are driving it? Which diagnoses dominate? Is any provider abusing the no-auth pathway (high volume + low denial rate)?

### 3c. PA Gate Analysis
Month-by-month trend of PA granted — is Clearline's committed liability rising? What benefit codes are driving admissions?

## 4. DISEASE BURDEN ANALYSIS

### 4a. Clinical Clusters (ICD Grouping)
Group all claim diagnoses into clinical clusters using ICD medical knowledge (e.g., all malaria variants → Malaria; hypertension + heart failure → Cardiovascular; UTI variants → Urinary Tract). Rank top 10 clusters by cost. What is rising? What is seasonal?

### 4b. PA vs Claims Disease Comparison
Do the same diseases appear in both PA diagnoses and claims diagnoses? Flag any major discrepancies — a disease appearing heavily in claims but not PA may indicate no-auth abuse.

### 4c. Inpatient Deep Dive
What is driving admissions? Which diagnoses have highest average admission cost? What proportion of total spend is inpatient?

## 5. PROVIDER ANALYSIS

### 5a. Top Provider Ranking
Rank top 15 providers by combined PA + claims cost. Flag YoY growth >30%. Are Band A/B providers receiving disproportionate referrals (cost efficiency concern)?

### 5b. Provider Anomalies
Identify providers where claims volume is dramatically higher than PA volume (potential fraud/no-auth abuse) or vice versa (PA not converting to claims — possible cancelled care or double-counting).

## 6. CLIENT GROUP ANALYSIS
Which groups are the highest utilisers? Which have the highest cost-per-enrollee (CPE)? Which groups have grown most year-over-year? Which groups need renewal repricing?

## 7. HIGH UTILISER PROFILE
Describe the high-utiliser cohort. What diagnoses do they carry? Are they concentrated in specific groups? What proportion of total cost do they represent? Recommend a case management approach.

## 8. SEASONALITY & FORECAST
Identify seasonal patterns from multi-year data. Which months consistently spike? Based on all trends, forecast PA and claims cost for the next 3 months. What are the key risk events to watch?

## 9. COST REDUCTION RECOMMENDATIONS (priority-ordered)
For each recommendation:
- **Finding:** what the data shows specifically
- **Action:** specific management action with owner
- **Estimated impact:** quantified ₦ saving or MLR point improvement
- **Timeline:** immediate / this quarter / next renewal cycle

Provide at least 8 recommendations covering: provider network management, claims vetting improvement, no-auth controls, disease management programs, high-utiliser case management, group-level interventions, and benefit design.

## 10. DATA QUALITY REPORT
Explain each data quality flag found. Quantify the financial impact of any data issues. What must be fixed before next month's report?

Write as a professional management report. Use ₦ for all monetary values. Name specific providers, groups, and diagnoses — do not anonymise. This is an internal Clearline management report.
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
# KEY METRICS (for frontend strip display)
# ══════════════════════════════════════════════════════════════════════════════

def extract_key_metrics(data: dict) -> dict:
    """Pull headline numbers for the frontend MetricsStrip component."""
    enrolled = data["current_enrolled"]

    # Disease burden totals
    cy_enc = [r for r in data["disease_burden_monthly"] if r["year"] == CY]
    py_enc = [r for r in data["disease_burden_monthly"] if r["year"] == PY]
    cy_approved  = sum(r["approved"]      for r in cy_enc)
    py_approved  = sum(r["approved"]      for r in py_enc)
    cy_inpatient = sum(r["inpatient_cost"] for r in cy_enc)
    cy_claims    = sum(r["claim_count"]   for r in cy_enc)

    # Vetting
    vetting_cy  = [r for r in data["vetting_monthly"] if r["year"] == CY]
    total_pa_cy = sum(r["pa_granted"]               for r in vetting_cy)
    total_cl_cy = sum(r["cl_approved"]              for r in vetting_cy)
    vetted_cy   = sum(r["amount_recovered_by_vetting"] for r in vetting_cy)

    # No-auth
    noauth_cy = sum(r["approved"] for r in data["noauth_monthly"] if r["year"] == CY)

    # PA
    pa_cy       = [r for r in data["pa_monthly"] if r["year"] == CY]
    pa_py       = [r for r in data["pa_monthly"] if r["year"] == PY]
    pa_total_cy = sum(r["granted"] for r in pa_cy)
    pa_total_py = sum(r["granted"] for r in pa_py)
    pa_count_cy = sum(r["pa_count"] for r in pa_cy)

    # PMPM
    recent = sorted(data["disease_burden_monthly"],
                    key=lambda r: (r["year"], r["month_num"]), reverse=True)[:3]
    pmpm = sum(r["approved"] for r in recent) / 3 / max(enrolled, 1)

    # Denial rate (latest year from claims_yearly)
    cy_yearly = next((r for r in data["claims_yearly"] if r["year"] == CY), {})
    denial_rate = cy_yearly.get("denial_rate_pct", 0)
    inpatient_pct = cy_yearly.get("inpatient_pct", 0)

    # Top items
    top_dx   = data["pa_top_diagnoses"][:5]
    top_prov = data["pa_by_provider"][:5]
    top_grp  = data["pa_by_group"][:5]

    return {
        "report_period":             REPORT_LABEL,
        "enrolled":                  enrolled,
        "pmpm":                      int(pmpm),
        # PA
        "pa_total_granted_2025":     int(pa_total_py),
        "pa_total_granted_2026_ytd": int(pa_total_cy),
        "pa_count_2026_ytd":         int(pa_count_cy),
        # Claims
        "claims_approved_2025":      int(py_approved),
        "claims_approved_2026_ytd":  int(cy_approved),
        "claims_count_2026_ytd":     int(cy_claims),
        "inpatient_pct_2026":        inpatient_pct,
        "denial_rate_2026":          denial_rate,
        # Vetting
        "vetting_pa_granted_cy":     int(total_pa_cy),
        "vetting_cl_approved_cy":    int(total_cl_cy),
        "vetting_recovered_cy":      int(vetted_cy),
        "vetting_reduction_pct":     round(vetted_cy / total_pa_cy * 100, 1) if total_pa_cy else 0,
        # No-auth
        "noauth_approved_cy":        int(noauth_cy),
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

    # Mark as running
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
