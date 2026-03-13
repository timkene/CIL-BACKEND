"""
Monthly AI Medical Analytics Report
=====================================
Runs on the last day of every month (or on-demand).
Queries all dimensions of PA, Claims, Provider, Client, Enrollee, Procedure,
and Diagnosis data — then feeds everything to Claude Opus 4.6 for deep medical
analysis and actionable cost-reduction recommendations.

Supabase table: ai_monthly_reports

Run locally:
  MOTHERDUCK_TOKEN=... SUPABASE_URL=... SUPABASE_KEY=... ANTHROPIC_API_KEY=... \
  python scripts/monthly_ai_report.py
"""

import os, json
from datetime import date, timedelta
from collections import defaultdict

import duckdb
import anthropic
from supabase import create_client

# ── Config ─────────────────────────────────────────────────────────────────────
MOTHERDUCK_TOKEN = os.environ["MOTHERDUCK_TOKEN"]
SUPABASE_URL     = os.environ["SUPABASE_URL"]
SUPABASE_KEY     = os.environ["SUPABASE_KEY"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
S = "AI DRIVEN DATA"

TODAY      = date.today()
CY         = TODAY.year
PY         = TODAY.year - 1
PPY        = TODAY.year - 2
# Report covers the just-completed month (or current month if run mid-month)
REPORT_MONTH = TODAY.month if TODAY.day > 20 else (TODAY.month - 1 or 12)
REPORT_YEAR  = CY if REPORT_MONTH <= TODAY.month else PY
REPORT_LABEL = f"{REPORT_YEAR}-{REPORT_MONTH:02d}"

MONTH_LABELS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

def fmt_n(n): return f"₦{n:,.0f}"
def fmt_m(n): return f"₦{n/1_000_000:.1f}M"
def pct(a, b): return f"{a/b*100:.1f}%" if b else "—"


def collect_data(con):
    """Run all analytical queries and return a structured dict of findings."""
    data = {}

    print("  [1/15] PA overview...")
    rows = con.execute(f"""
        SELECT
            YEAR(CAST(requestdate AS DATE))  AS yr,
            MONTH(CAST(requestdate AS DATE)) AS mo,
            COUNT(DISTINCT panumber)         AS pa_count,
            ROUND(SUM(requested), 0)         AS total_requested,
            ROUND(SUM(granted), 0)           AS total_granted,
            COUNT(DISTINCT groupname)        AS groups,
            COUNT(DISTINCT providerid)       AS providers
        FROM "{S}"."PA DATA"
        WHERE CAST(requestdate AS DATE) >= DATE '2025-01-01'
        GROUP BY 1, 2 ORDER BY 1, 2
    """).fetchall()
    data["pa_monthly"] = [
        {"year": r[0], "month": MONTH_LABELS[r[1]-1], "pa_count": r[2],
         "requested": int(r[3] or 0), "granted": int(r[4] or 0),
         "approval_rate": round((r[4] or 0)/(r[3] or 1)*100, 1),
         "groups": r[5], "providers": r[6]}
        for r in rows
    ]

    print("  [2/15] PA by benefit code...")
    rows = con.execute(f"""
        SELECT
            p.benefitcode,
            COALESCE(bc.benefitcodename, p.benefitcode) AS benefit_name,
            COUNT(DISTINCT p.panumber)  AS pa_count,
            ROUND(SUM(p.granted), 0)    AS total_granted,
            ROUND(AVG(p.granted), 0)    AS avg_granted
        FROM "{S}"."PA DATA" p
        LEFT JOIN "{S}"."BENEFITCODES" bc ON p.benefitcode = bc.benefitcodeid::VARCHAR
        WHERE CAST(p.requestdate AS DATE) >= DATE '2025-01-01'
          AND p.benefitcode IS NOT NULL
        GROUP BY 1, 2 ORDER BY 4 DESC LIMIT 20
    """).fetchall()
    data["pa_by_benefit"] = [
        {"benefit_code": r[0], "benefit_name": r[1], "pa_count": r[2],
         "total_granted": int(r[3] or 0), "avg_granted": int(r[4] or 0)}
        for r in rows
    ]

    print("  [3/15] PA by provider (top 30)...")
    rows = con.execute(f"""
        SELECT
            p.providerid,
            COALESCE(pr.providername, '(Unknown)') AS provider_name,
            pr.bands,
            YEAR(CAST(p.requestdate AS DATE))      AS yr,
            COUNT(DISTINCT p.panumber)             AS pa_count,
            ROUND(SUM(p.granted), 0)               AS total_granted,
            COUNT(DISTINCT p.IID)                  AS unique_enrollees
        FROM "{S}"."PA DATA" p
        LEFT JOIN "{S}"."PROVIDERS" pr
            ON TRY_CAST(p.providerid AS BIGINT) = TRY_CAST(pr.providerid AS BIGINT)
        WHERE CAST(p.requestdate AS DATE) >= DATE '2025-01-01'
        GROUP BY 1, 2, 3, 4
        ORDER BY 4, 6 DESC
    """).fetchall()
    # Build YoY comparison per provider
    prov_map = defaultdict(lambda: {"name": "", "band": "", "2025": {}, "2026": {}})
    for r in rows:
        pid = r[0]
        prov_map[pid]["name"] = r[1]
        prov_map[pid]["band"] = r[2] or "—"
        prov_map[pid][str(r[3])] = {"pa_count": r[4], "granted": int(r[5] or 0), "enrollees": r[6]}
    # Take top 30 by 2025 spend
    sorted_provs = sorted(prov_map.items(),
                          key=lambda x: x[1].get("2025", {}).get("granted", 0) +
                                        x[1].get("2026", {}).get("granted", 0), reverse=True)[:30]
    data["pa_by_provider"] = [
        {"provider_id": pid, "name": v["name"], "band": v["band"],
         "2025_pa": v["2025"].get("pa_count", 0), "2025_granted": v["2025"].get("granted", 0),
         "2026_pa": v["2026"].get("pa_count", 0), "2026_granted": v["2026"].get("granted", 0),
         "2026_ytd_enrollees": v["2026"].get("enrollees", 0)}
        for pid, v in sorted_provs
    ]

    print("  [4/15] PA by client group (top 30)...")
    rows = con.execute(f"""
        SELECT
            groupname,
            YEAR(CAST(requestdate AS DATE)) AS yr,
            COUNT(DISTINCT panumber)         AS pa_count,
            ROUND(SUM(granted), 0)           AS total_granted,
            COUNT(DISTINCT IID)              AS unique_enrollees,
            ROUND(SUM(granted) / NULLIF(COUNT(DISTINCT IID), 0), 0) AS cost_per_enrollee
        FROM "{S}"."PA DATA"
        WHERE CAST(requestdate AS DATE) >= DATE '2025-01-01'
          AND groupname IS NOT NULL AND TRIM(groupname) != ''
        GROUP BY 1, 2 ORDER BY 4 DESC
    """).fetchall()
    grp_map = defaultdict(lambda: {"2025": {}, "2026": {}})
    for r in rows:
        grp_map[r[0]][str(r[1])] = {"pa_count": r[2], "granted": int(r[3] or 0),
                                     "enrollees": r[4], "cpe": int(r[5] or 0)}
    sorted_grps = sorted(grp_map.items(),
                         key=lambda x: x[1].get("2025", {}).get("granted", 0) +
                                       x[1].get("2026", {}).get("granted", 0), reverse=True)[:30]
    data["pa_by_group"] = [
        {"group": g, "2025_pa": v["2025"].get("pa_count", 0), "2025_granted": v["2025"].get("granted", 0),
         "2025_cpe": v["2025"].get("cpe", 0),
         "2026_pa": v["2026"].get("pa_count", 0), "2026_granted": v["2026"].get("granted", 0),
         "2026_cpe": v["2026"].get("cpe", 0)}
        for g, v in sorted_grps
    ]

    print("  [5/15] PA diagnoses (top 50 by frequency)...")
    rows = con.execute(f"""
        SELECT
            td.code,
            td.desc,
            COUNT(DISTINCT td.panumber) AS pa_count,
            ROUND(SUM(pa.granted), 0)   AS total_granted
        FROM "{S}"."TBPADIAGNOSIS" td
        JOIN "{S}"."PA DATA" pa ON td.panumber = TRY_CAST(pa.panumber AS BIGINT)
        WHERE CAST(pa.requestdate AS DATE) >= DATE '2025-01-01'
        GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 50
    """).fetchall()
    data["pa_top_diagnoses"] = [
        {"code": r[0], "description": r[1], "pa_count": r[2], "total_granted": int(r[3] or 0)}
        for r in rows
    ]

    print("  [6/15] Claims multi-year overview...")
    rows = con.execute(f"""
        SELECT
            YEAR(encounterdatefrom)            AS yr,
            COUNT(DISTINCT claimnumber)        AS claim_count,
            COUNT(DISTINCT enrollee_id)        AS unique_enrollees,
            ROUND(SUM(approvedamount), 0)      AS total_approved,
            ROUND(SUM(chargeamount), 0)        AS total_charged,
            ROUND(SUM(deniedamount), 0)        AS total_denied,
            SUM(CASE WHEN isinpatient THEN 1 ELSE 0 END) AS inpatient_count,
            ROUND(SUM(CASE WHEN isinpatient THEN approvedamount ELSE 0 END), 0) AS inpatient_cost
        FROM "{S}"."CLAIMS DATA"
        WHERE encounterdatefrom >= DATE '2020-01-01'
          AND approvedamount > 0
        GROUP BY 1 ORDER BY 1
    """).fetchall()
    data["claims_yearly"] = [
        {"year": r[0], "claim_count": r[1], "enrollees": r[2],
         "approved": int(r[3] or 0), "charged": int(r[4] or 0), "denied": int(r[5] or 0),
         "inpatient_count": r[6], "inpatient_cost": int(r[7] or 0),
         "denial_rate_pct": round((r[5] or 0)/(r[4] or 1)*100, 1),
         "inpatient_pct": round(r[6]/(r[1] or 1)*100, 1)}
        for r in rows
    ]

    print("  [7/15] Claims by diagnosis (top 40, with names)...")
    rows = con.execute(f"""
        SELECT
            cd.diagnosiscode,
            COALESCE(d.diagnosisdesc, cd.diagnosiscode) AS diagnosis_name,
            YEAR(cd.encounterdatefrom)                  AS yr,
            COUNT(DISTINCT cd.claimnumber)              AS claim_count,
            ROUND(SUM(cd.approvedamount), 0)            AS total_approved,
            COUNT(DISTINCT cd.enrollee_id)              AS unique_enrollees
        FROM "{S}"."CLAIMS DATA" cd
        LEFT JOIN "{S}"."DIAGNOSIS" d ON cd.diagnosiscode = d.diagnosiscode
        WHERE cd.encounterdatefrom >= DATE '2022-01-01'
          AND cd.approvedamount > 0
          AND cd.diagnosiscode IS NOT NULL AND cd.diagnosiscode != ''
        GROUP BY 1, 2, 3
        ORDER BY 3 DESC, 5 DESC
    """).fetchall()
    # Aggregate top diagnoses across years
    dx_map = defaultdict(lambda: {"name": "", "years": {}})
    for r in rows:
        dx_map[r[0]]["name"] = r[1]
        dx_map[r[0]]["years"][str(r[2])] = {"claims": r[3], "approved": int(r[4] or 0), "enrollees": r[5]}
    sorted_dx = sorted(dx_map.items(),
                       key=lambda x: sum(v["approved"] for v in x[1]["years"].values()), reverse=True)[:40]
    data["claims_by_diagnosis"] = [
        {"code": code, "name": v["name"], "years": v["years"]}
        for code, v in sorted_dx
    ]

    print("  [8/15] Claims by procedure (top 30)...")
    rows = con.execute(f"""
        SELECT
            cd.code,
            COALESCE(pd.proceduredesc, cd.code) AS procedure_name,
            YEAR(cd.encounterdatefrom)           AS yr,
            COUNT(DISTINCT cd.claimnumber)       AS claim_count,
            ROUND(SUM(cd.approvedamount), 0)     AS total_approved
        FROM "{S}"."CLAIMS DATA" cd
        LEFT JOIN "{S}"."PROCEDURE DATA" pd ON cd.code = pd.procedurecode
        WHERE cd.encounterdatefrom >= DATE '2022-01-01'
          AND cd.approvedamount > 0
          AND cd.code IS NOT NULL AND cd.code != ''
        GROUP BY 1, 2, 3
        ORDER BY 3 DESC, 5 DESC
    """).fetchall()
    proc_map = defaultdict(lambda: {"name": "", "years": {}})
    for r in rows:
        proc_map[r[0]]["name"] = r[1]
        proc_map[r[0]]["years"][str(r[2])] = {"claims": r[3], "approved": int(r[4] or 0)}
    sorted_proc = sorted(proc_map.items(),
                         key=lambda x: sum(v["approved"] for v in x[1]["years"].values()), reverse=True)[:30]
    data["claims_by_procedure"] = [
        {"code": code, "name": v["name"], "years": v["years"]}
        for code, v in sorted_proc
    ]

    print("  [9/15] Claims by provider (top 30, multi-year)...")
    rows = con.execute(f"""
        SELECT
            cd.nhisproviderid,
            COALESCE(pr.providername, '(Unknown)') AS provider_name,
            pr.bands,
            YEAR(cd.encounterdatefrom)             AS yr,
            COUNT(DISTINCT cd.claimnumber)         AS claim_count,
            ROUND(SUM(cd.approvedamount), 0)       AS total_approved,
            COUNT(DISTINCT cd.enrollee_id)         AS enrollees,
            SUM(CASE WHEN cd.isinpatient THEN 1 ELSE 0 END) AS inpatient_count
        FROM "{S}"."CLAIMS DATA" cd
        LEFT JOIN "{S}"."PROVIDERS" pr
            ON TRY_CAST(cd.nhisproviderid AS BIGINT) = TRY_CAST(pr.providerid AS BIGINT)
        WHERE cd.encounterdatefrom >= DATE '2022-01-01'
          AND cd.approvedamount > 0
        GROUP BY 1, 2, 3, 4
        ORDER BY 4 DESC, 6 DESC
    """).fetchall()
    cprov_map = defaultdict(lambda: {"name": "", "band": "", "years": {}})
    for r in rows:
        cprov_map[r[0]]["name"] = r[1]
        cprov_map[r[0]]["band"] = r[2] or "—"
        cprov_map[r[0]]["years"][str(r[3])] = {"claims": r[4], "approved": int(r[5] or 0),
                                                "enrollees": r[6], "inpatient": r[7]}
    sorted_cprov = sorted(cprov_map.items(),
                          key=lambda x: sum(v["approved"] for v in x[1]["years"].values()), reverse=True)[:30]
    data["claims_by_provider"] = [
        {"provider_id": pid, "name": v["name"], "band": v["band"], "years": v["years"]}
        for pid, v in sorted_cprov
    ]

    print("  [10/15] High utiliser enrollees...")
    rows = con.execute(f"""
        SELECT
            cd.enrollee_id,
            COALESCE(m.firstname || ' ' || m.lastname, cd.enrollee_id) AS name,
            g.groupname,
            YEAR(cd.encounterdatefrom) AS yr,
            COUNT(DISTINCT cd.claimnumber)   AS claim_count,
            ROUND(SUM(cd.approvedamount), 0) AS total_approved,
            COUNT(DISTINCT cd.diagnosiscode) AS distinct_diagnoses,
            SUM(CASE WHEN cd.isinpatient THEN 1 ELSE 0 END) AS inpatient_episodes
        FROM "{S}"."CLAIMS DATA" cd
        LEFT JOIN "{S}"."MEMBERS" mem ON cd.enrollee_id = mem.memberid::VARCHAR
        LEFT JOIN "{S}"."MEMBER"  m   ON TRY_CAST(mem.memberid AS BIGINT) = m.memberid
        LEFT JOIN "{S}"."GROUPS"  g   ON mem.groupid = g.groupid
        WHERE cd.encounterdatefrom >= DATE '2023-01-01'
          AND cd.approvedamount > 0
        GROUP BY 1, 2, 3, 4
        HAVING SUM(cd.approvedamount) > 500000
        ORDER BY 4 DESC, 6 DESC
        LIMIT 30
    """).fetchall()
    data["high_utilisers"] = [
        {"enrollee_id": r[0], "name": r[1], "group": r[2] or "—",
         "year": r[3], "claims": r[4], "approved": int(r[5] or 0),
         "diagnoses": r[6], "inpatient_episodes": r[7]}
        for r in rows
    ]

    print("  [11/15] Inpatient deep dive...")
    rows = con.execute(f"""
        SELECT
            YEAR(cd.encounterdatefrom) AS yr,
            COALESCE(d.diagnosisdesc, cd.diagnosiscode) AS diagnosis,
            COUNT(DISTINCT cd.claimnumber)   AS admissions,
            ROUND(SUM(cd.approvedamount), 0) AS total_cost,
            ROUND(AVG(cd.approvedamount), 0) AS avg_cost_per_claim,
            COUNT(DISTINCT cd.enrollee_id)   AS unique_patients
        FROM "{S}"."CLAIMS DATA" cd
        LEFT JOIN "{S}"."DIAGNOSIS" d ON cd.diagnosiscode = d.diagnosiscode
        WHERE cd.isinpatient = TRUE
          AND cd.encounterdatefrom >= DATE '2022-01-01'
          AND cd.approvedamount > 0
        GROUP BY 1, 2 ORDER BY 1 DESC, 4 DESC
    """).fetchall()
    ip_map = defaultdict(lambda: {"years": {}})
    for r in rows:
        ip_map[r[1]]["years"][str(r[0])] = {"admissions": r[2], "cost": int(r[3] or 0),
                                              "avg_cost": int(r[4] or 0), "patients": r[5]}
    sorted_ip = sorted(ip_map.items(),
                       key=lambda x: sum(v["cost"] for v in x[1]["years"].values()), reverse=True)[:25]
    data["inpatient_by_diagnosis"] = [
        {"diagnosis": dx, "years": v["years"]}
        for dx, v in sorted_ip
    ]

    print("  [12/15] Seasonal & monthly claims patterns...")
    rows = con.execute(f"""
        SELECT
            YEAR(encounterdatefrom)  AS yr,
            MONTH(encounterdatefrom) AS mo,
            COUNT(DISTINCT claimnumber)   AS claim_count,
            ROUND(SUM(approvedamount), 0) AS total_approved,
            COUNT(DISTINCT enrollee_id)   AS enrollees
        FROM "{S}"."CLAIMS DATA"
        WHERE encounterdatefrom >= DATE '2022-01-01'
          AND approvedamount > 0
        GROUP BY 1, 2 ORDER BY 1, 2
    """).fetchall()
    data["claims_monthly"] = [
        {"year": r[0], "month": MONTH_LABELS[r[1]-1],
         "claim_count": r[2], "approved": int(r[3] or 0), "enrollees": r[4]}
        for r in rows
    ]

    # ── NEW: PA-to-Claims vetting gap ─────────────────────────────────────────
    print("  [13/15] PA-to-Claims vetting gap (authorised vs approved)...")
    rows = con.execute(f"""
        WITH pa_agg AS (
            SELECT
                CAST(panumber AS VARCHAR)       AS pa_num,
                CAST(pa.providerid AS VARCHAR)  AS pa_provid,
                COALESCE(pr.providername, '(Unknown)') AS provider_name,
                pa.groupname,
                ROUND(SUM(pa.granted), 0)       AS pa_granted
            FROM "{S}"."PA DATA" pa
            LEFT JOIN "{S}"."PROVIDERS" pr
                ON TRY_CAST(pa.providerid AS BIGINT) = TRY_CAST(pr.providerid AS BIGINT)
            WHERE CAST(pa.requestdate AS DATE) >= DATE '2025-01-01'
              AND pa.panumber IS NOT NULL
            GROUP BY 1, 2, 3, 4
        ),
        cl_agg AS (
            SELECT
                CAST(cd.panumber AS VARCHAR)    AS pa_num,
                ROUND(SUM(cd.approvedamount), 0) AS cl_approved,
                ROUND(SUM(cd.deniedamount), 0)   AS cl_denied
            FROM "{S}"."CLAIMS DATA" cd
            WHERE cd.panumber IS NOT NULL
              AND cd.approvedamount > 0
            GROUP BY 1
        )
        SELECT
            pa.pa_provid,
            pa.provider_name,
            pa.groupname,
            COUNT(*)                      AS matched_panumbers,
            ROUND(SUM(pa.pa_granted), 0)  AS total_pa_granted,
            ROUND(SUM(cl.cl_approved), 0) AS total_cl_approved,
            ROUND(SUM(cl.cl_denied), 0)   AS total_cl_denied,
            ROUND(
                (SUM(pa.pa_granted) - SUM(cl.cl_approved))
                / NULLIF(SUM(pa.pa_granted), 0) * 100
            , 1) AS vetting_reduction_pct
        FROM pa_agg pa
        JOIN cl_agg cl ON pa.pa_num = cl.pa_num
        GROUP BY 1, 2, 3
        ORDER BY total_pa_granted DESC
        LIMIT 30
    """).fetchall()
    data["pa_claims_gap"] = [
        {"provider_id": r[0], "provider_name": r[1], "group": r[2] or "—",
         "matched_panumbers": r[3],
         "pa_granted": int(r[4] or 0), "cl_approved": int(r[5] or 0), "cl_denied": int(r[6] or 0),
         "vetting_reduction_pct": float(r[7] or 0)}
        for r in rows
    ]

    # Overall vetting summary (year-level)
    rows2 = con.execute(f"""
        WITH pa_agg AS (
            SELECT CAST(panumber AS VARCHAR) AS pa_num,
                   YEAR(CAST(requestdate AS DATE)) AS yr,
                   ROUND(SUM(granted), 0) AS pa_granted
            FROM "{S}"."PA DATA"
            WHERE panumber IS NOT NULL AND CAST(requestdate AS DATE) >= DATE '2025-01-01'
            GROUP BY 1, 2
        ),
        cl_agg AS (
            SELECT CAST(panumber AS VARCHAR) AS pa_num,
                   ROUND(SUM(approvedamount), 0) AS cl_approved
            FROM "{S}"."CLAIMS DATA"
            WHERE panumber IS NOT NULL AND approvedamount > 0
            GROUP BY 1
        )
        SELECT
            pa.yr,
            COUNT(*)                      AS matched_panumbers,
            ROUND(SUM(pa.pa_granted), 0)  AS total_pa_granted,
            ROUND(SUM(cl.cl_approved), 0) AS total_cl_approved,
            ROUND((SUM(pa.pa_granted) - SUM(cl.cl_approved))
                  / NULLIF(SUM(pa.pa_granted), 0) * 100, 1) AS vetting_reduction_pct
        FROM pa_agg pa JOIN cl_agg cl ON pa.pa_num = cl.pa_num
        GROUP BY 1 ORDER BY 1
    """).fetchall()
    data["pa_claims_gap_summary"] = [
        {"year": r[0], "matched_panumbers": r[1],
         "pa_granted": int(r[2] or 0), "cl_approved": int(r[3] or 0),
         "vetted_away": int((r[2] or 0) - (r[3] or 0)),
         "vetting_reduction_pct": float(r[4] or 0)}
        for r in rows2
    ]

    # ── NEW: No-auth claims (NI Auth — panumber IS NULL) ─────────────────────
    print("  [14/15] No-auth (NI Auth) claims analysis...")
    rows = con.execute(f"""
        SELECT
            YEAR(encounterdatefrom)  AS yr,
            MONTH(encounterdatefrom) AS mo,
            COUNT(DISTINCT claimnumber)   AS claim_count,
            ROUND(SUM(approvedamount), 0) AS total_approved,
            COUNT(DISTINCT enrollee_id)   AS unique_enrollees,
            SUM(CASE WHEN isinpatient THEN 1 ELSE 0 END) AS inpatient_count
        FROM "{S}"."CLAIMS DATA"
        WHERE panumber IS NULL
          AND approvedamount > 0
          AND encounterdatefrom >= DATE '2022-01-01'
        GROUP BY 1, 2 ORDER BY 1, 2
    """).fetchall()
    data["noauth_monthly"] = [
        {"year": r[0], "month": MONTH_LABELS[r[1]-1],
         "claim_count": r[2], "approved": int(r[3] or 0),
         "enrollees": r[4], "inpatient": r[5]}
        for r in rows
    ]

    rows = con.execute(f"""
        SELECT
            cd.nhisproviderid,
            COALESCE(pr.providername, '(Unknown)') AS provider_name,
            pr.bands,
            YEAR(cd.encounterdatefrom) AS yr,
            COUNT(DISTINCT cd.claimnumber)   AS claim_count,
            ROUND(SUM(cd.approvedamount), 0) AS total_approved
        FROM "{S}"."CLAIMS DATA" cd
        LEFT JOIN "{S}"."PROVIDERS" pr
            ON TRY_CAST(cd.nhisproviderid AS BIGINT) = TRY_CAST(pr.providerid AS BIGINT)
        WHERE cd.panumber IS NULL
          AND cd.approvedamount > 0
          AND cd.encounterdatefrom >= DATE '2025-01-01'
        GROUP BY 1, 2, 3, 4
        ORDER BY total_approved DESC LIMIT 30
    """).fetchall()
    noauth_prov = defaultdict(lambda: {"name": "", "band": "", "years": {}})
    for r in rows:
        noauth_prov[r[0]]["name"] = r[1]
        noauth_prov[r[0]]["band"] = r[2] or "—"
        noauth_prov[r[0]]["years"][str(r[3])] = {"claims": r[4], "approved": int(r[5] or 0)}
    data["noauth_by_provider"] = [
        {"provider_id": pid, "name": v["name"], "band": v["band"], "years": v["years"]}
        for pid, v in sorted(noauth_prov.items(),
                             key=lambda x: sum(y["approved"] for y in x[1]["years"].values()),
                             reverse=True)[:20]
    ]

    rows = con.execute(f"""
        SELECT
            cd.diagnosiscode,
            COALESCE(d.diagnosisdesc, cd.diagnosiscode) AS diagnosis_name,
            COUNT(DISTINCT cd.claimnumber)              AS claim_count,
            ROUND(SUM(cd.approvedamount), 0)            AS total_approved
        FROM "{S}"."CLAIMS DATA" cd
        LEFT JOIN "{S}"."DIAGNOSIS" d ON cd.diagnosiscode = d.diagnosiscode
        WHERE cd.panumber IS NULL
          AND cd.approvedamount > 0
          AND cd.encounterdatefrom >= DATE '2025-01-01'
          AND cd.diagnosiscode IS NOT NULL AND cd.diagnosiscode != ''
        GROUP BY 1, 2 ORDER BY total_approved DESC LIMIT 30
    """).fetchall()
    data["noauth_by_diagnosis"] = [
        {"code": r[0], "name": r[1], "claim_count": r[2], "approved": int(r[3] or 0)}
        for r in rows
    ]

    # ── NEW: Submission lag analysis ──────────────────────────────────────────
    print("  [15/15] Submission lag (encounter → submission gap)...")
    rows = con.execute(f"""
        SELECT
            YEAR(encounterdatefrom)   AS encounter_yr,
            YEAR(datesubmitted)       AS submit_yr,
            CASE
                WHEN DATEDIFF('day', encounterdatefrom, datesubmitted) < 0    THEN 'invalid'
                WHEN DATEDIFF('day', encounterdatefrom, datesubmitted) <= 30  THEN '0-30d'
                WHEN DATEDIFF('day', encounterdatefrom, datesubmitted) <= 60  THEN '31-60d'
                WHEN DATEDIFF('day', encounterdatefrom, datesubmitted) <= 90  THEN '61-90d'
                WHEN DATEDIFF('day', encounterdatefrom, datesubmitted) <= 180 THEN '91-180d'
                WHEN DATEDIFF('day', encounterdatefrom, datesubmitted) <= 365 THEN '181-365d'
                ELSE '365d+'
            END                        AS lag_bucket,
            COUNT(DISTINCT claimnumber)   AS claim_count,
            ROUND(SUM(approvedamount), 0) AS total_approved
        FROM "{S}"."CLAIMS DATA"
        WHERE encounterdatefrom >= DATE '2023-01-01'
          AND datesubmitted IS NOT NULL
          AND approvedamount > 0
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, lag_bucket
    """).fetchall()
    data["submission_lag"] = [
        {"encounter_yr": r[0], "submit_yr": r[1], "lag_bucket": r[2],
         "claim_count": r[3], "approved": int(r[4] or 0)}
        for r in rows
    ]

    # How much 2025 encounter liability is being submitted in 2026
    rows = con.execute(f"""
        SELECT
            YEAR(datesubmitted) AS submit_yr,
            COUNT(DISTINCT claimnumber)   AS claim_count,
            ROUND(SUM(approvedamount), 0) AS total_approved,
            ROUND(AVG(DATEDIFF('day', encounterdatefrom, datesubmitted)), 0) AS avg_lag_days
        FROM "{S}"."CLAIMS DATA"
        WHERE encounterdatefrom >= DATE '2025-01-01'
          AND encounterdatefrom < DATE '2026-01-01'
          AND datesubmitted IS NOT NULL
          AND approvedamount > 0
        GROUP BY 1 ORDER BY 1
    """).fetchall()
    data["lag_2025_encounters"] = [
        {"submit_yr": r[0], "claim_count": r[1],
         "total_approved": int(r[2] or 0), "avg_lag_days": int(r[3] or 0)}
        for r in rows
    ]

    return data


def build_prompt(data: dict) -> str:
    """Format all collected data into a comprehensive analysis prompt for Claude Opus."""

    pa_total_25 = sum(m["granted"] for m in data["pa_monthly"] if m["year"] == 2025)
    pa_total_26 = sum(m["granted"] for m in data["pa_monthly"] if m["year"] == 2026)
    pa_count_25 = sum(m["pa_count"] for m in data["pa_monthly"] if m["year"] == 2025)
    pa_count_26 = sum(m["pa_count"] for m in data["pa_monthly"] if m["year"] == 2026)

    cl_25 = next((r for r in data["claims_yearly"] if r["year"] == 2025), {})
    cl_26 = next((r for r in data["claims_yearly"] if r["year"] == 2026), {})

    # No-auth totals for prompt header
    noauth_25 = sum(r["approved"] for r in data["noauth_monthly"] if r["year"] == 2025)
    noauth_26 = sum(r["approved"] for r in data["noauth_monthly"] if r["year"] == 2026)
    gap_summary = data.get("pa_claims_gap_summary", [])
    gap_25 = next((r for r in gap_summary if r["year"] == 2025), {})
    gap_26 = next((r for r in gap_summary if r["year"] == 2026), {})

    prompt = f"""You are a senior medical data analyst and actuary specialising in health insurance in Nigeria. You are analysing data for Clearline HMO (a Health Maintenance Organisation), covering their entire insured population.

Today is {TODAY}. This is the monthly analytics report for {REPORT_LABEL}.

---
# DATA MODEL — CRITICAL CONTEXT (read carefully before analysis)

## PA (Prior Authorisation) System
- A **panumber** represents a single facility visit by one enrollee on one day. It is the unit of PA counting.
- When a patient visits a hospital, the provider requests authorisation for care. Clearline HMO grants a **PA amount** (`granted`). This is the maximum liability Clearline has committed to for that visit.
- One panumber = one visit. Multiple rows in PA DATA can share a panumber if the visit covers multiple services; they are deduplicated by panumber for counting.
- PA data is available from 2025 onwards. It measures **disease burden** and **authorised liability**.

## Claims (Vetting) System
- Once care is delivered, the provider submits a claim. Clearline vets the claim against the PA.
- **`approvedamount`** = what Clearline actually pays after vetting. This is always ≤ `granted` (Clearline can claw back).
- **`deniedamount`** = what was refused/clawed back during vetting.
- The **PA-to-Claims gap** (granted minus approved) = vetting effectiveness — how much Clearline saved by scrutinising claims.
- Claims data is available from 2020 onwards.

## Two Date Dimensions
- **`encounterdatefrom`**: The date the patient was seen at the facility. Use this for **disease burden analysis** — it tells you when illness occurred.
- **`datesubmitted`**: The date the provider billed Clearline. Use this for **cash flow analysis** — it tells you when Clearline faces payment pressure.
- A 2025 encounter can be billed (submitted) in 2026. This creates **outstanding liability** — 2026 MLR is inflated by late-submitted 2025 claims.

## No-Auth Claims (NI Auth)
- Some claims have **no panumber** (`panumber IS NULL`). These are called "NI Auth" or no-auth claims.
- Certain services (e.g. emergency presentations, some outpatient services) are allowed without prior authorisation.
- No-auth claims represent **uncontrolled spend** — Clearline has no prior gate on these costs.
- They are a separate liability pool from PA-authorised claims.

---
# DATASET OVERVIEW

**PA (Prior Authorisation) Data:** 2025–2026 only
- 2025: {pa_count_25:,} PAs totalling {fmt_m(pa_total_25)} granted
- 2026 YTD: {pa_count_26:,} PAs totalling {fmt_m(pa_total_26)} granted

**PA-to-Claims Vetting (matched panumbers):**
- 2025: {gap_25.get('matched_panumbers', 0):,} panumbers matched, {fmt_m(gap_25.get('pa_granted', 0))} authorised → {fmt_m(gap_25.get('cl_approved', 0))} approved ({gap_25.get('vetting_reduction_pct', 0)}% clawed back)
- 2026 YTD: {gap_26.get('matched_panumbers', 0):,} panumbers matched, {fmt_m(gap_26.get('pa_granted', 0))} authorised → {fmt_m(gap_26.get('cl_approved', 0))} approved ({gap_26.get('vetting_reduction_pct', 0)}% clawed back)

**Claims Data (by encounter date):** Available from 2020
- 2025: {cl_25.get('claim_count', 0):,} claims, {fmt_m(cl_25.get('approved', 0))} approved
- 2026 YTD: {cl_26.get('claim_count', 0):,} claims, {fmt_m(cl_26.get('approved', 0))} approved

**No-Auth (NI Auth) Claims:**
- 2025: {fmt_m(noauth_25)} approved without PA
- 2026 YTD: {fmt_m(noauth_26)} approved without PA

---
# SECTION A — PA MONTHLY TRENDS (2025–2026)

{json.dumps(data["pa_monthly"], indent=2)}

---
# SECTION B — PA BY BENEFIT CODE (top 20 by cost)

Benefit codes represent the type of care: ADM=Admission, ICU=Intensive Care, etc.
{json.dumps(data["pa_by_benefit"], indent=2)}

---
# SECTION C — PA BY PROVIDER (top 30, YoY comparison)

{json.dumps(data["pa_by_provider"], indent=2)}

---
# SECTION D — PA BY CLIENT GROUP (top 30, YoY comparison)

{json.dumps(data["pa_by_group"], indent=2)}

---
# SECTION E — TOP PA DIAGNOSES (from PA authorisation requests)

These are the diagnoses attached to PA requests. Use your medical knowledge to group related diagnoses into clinical clusters (e.g., all malaria variants → Malaria; UTI variants → Urinary Tract Infection; hypertension-related → Cardiovascular Disease, etc.). Identify the true clinical burden.
{json.dumps(data["pa_top_diagnoses"], indent=2)}

---
# SECTION F — CLAIMS MULTI-YEAR OVERVIEW (2020–2026)

{json.dumps(data["claims_yearly"], indent=2)}

---
# SECTION G — CLAIMS BY DIAGNOSIS (top 40 diagnoses, multi-year)

Apply the same medical grouping — cluster ICD codes into clinical categories. Identify which disease groups are driving the most cost. Note any shift in disease burden year-over-year.
{json.dumps(data["claims_by_diagnosis"], indent=2)}

---
# SECTION H — CLAIMS BY PROCEDURE (top 30, multi-year)

{json.dumps(data["claims_by_procedure"], indent=2)}

---
# SECTION I — CLAIMS BY PROVIDER (top 30, multi-year)

{json.dumps(data["claims_by_provider"], indent=2)}

---
# SECTION J — HIGH UTILISER ENROLLEES (>₦500K annual claims)

{json.dumps(data["high_utilisers"], indent=2)}

---
# SECTION K — INPATIENT ADMISSIONS BY DIAGNOSIS (top 25, multi-year)

{json.dumps(data["inpatient_by_diagnosis"], indent=2)}

---
# SECTION L — MONTHLY CLAIMS SEASONALITY (2022–2026)

{json.dumps(data["claims_monthly"], indent=2)}

---
# SECTION M — PA-TO-CLAIMS VETTING GAP (top 30 providers by PA volume, 2025–2026)

For each provider: how much was authorised (PA granted) vs how much was actually paid (claims approved)?
The difference is what the vetting/claims team clawed back. A high vetting_reduction_pct means strong claims control; a very low reduction may signal rubber-stamping.

Overall summary by year:
{json.dumps(data["pa_claims_gap_summary"], indent=2)}

Top providers (ordered by PA authorised):
{json.dumps(data["pa_claims_gap"], indent=2)}

---
# SECTION N — NO-AUTH (NI AUTH) CLAIMS — Spend Without Prior Authorisation

These claims have no panumber — care was delivered without a prior authorisation gate.
This is legitimate for certain service types but represents uncontrolled cost exposure.

Monthly trend (by encounter date, 2022–2026):
{json.dumps(data["noauth_monthly"], indent=2)}

Top providers driving no-auth spend (2025–2026):
{json.dumps(data["noauth_by_provider"], indent=2)}

Top diagnoses in no-auth claims (2025–2026):
{json.dumps(data["noauth_by_diagnosis"], indent=2)}

---
# SECTION O — SUBMISSION LAG (Outstanding Liability from Late-Submitted Claims)

`lag_bucket` = gap between when the patient was seen (encounterdatefrom) and when the provider billed (datesubmitted).
Late submissions mean Clearline carries undisclosed liability — 2025 encounters appearing in 2026 submissions inflate 2026 MLR.

Lag distribution by encounter year and submission year:
{json.dumps(data["submission_lag"], indent=2)}

2025 encounters split by submission year (shows how much 2025 liability has spilled into 2026):
{json.dumps(data["lag_2025_encounters"], indent=2)}

---

# YOUR TASK

Produce a comprehensive medical analytics report structured exactly as follows. Be specific, use the actual numbers from the data, and apply deep medical and actuarial insight. DO NOT be generic — every finding must reference actual data points.

## 1. EXECUTIVE SUMMARY (5–8 bullet points)
Key headline findings the CEO needs to know immediately.

## 2. PA ANALYSIS

### 2a. Volume & Cost Trends
Month-by-month analysis. Is utilisation accelerating or decelerating? What is the trajectory heading into next month?

### 2b. Benefit Code Breakdown
What categories of care dominate PA costs? What is driving admissions specifically?

### 2c. Top Provider Analysis
Identify the top 10 providers by cost. Flag any providers showing unusual year-over-year growth (>30%). Are higher-band providers receiving too many referrals?

### 2d. Top Client Group Analysis
Which groups are the highest utilisers? Which groups show the most concerning cost-per-enrollee ratios? Which groups have grown the most?

### 2e. Disease Burden from PA (Clinical Clusters)
Group all PA diagnoses into clinical clusters using ICD knowledge. Rank clusters by frequency and cost. What is the #1 disease burden? What is rising?

## 3. CLAIMS ANALYSIS

### 3a. Multi-Year Cost Trends
How has total claims cost evolved since 2020? What is the CAGR? Is the trend accelerating?

### 3b. Disease Burden from Claims (Clinical Clusters)
Apply full ICD clinical grouping. Identify the top 10 disease clusters by cost. Which clusters are growing fastest? Which have declined? Cross-reference with the PA diagnoses — are the same diseases appearing in both?

### 3c. Top Procedure Analysis
What are the most frequently claimed and highest-cost procedures? Are there procedures growing disproportionately?

### 3d. Provider Claims Patterns
Which providers are growing fastest in claims? Any discrepancies between a provider's PA volume and claims volume (potential fraud signal)?

### 3e. Inpatient Deep Dive
What is driving inpatient admissions? Which diagnoses have the highest average cost per admission? What proportion of total cost is inpatient?

### 3f. High Utiliser Profile
Describe the profile of high utilisers. What diagnoses do they carry? What groups are they concentrated in? What proportion of total cost do they represent?

### 3g. Seasonality Patterns
Identify clear seasonal patterns. Which months consistently have the highest utilisation? Use this to forecast the next 3 months.

## 3h. PA-to-Claims Vetting Gap Analysis
What proportion of authorised (PA granted) spend is actually paid (claims approved)? Which providers have the highest and lowest vetting reduction rates? Is Clearline's claims team being rigorous or lenient? Flag any providers where the PA amount is dramatically higher than what was ultimately approved — this may indicate over-authorisation, overbilling, or strong vetting.

## 3i. No-Auth (NI Auth) Claims Analysis
What is the scale and trend of spend that bypasses the PA gate entirely? Which diagnoses and providers dominate no-auth claims? Is no-auth spend growing faster than PA-authorised spend? Are any providers suspiciously concentrated in no-auth claims (potential abuse of no-auth pathways)? What proportion of total approved spend does no-auth represent?

## 3j. Submission Lag & Outstanding Liability
What is the typical lag between encounter date and submission date? How much 2025 liability is being submitted in 2026 — quantify the amount and proportion. What does this mean for Clearline's MLR? Are certain providers systematically late submitters? Flag any claims submitted 180+ days after the encounter as operationally abnormal and potentially fraudulent.

## 4. CROSS-CUTTING INSIGHTS (patterns humans would miss)
Identify at least 5 non-obvious patterns, correlations, or anomalies from combining multiple data sections. Examples: a group appearing in both top PA and top claims, a provider with high PA but low claims, a diagnosis cluster spiking only in specific months, etc.

## 5. COST REDUCTION RECOMMENDATIONS (priority-ordered)
For each recommendation, state:
- **Finding:** what the data shows
- **Action:** specific action for management to take
- **Estimated impact:** quantified estimate of potential savings
- **Timeline:** when this should be actioned

Provide at least 8 actionable recommendations covering: provider management, benefit design, disease management programs, group-level interventions, inpatient cost control, and high-utiliser case management.

## 6. FORECAST — NEXT 3 MONTHS
Based on all trends, what do you forecast for PA volume/cost and claims cost for the next 3 months? What are the key risks to watch?

## 7. DATA QUALITY FLAGS
Note any data anomalies, gaps, or inconsistencies you noticed that may affect accuracy.

Write this as a professional report. Use Nigerian Naira (₦) for all monetary values. Reference specific company names, provider names, and diagnosis names from the data — do not anonymise. This is an internal management report.
"""
    return prompt


def call_claude(prompt: str) -> str:
    """Send the prompt to Claude Opus 4.6 and return the full report."""
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    print("  Calling Claude Opus 4.6 (this may take 60–120 seconds)...")
    message = client.messages.create(
        model="claude-opus-4-6",
        max_tokens=8192,
        messages=[{"role": "user", "content": prompt}],
    )
    return message.content[0].text


def extract_key_metrics(data: dict) -> dict:
    """Pull out headline numbers for quick display on the frontend."""
    pa_25 = sum(m["granted"] for m in data["pa_monthly"] if m["year"] == 2025)
    pa_26 = sum(m["granted"] for m in data["pa_monthly"] if m["year"] == 2026)
    pa_n_26 = sum(m["pa_count"] for m in data["pa_monthly"] if m["year"] == 2026)
    cl_26 = next((r for r in data["claims_yearly"] if r["year"] == 2026), {})
    cl_25 = next((r for r in data["claims_yearly"] if r["year"] == 2025), {})
    top_dx = data["pa_top_diagnoses"][:5] if data["pa_top_diagnoses"] else []
    top_prov = data["pa_by_provider"][:5] if data["pa_by_provider"] else []
    top_grp  = data["pa_by_group"][:5]    if data["pa_by_group"]    else []
    return {
        "report_period": REPORT_LABEL,
        "pa_total_granted_2026_ytd": int(pa_26),
        "pa_total_granted_2025":     int(pa_25),
        "pa_count_2026_ytd":         int(pa_n_26),
        "claims_approved_2026_ytd":  cl_26.get("approved", 0),
        "claims_approved_2025":      cl_25.get("approved", 0),
        "claims_count_2026_ytd":     cl_26.get("claim_count", 0),
        "inpatient_pct_2026":        cl_26.get("inpatient_pct", 0),
        "denial_rate_2026":          cl_26.get("denial_rate_pct", 0),
        "top_5_pa_diagnoses":        [d["description"] for d in top_dx],
        "top_5_pa_providers":        [p["name"] for p in top_prov],
        "top_5_pa_groups":           [g["group"] for g in top_grp],
    }


def main():
    print(f"[{TODAY}] Monthly AI Report — {REPORT_LABEL}")
    con = duckdb.connect(f"md:?motherduck_token={MOTHERDUCK_TOKEN}")
    sb  = create_client(SUPABASE_URL, SUPABASE_KEY)

    # Mark as running
    sb.table("ai_monthly_reports").upsert([{
        "report_period": REPORT_LABEL,
        "status": "running",
        "triggered_at": str(TODAY),
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
            "report_period":  REPORT_LABEL,
            "status":         "complete",
            "report_markdown": report_md,
            "key_metrics":    json.dumps(metrics),
            "triggered_at":   str(TODAY),
            "generated_at":   str(TODAY),
        }], on_conflict="report_period").execute()

        print(f"[{TODAY}] Report complete — {len(report_md):,} characters saved.")

    except Exception as e:
        sb.table("ai_monthly_reports").upsert([{
            "report_period": REPORT_LABEL,
            "status": f"error: {str(e)[:500]}",
            "triggered_at": str(TODAY),
        }], on_conflict="report_period").execute()
        raise


if __name__ == "__main__":
    main()
