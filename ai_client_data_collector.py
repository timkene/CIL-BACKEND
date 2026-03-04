"""
Data Collector for Renewal Analysis API
Queries DuckDB to gather all metrics for a given group/contract period.
"""

import duckdb
from datetime import date, datetime
from typing import Optional
from dataclasses import dataclass, field

SCHEMA = '"AI DRIVEN DATA"'

@dataclass
class RenewalData:
    # Group info
    group_id: int = 0
    group_name: str = ""
    
    # Contract periods
    current_start: Optional[date] = None
    current_end: Optional[date] = None
    prev_start: Optional[date] = None
    prev_end: Optional[date] = None
    
    # Members
    active_members: int = 0
    prev_members: int = 0
    plans: list = field(default_factory=list)  # [{name, limit, premium, count}]
    
    # Current contract financials
    total_debit: float = 0
    cash_received: float = 0
    debit_notes: list = field(default_factory=list)  # [{ref, desc, amount, period}]
    
    # Claims
    claims_total: float = 0
    claims_count: int = 0
    claims_months: float = 5.5
    monthly_claims: list = field(default_factory=list)  # [{month, count, amount, pa_amount, driver}]
    
    # PA
    pa_total_authorized: float = 0
    pa_count: int = 0
    unclaimed_pa: float = 0        # PA granted in contract period NOT matched to a claim (encounterdatefrom)
    unclaimed_pa_count: int = 0    # Number of unclaimed PA records
    
    # Previous contract
    prev_claims: float = 0
    prev_debit: float = 0
    prev_cash: float = 0
    prev_pa: float = 0
    prev_members_utilizing: int = 0
    
    # Top members
    top_members: list = field(default_factory=list)  # [{iid, name, gender, age, amount, pct, primary_condition, nature}]
    members_utilizing: int = 0
    
    # Plan utilization (for Plan Limit Utilisation table)
    plan_utilization: list = field(default_factory=list)  # [{name, limit, premium, avg_spend, limit_pct, over_limit_risk}]
    
    # Top providers
    top_providers: list = field(default_factory=list)  # [{name, pa_count, amount, pct, prev_amount}]
    prev_top_providers: list = field(default_factory=list)
    
    # Top diagnoses
    top_diagnoses: list = field(default_factory=list)  # [{code, name, count, amount, pct}]
    
    # Calculated metrics
    annualized_claims: float = 0
    projected_mlr: float = 0
    cash_mlr: float = 0
    prev_mlr: float = 0
    ytd_mlr: float = 0
    top5_pct: float = 0
    chronic_pct: float = 0
    
    # PMPM analysis
    prev_pmpm: float = 0
    curr_pmpm: float = 0
    actuarial_premium: float = 0
    
    # SRS
    srs_classification: str = "EPISODIC"
    
    # Strategy intelligence data — fed to Claude Opus for deep renewal strategy
    repeat_high_cost_members: list = field(default_factory=list)  # members high-cost in BOTH contract periods
    provider_dual_period: list = field(default_factory=list)      # provider performance across both periods
    high_cost_threshold: float = 0    # 80th percentile member spend (current contract)
    
    analysis_date: str = ""
    data_cutoff: str = ""


def collect_data(
    group_name_pattern: str,
    current_start: str,
    current_end: str,
    prev_start: str,
    prev_end: str,
    db_path: str = None
) -> RenewalData:
    """Collect all renewal data for a group from DuckDB."""
    
    if db_path:
        conn = duckdb.connect(db_path)
    else:
        # Try to find the DB from environment or use default
        import os
        db_path = os.environ.get("DUCKDB_PATH", "/data/ai_driven.duckdb")
        conn = duckdb.connect(db_path, read_only=True)
    
    data = RenewalData()
    data.analysis_date = datetime.now().strftime("%B %Y")
    data.data_cutoff = datetime.now().strftime("%B %d, %Y")
    
    try:
        # ─── GROUP INFO ───────────────────────────────────────────────
        row = conn.execute(f"""
            SELECT groupid, groupname 
            FROM {SCHEMA}."GROUPS" 
            WHERE UPPER(groupname) LIKE UPPER('%{group_name_pattern}%')
            LIMIT 1
        """).fetchone()
        
        if not row:
            raise ValueError(f"Group not found: {group_name_pattern}")
        
        data.group_id = row[0]
        data.group_name = row[1]
        
        # ─── CONTRACT DATES ───────────────────────────────────────────
        data.current_start = date.fromisoformat(current_start)
        data.current_end = date.fromisoformat(current_end)
        data.prev_start = date.fromisoformat(prev_start)
        data.prev_end = date.fromisoformat(prev_end)
        
        from datetime import date as date_type
        today = date_type.today()
        curr_months = max(0.5, (min(today, data.current_end) - data.current_start).days / 30.44)
        data.claims_months = round(curr_months, 1)
        
        # ─── PLANS & MEMBERS ──────────────────────────────────────────
        # GROUP_PLANS has planlimit + individualprice; members link via MEMBER_PLANS
        plans = conn.execute(f"""
            SELECT p.planname, gp.planlimit, COUNT(DISTINCT mp.memberid) as cnt,
                   gp.planlimit / NULLIF(gp.individualprice, 0) as ratio,
                   gp.individualprice
            FROM {SCHEMA}."GROUP_PLANS" gp
            JOIN {SCHEMA}."PLANS" p ON gp.planid = p.planid
            JOIN {SCHEMA}."MEMBER_PLANS" mp ON mp.planid = gp.planid AND mp.iscurrent = true
            JOIN {SCHEMA}."MEMBER" m ON mp.memberid = m.memberid
            WHERE gp.groupid = {data.group_id}
              AND gp.iscurrent = true
              AND m.groupid = {data.group_id}
              AND m.isterminated = false AND m.isdeleted = false
            GROUP BY p.planname, gp.planlimit, gp.individualprice
            ORDER BY cnt DESC
        """).fetchall()
        
        data.plans = [
            {
                "name": r[0], 
                "limit": float(r[1] or 0), 
                "count": int(r[2]),
                "ratio": round(float(r[3] or 0), 1),
                "premium": float(r[4] or 0)
            } for r in plans
        ]
        data.active_members = sum(p["count"] for p in data.plans)
        
        # Previous contract members (from debit notes or member count at that time)
        prev_row = conn.execute(f"""
            SELECT COUNT(DISTINCT m.memberid)
            FROM {SCHEMA}."MEMBER" m
            WHERE m.groupid = {data.group_id}
              AND m.registrationdate <= '{prev_end}'
        """).fetchone()
        data.prev_members = int(prev_row[0]) if prev_row else data.active_members
        
        # ─── DEBIT NOTES ─────────────────────────────────────────────
        debit_rows = conn.execute(f"""
            SELECT "RefNo", "Description", "Amount", "From", "To", "Date"
            FROM {SCHEMA}."DEBIT_NOTE"
            WHERE UPPER("CompanyName") LIKE UPPER('%{group_name_pattern}%')
              AND "Date" >= '{current_start}'
            ORDER BY "Date"
        """).fetchall()
        
        data.debit_notes = [
            {
                "ref": str(r[0]),
                "desc": str(r[1] or ""),
                "amount": float(r[2] or 0),
                "from": str(r[3]),
                "to": str(r[4]),
                "date": str(r[5])
            } for r in debit_rows
        ]
        data.total_debit = sum(d["amount"] for d in data.debit_notes)
        
        # ─── CASH RECEIVED ────────────────────────────────────────────
        # Current contract cash — MUST filter by contract dates to avoid pulling all-time cash
        curr_cash = 0
        for year in ["2024", "2025", "2026"]:
            try:
                rows = conn.execute(f"""
                    SELECT SUM(cash_amount) 
                    FROM {SCHEMA}."CLIENT_CASH_RECEIVED_{year}"
                    WHERE UPPER(groupname) LIKE UPPER('%{group_name_pattern}%')
                      AND CAST("Date" AS DATE) >= '{current_start}'
                      AND CAST("Date" AS DATE) <= '{current_end}'
                """).fetchone()
                if rows and rows[0]:
                    curr_cash += float(rows[0])
            except:
                pass
        data.cash_received = curr_cash
        
        # Previous contract cash — filter to previous contract period only
        prev_cash = 0
        for year in ["2023", "2024", "2025"]:
            try:
                rows = conn.execute(f"""
                    SELECT SUM(cash_amount) 
                    FROM {SCHEMA}."CLIENT_CASH_RECEIVED_{year}"
                    WHERE UPPER(groupname) LIKE UPPER('%{group_name_pattern}%')
                      AND CAST("Date" AS DATE) >= '{prev_start}'
                      AND CAST("Date" AS DATE) < '{current_start}'
                """).fetchone()
                if rows and rows[0]:
                    prev_cash += float(rows[0])
            except:
                pass
        data.prev_cash = prev_cash
        
        # Previous debit total
        prev_debit_rows = conn.execute(f"""
            SELECT SUM("Amount")
            FROM {SCHEMA}."DEBIT_NOTE"
            WHERE UPPER("CompanyName") LIKE UPPER('%{group_name_pattern}%')
              AND "Date" >= '{prev_start}' AND "Date" < '{current_start}'
        """).fetchone()
        data.prev_debit = float(prev_debit_rows[0] or 0) if prev_debit_rows else 0
        
        # ─── CLAIMS DATA ─────────────────────────────────────────────
        # Filter by encounterdatefrom (service date) within client contract period
        claims_row = conn.execute(f"""
            SELECT COUNT(*), SUM(CAST(approvedamount AS FLOAT)),
                   COUNT(DISTINCT enrollee_id)
            FROM {SCHEMA}."CLAIMS DATA"
            WHERE nhisgroupid = '{data.group_id}'
              AND CAST(encounterdatefrom AS DATE) >= '{current_start}'
              AND CAST(encounterdatefrom AS DATE) <= '{current_end}'
        """).fetchone()
        
        if claims_row and claims_row[1]:
            data.claims_count = int(claims_row[0])
            data.claims_total = float(claims_row[1])
            data.members_utilizing = int(claims_row[2])
        
        # Monthly breakdown of claims
        monthly = conn.execute(f"""
            SELECT DATE_TRUNC('month', CAST(encounterdatefrom AS DATE)) as month,
                   COUNT(*) as cnt,
                   SUM(CAST(approvedamount AS FLOAT)) as total
            FROM {SCHEMA}."CLAIMS DATA"
            WHERE nhisgroupid = '{data.group_id}'
              AND CAST(encounterdatefrom AS DATE) >= '{current_start}'
              AND CAST(encounterdatefrom AS DATE) <= '{current_end}'
            GROUP BY 1 ORDER BY 1
        """).fetchall()
        
        # Monthly PA amounts (to show PA Granted alongside claims)
        monthly_pa = conn.execute(f"""
            SELECT DATE_TRUNC('month', CAST(requestdate AS DATE)) as month,
                   SUM(granted) as pa_total
            FROM {SCHEMA}."PA DATA"
            WHERE groupname LIKE '%{group_name_pattern}%'
              AND CAST(requestdate AS DATE) >= '{current_start}'
              AND CAST(requestdate AS DATE) <= '{current_end}'
              AND pastatus = 'AUTHORIZED'
            GROUP BY 1 ORDER BY 1
        """).fetchall()
        
        pa_by_month = {str(r[0])[:7]: float(r[1]) for r in monthly_pa if r[1]}
        
        monthly_list = [
            {"month": str(r[0])[:7], "count": int(r[1]), "amount": float(r[2]),
             "pa_amount": pa_by_month.get(str(r[0])[:7], 0)}
            for r in monthly
        ]
        
        # Identify spike months as potential drivers
        if monthly_list:
            avg_m = sum(m["amount"] for m in monthly_list) / len(monthly_list)
            for m in monthly_list:
                if m["amount"] > avg_m * 2:
                    m["driver"] = "⚠ HIGH SPIKE"
                elif m["amount"] > avg_m * 1.3:
                    m["driver"] = "Above average"
                else:
                    m["driver"] = "Routine"
        
        data.monthly_claims = monthly_list
        
        # Previous claims (encounterdatefrom within previous contract period)
        prev_claims_row = conn.execute(f"""
            SELECT SUM(CAST(approvedamount AS FLOAT)),
                   COUNT(DISTINCT enrollee_id)
            FROM {SCHEMA}."CLAIMS DATA"
            WHERE nhisgroupid = '{data.group_id}'
              AND CAST(encounterdatefrom AS DATE) >= '{prev_start}'
              AND CAST(encounterdatefrom AS DATE) < '{current_start}'
        """).fetchone()
        
        if prev_claims_row and prev_claims_row[0]:
            data.prev_claims = float(prev_claims_row[0])
            data.prev_members_utilizing = int(prev_claims_row[1])
        
        # ─── PA DATA ─────────────────────────────────────────────────
        pa_row = conn.execute(f"""
            SELECT COUNT(*), SUM(granted)
            FROM {SCHEMA}."PA DATA"
            WHERE groupname LIKE '%{group_name_pattern}%'
              AND CAST(requestdate AS DATE) >= '{current_start}'
              AND CAST(requestdate AS DATE) <= '{current_end}'
              AND pastatus = 'AUTHORIZED'
        """).fetchone()
        
        if pa_row and pa_row[1]:
            data.pa_count = int(pa_row[0])
            data.pa_total_authorized = float(pa_row[1])
        
        # Previous PA
        prev_pa_row = conn.execute(f"""
            SELECT SUM(granted)
            FROM {SCHEMA}."PA DATA"
            WHERE groupname LIKE '%{group_name_pattern}%'
              AND requestdate >= '{prev_start}' AND requestdate < '{current_start}'
              AND pastatus = 'AUTHORIZED'
        """).fetchone()
        data.prev_pa = float(prev_pa_row[0] or 0) if prev_pa_row and prev_pa_row[0] else 0
        
        # ─── UNCLAIMED PA ─────────────────────────────────────────────
        # PA granted within contract period (requestdate) that do NOT have a
        # matching claim (by panumber) with encounterdatefrom also within contract period.
        # This is the correct numerator component: Claims(encounterdatefrom) + Unclaimed PA(requestdate)
        # Both date anchors must be within the same client contract period.
        try:
            unclaimed_pa_row = conn.execute(f"""
                SELECT SUM(pa.granted) as unclaimed_amount, COUNT(*) as unclaimed_count
                FROM {SCHEMA}."PA DATA" pa
                WHERE pa.groupname LIKE '%{group_name_pattern}%'
                  AND CAST(pa.requestdate AS DATE) >= '{current_start}'
                  AND CAST(pa.requestdate AS DATE) <= '{current_end}'
                  AND pa.pastatus = 'AUTHORIZED'
                  AND pa.panumber IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1
                      FROM {SCHEMA}."CLAIMS DATA" cd
                      WHERE CAST(cd.panumber AS BIGINT) = CAST(pa.panumber AS BIGINT)
                        AND cd.nhisgroupid = '{data.group_id}'
                        AND CAST(cd.encounterdatefrom AS DATE) >= '{current_start}'
                        AND CAST(cd.encounterdatefrom AS DATE) <= '{current_end}'
                  )
            """).fetchone()
            data.unclaimed_pa = float(unclaimed_pa_row[0] or 0) if unclaimed_pa_row and unclaimed_pa_row[0] else 0
            data.unclaimed_pa_count = int(unclaimed_pa_row[1] or 0) if unclaimed_pa_row and unclaimed_pa_row[1] else 0
        except Exception as e:
            # Fallback if panumber type cast fails — skip unclaimed PA
            data.unclaimed_pa = 0
            data.unclaimed_pa_count = 0
        
        # ─── TOP MEMBERS ──────────────────────────────────────────────
        top_members = conn.execute(f"""
            SELECT cd.enrollee_id,
                   MAX(m.firstname || ' ' || m.lastname) as name,
                   MAX(m.genderid) as genderid,
                   EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM CAST(MAX(m.dob) AS DATE)) as age,
                   SUM(CAST(cd.approvedamount AS FLOAT)) as total,
                   COUNT(DISTINCT cd.diagnosiscode) as conditions
            FROM {SCHEMA}."CLAIMS DATA" cd
            LEFT JOIN {SCHEMA}."MEMBER" m ON SPLIT_PART(cd.enrollee_id, '/', 3) = CAST(m.memberid AS VARCHAR)
            WHERE cd.nhisgroupid = '{data.group_id}'
              AND CAST(cd.encounterdatefrom AS DATE) >= '{current_start}'
              AND CAST(cd.encounterdatefrom AS DATE) <= '{current_end}'
            GROUP BY cd.enrollee_id
            ORDER BY total DESC
            LIMIT 10
        """).fetchall()
        
        # Get primary diagnosis per top member (most costly condition)
        top_iids = [str(r[0]) for r in top_members[:10]]
        member_dx = {}
        if top_iids:
            iid_list = "', '".join(top_iids)
            try:
                dx_rows = conn.execute(f"""
                    SELECT cd.enrollee_id, cd.diagnosiscode,
                           MAX(COALESCE(d.diagnosisdesc, cd.diagnosiscode)) as dxname,
                           SUM(CAST(cd.approvedamount AS FLOAT)) as total
                    FROM {SCHEMA}."CLAIMS DATA" cd
                    LEFT JOIN {SCHEMA}."DIAGNOSIS" d ON cd.diagnosiscode = d.diagnosiscode
                    WHERE cd.enrollee_id IN ('{iid_list}')
                      AND cd.nhisgroupid = '{data.group_id}'
                      AND CAST(cd.encounterdatefrom AS DATE) >= '{current_start}'
                      AND CAST(cd.encounterdatefrom AS DATE) <= '{current_end}'
                    GROUP BY cd.enrollee_id, cd.diagnosiscode
                    ORDER BY cd.enrollee_id, total DESC
                """).fetchall()
                
                # Keep only the top (most costly) diagnosis per member
                seen = set()
                for r in dx_rows:
                    iid = str(r[0])
                    if iid not in seen:
                        member_dx[iid] = {"code": str(r[1]), "name": str(r[2]), "amount": float(r[3])}
                        seen.add(iid)
            except:
                pass
        
        # Classify nature based on ICD code
        ACUTE_SURGICAL = {"S", "D2", "O", "T"}  # fractures, surgical, obstetric, trauma
        CHRONIC_CODES_SET = {"I10", "I11", "I119", "I12", "I13", "I20", "I209", "I25",
                             "E11", "E14", "J44", "J45", "N18", "K74", "G20"}
        
        def classify_nature(code: str) -> str:
            if not code:
                return "MIXED"
            code_up = code.upper()
            if any(code_up.startswith(c) for c in CHRONIC_CODES_SET):
                return "⚠ CHRONIC/FREQUENT"
            if code_up.startswith("S") or code_up.startswith("T"):
                return "⚠ ACUTE/EPISODIC"
            if code_up.startswith("D2") or code_up.startswith("O"):
                return "⚠ ACUTE/EPISODIC"
            if code_up.startswith("A") or code_up.startswith("B"):
                return "INFECTIOUS/ENDEMIC"
            return "ACUTE"
        
        # Denominator for member/diagnosis/plan % breakdowns = claims_total only.
        # Unclaimed PA cannot be broken down by member or diagnosis (it's still pending),
        # so percentages here reflect share of PAID claims only.
        # MLR numerator (claims + unclaimed PA) is calculated separately below.
        total_claims = data.claims_total or 1
        data.top_members = []
        for r in top_members:
            iid = str(r[0])
            dx = member_dx.get(iid, {"code": "", "name": "Multiple", "amount": 0})
            nature = classify_nature(dx["code"])
            gender_map = {1: "M", 2: "F"}
            genderid = int(r[2]) if r[2] else 0
            data.top_members.append({
                "iid": iid,
                "name": str(r[1] or "Unknown"),
                "gender": gender_map.get(genderid, "?"),
                "age": int(r[3] or 0) if r[3] else 0,
                "amount": float(r[4]),
                "pct": round(float(r[4]) / total_claims * 100, 1),
                "primary_condition": f"{dx['code']} — {dx['name'][:30]}" if dx["code"] else "Multiple conditions",
                "nature": nature,
            })
        
        if data.top_members:
            top5_total = sum(m["amount"] for m in data.top_members[:5])
            data.top5_pct = round(top5_total / total_claims * 100, 1)
        
        # ─── PLAN UTILIZATION TABLE ───────────────────────────────────
        # Avg spend per member per plan (for Plan Limit Utilisation section)
        plan_util_rows = conn.execute(f"""
            SELECT p.planname, gp.planlimit, gp.individualprice,
                   COUNT(DISTINCT m.memberid) as members,
                   SUM(CAST(cd.approvedamount AS FLOAT)) as total_claims
            FROM {SCHEMA}."GROUP_PLANS" gp
            JOIN {SCHEMA}."PLANS" p ON gp.planid = p.planid
            JOIN {SCHEMA}."MEMBER_PLANS" mp ON mp.planid = gp.planid AND mp.iscurrent = true
            JOIN {SCHEMA}."MEMBER" m ON mp.memberid = m.memberid
            LEFT JOIN {SCHEMA}."CLAIMS DATA" cd
                ON SPLIT_PART(cd.enrollee_id, '/', 3) = CAST(m.memberid AS VARCHAR)
                AND cd.nhisgroupid = '{data.group_id}'
                AND CAST(cd.encounterdatefrom AS DATE) >= '{current_start}'
                AND CAST(cd.encounterdatefrom AS DATE) <= '{current_end}'
            WHERE gp.groupid = {data.group_id}
              AND gp.iscurrent = true
              AND m.groupid = {data.group_id}
              AND m.isterminated = false AND m.isdeleted = false
            GROUP BY p.planname, gp.planlimit, gp.individualprice
            ORDER BY members DESC
        """).fetchall()
        
        data.plan_utilization = []
        for r in plan_util_rows:
            limit = float(r[1] or 0)
            premium = float(r[2] or 0)
            members = int(r[3] or 1)
            total_cl = float(r[4] or 0)
            avg_spend = round(total_cl / members, 0) if members > 0 else 0
            limit_pct = round(avg_spend / limit * 100, 1) if limit > 0 else 0
            
            if limit_pct > 100:
                over_risk = "⚠ EXCEEDED — member over limit"
            elif limit_pct > 50:
                over_risk = "⚠ HIGH — monitor closely"
            elif limit > premium * 20:
                over_risk = "Low under normal use (HIGH limit risk)"
            else:
                over_risk = "Low under normal use"
            
            data.plan_utilization.append({
                "name": str(r[0]),
                "limit": limit,
                "premium": premium,
                "members": members,
                "avg_spend": avg_spend,
                "limit_pct": limit_pct,
                "over_risk": over_risk,
            })
        
        # ─── TOP PROVIDERS ────────────────────────────────────────────
        # Uses PA requestdate within contract period (PA is the source of provider data)
        top_prov = conn.execute(f"""
            SELECT p.providername,
                   COUNT(*) as pa_count,
                   SUM(pa.granted) as total,
                   ROUND(SUM(pa.granted) * 100.0 / NULLIF(
                       (SELECT SUM(granted) FROM {SCHEMA}."PA DATA"
                        WHERE groupname LIKE '%{group_name_pattern}%'
                          AND CAST(requestdate AS DATE) >= '{current_start}'
                          AND CAST(requestdate AS DATE) <= '{current_end}'
                          AND pastatus = 'AUTHORIZED'), 0), 2) as pct
            FROM {SCHEMA}."PA DATA" pa
            LEFT JOIN {SCHEMA}."PROVIDERS" p ON pa.providerid = p.providerid
            WHERE pa.groupname LIKE '%{group_name_pattern}%'
              AND CAST(pa.requestdate AS DATE) >= '{current_start}'
              AND CAST(pa.requestdate AS DATE) <= '{current_end}'
              AND pa.pastatus = 'AUTHORIZED'
            GROUP BY p.providername
            ORDER BY total DESC
            LIMIT 15
        """).fetchall()
        
        data.top_providers = [
            {
                "name": str(r[0] or "Unknown"),
                "pa_count": int(r[1]),
                "amount": float(r[2]),
                "pct": float(r[3] or 0)
            } for r in top_prov
        ]
        
        # Previous contract top providers
        prev_prov = conn.execute(f"""
            SELECT p.providername, SUM(pa.granted) as total
            FROM {SCHEMA}."PA DATA" pa
            LEFT JOIN {SCHEMA}."PROVIDERS" p ON pa.providerid = p.providerid
            WHERE pa.groupname LIKE '%{group_name_pattern}%'
              AND pa.requestdate >= '{prev_start}' AND pa.requestdate < '{current_start}'
              AND pa.pastatus = 'AUTHORIZED'
            GROUP BY p.providername
            ORDER BY total DESC
            LIMIT 10
        """).fetchall()
        
        data.prev_top_providers = [
            {"name": str(r[0] or "Unknown"), "amount": float(r[1])}
            for r in prev_prov
        ]
        
        # ─── TOP DIAGNOSES ────────────────────────────────────────────
        try:
            top_dx = conn.execute(f"""
                SELECT cd.diagnosiscode,
                       MAX(d.diagnosisdesc) as name,
                       COUNT(*) as cnt,
                       SUM(CAST(cd.approvedamount AS FLOAT)) as total
                FROM {SCHEMA}."CLAIMS DATA" cd
                LEFT JOIN {SCHEMA}."DIAGNOSIS" d ON cd.diagnosiscode = d.diagnosiscode
                WHERE cd.nhisgroupid = '{data.group_id}'
                  AND CAST(cd.encounterdatefrom AS DATE) >= '{current_start}'
                  AND CAST(cd.encounterdatefrom AS DATE) <= '{current_end}'
                GROUP BY cd.diagnosiscode
                ORDER BY total DESC
                LIMIT 15
            """).fetchall()
            
            data.top_diagnoses = [
                {
                    "code": str(r[0]),
                    "name": str(r[1] or r[0]),
                    "count": int(r[2]),
                    "amount": float(r[3]),
                    "pct": round(float(r[3]) / total_claims * 100, 1)
                } for r in top_dx
            ]
        except:
            data.top_diagnoses = []
        
        # ─── STRATEGY INTELLIGENCE DATA ──────────────────────────────
        # 1. Repeat high-cost members: appeared as high-cost in BOTH contract periods
        try:
            repeat_rows = conn.execute(f"""
                SELECT 
                    SPLIT_PART(cd.enrollee_id, '/', 3) as mid,
                    ANY_VALUE(cd.enrollee_id) as iid,
                    MAX(m.firstname || ' ' || m.lastname) as name,
                    SUM(CASE WHEN CAST(cd.encounterdatefrom AS DATE) >= '{prev_start}'
                              AND CAST(cd.encounterdatefrom AS DATE) < '{current_start}'
                         THEN cd.approvedamount ELSE 0 END) as prev_amount,
                    SUM(CASE WHEN CAST(cd.encounterdatefrom AS DATE) >= '{current_start}'
                              AND CAST(cd.encounterdatefrom AS DATE) <= '{current_end}'
                         THEN cd.approvedamount ELSE 0 END) as curr_amount
                FROM {SCHEMA}."CLAIMS DATA" cd
                LEFT JOIN {SCHEMA}."MEMBER" m 
                    ON SPLIT_PART(cd.enrollee_id, '/', 3) = CAST(m.memberid AS VARCHAR)
                WHERE cd.nhisgroupid = '{data.group_id}'
                  AND CAST(cd.encounterdatefrom AS DATE) >= '{prev_start}'
                GROUP BY SPLIT_PART(cd.enrollee_id, '/', 3)
                HAVING prev_amount > 50000 AND curr_amount > 50000
                ORDER BY curr_amount DESC
                LIMIT 10
            """).fetchall()
            
            data.repeat_high_cost_members = []
            for r in repeat_rows:
                prev_a = float(r[3] or 0)
                curr_a = float(r[4] or 0)
                trend = round((curr_a / prev_a - 1) * 100, 1) if prev_a > 0 else 999
                data.repeat_high_cost_members.append({
                    "iid": str(r[1]),
                    "name": str(r[2] or "Unknown"),
                    "prev_amount": prev_a,
                    "curr_amount": curr_a,
                    "trend_pct": trend,
                    "risk": "ESCALATING" if trend > 30 else ("PERSISTENT" if trend >= -20 else "DECLINING")
                })
        except Exception as e:
            data.repeat_high_cost_members = []
        
        # 2. Provider dual-period analysis with per-PA averages
        try:
            prov_dual = conn.execute(f"""
                SELECT 
                    p.providername,
                    COUNT(CASE WHEN CAST(pa.requestdate AS DATE) >= '{prev_start}'
                               AND CAST(pa.requestdate AS DATE) < '{current_start}' THEN 1 END) as prev_count,
                    SUM(CASE WHEN CAST(pa.requestdate AS DATE) >= '{prev_start}'
                              AND CAST(pa.requestdate AS DATE) < '{current_start}'
                         THEN pa.granted ELSE 0 END) as prev_amount,
                    COUNT(CASE WHEN CAST(pa.requestdate AS DATE) >= '{current_start}'
                               AND CAST(pa.requestdate AS DATE) <= '{current_end}' THEN 1 END) as curr_count,
                    SUM(CASE WHEN CAST(pa.requestdate AS DATE) >= '{current_start}'
                              AND CAST(pa.requestdate AS DATE) <= '{current_end}'
                         THEN pa.granted ELSE 0 END) as curr_amount
                FROM {SCHEMA}."PA DATA" pa
                LEFT JOIN {SCHEMA}."PROVIDERS" p ON pa.providerid = p.providerid
                WHERE pa.groupname LIKE '%{group_name_pattern}%'
                  AND pa.pastatus = 'AUTHORIZED'
                  AND CAST(pa.requestdate AS DATE) >= '{prev_start}'
                GROUP BY p.providername
                HAVING curr_amount > 0 OR prev_amount > 0
                ORDER BY curr_amount DESC
                LIMIT 15
            """).fetchall()
            
            data.provider_dual_period = []
            for r in prov_dual:
                pc = int(r[1] or 0); pa_prev = float(r[2] or 0)
                cc = int(r[3] or 0); ca = float(r[4] or 0)
                avg_curr = round(ca / cc, 0) if cc > 0 else 0
                avg_prev = round(pa_prev / pc, 0) if pc > 0 else 0
                growth = round((ca / pa_prev - 1) * 100, 1) if pa_prev > 0 else 999
                # Flag logic
                flag = ""
                if cc > 0 and pc == 0:
                    flag = "NEW_PROVIDER"
                if avg_curr > 80000:
                    flag = flag + "|HIGH_AVG_PA" if flag else "HIGH_AVG_PA"
                if growth > 80 and pa_prev > 50000:
                    flag = flag + "|RAPID_GROWTH" if flag else "RAPID_GROWTH"
                data.provider_dual_period.append({
                    "name": str(r[0] or "Unknown"),
                    "prev_count": pc, "prev_amount": pa_prev,
                    "curr_count": cc, "curr_amount": ca,
                    "avg_per_pa_curr": avg_curr,
                    "avg_per_pa_prev": avg_prev,
                    "growth_pct": growth,
                    "flag": flag
                })
        except Exception as e:
            data.provider_dual_period = []
        
        # 3. 80th percentile spend threshold
        if data.top_members:
            amounts = sorted([m["amount"] for m in data.top_members])
            idx = int(len(amounts) * 0.8)
            data.high_cost_threshold = amounts[min(idx, len(amounts)-1)]
        
        # ─── CALCULATE METRICS ────────────────────────────────────────
        months = data.claims_months or 5.5
        
        # MLR numerator: claims (encounterdatefrom in contract) + unclaimed PA (requestdate in contract)
        # This is the correct actuarial numerator — matches Clearline MLR standard
        mlr_numerator = data.claims_total + data.unclaimed_pa
        
        data.annualized_claims = (mlr_numerator / months) * 12 if months > 0 else 0
        
        if data.total_debit > 0:
            data.projected_mlr = round(data.annualized_claims / data.total_debit * 100, 1)
            data.ytd_mlr = round(mlr_numerator / data.total_debit * 100, 1)
        
        if data.cash_received > 0:
            data.cash_mlr = round(mlr_numerator / data.cash_received * 100, 1)
        
        if data.prev_debit > 0:
            data.prev_mlr = round(data.prev_claims / data.prev_debit * 100, 1)
        
        # PMPM analysis
        if data.prev_members > 0 and data.prev_claims > 0:
            data.prev_pmpm = round(data.prev_claims / (data.prev_members * 12), 0)
        
        if data.active_members > 0 and data.claims_total > 0:
            data.curr_pmpm = round(data.claims_total / (data.active_members * months), 0)
        
        # Actuarial premium based on PMPM (70% target MLR)
        pmpm_base = data.prev_pmpm or data.curr_pmpm
        data.actuarial_premium = round((pmpm_base * 12) / 0.70, 0)
        
        # Chronic disease load
        CHRONIC_CODES = ["I10", "I11", "I119", "I12", "I13", "I20", "I209", "I25",
                         "E11", "E14", "J44", "J45", "N18", "K74", "G20"]
        chronic_total = sum(
            d["amount"] for d in data.top_diagnoses
            if any(d["code"].startswith(c) for c in CHRONIC_CODES)
        )
        data.chronic_pct = round(chronic_total / total_claims * 100, 1) if total_claims > 0 else 0
        
        # SRS classification
        if data.top5_pct > 40:
            data.srs_classification = "EPISODIC"
        elif data.chronic_pct > 30:
            data.srs_classification = "STRUCTURAL"
        else:
            data.srs_classification = "MIXED"
    
    finally:
        conn.close()
    
    return data