"""
Wrong Provider Detection API
==============================
Detects enrollees using hospitals they are not mapped/assigned to.

Routes
------
GET /enrollee/{enrollee_id}
    Full profile: mapped provider, plan details, wrong-provider claims.

GET /enrollee/{enrollee_id}/member-provider
    Fold 1 — MEMBER_PROVIDER table only.
    Returns the iscurrent=TRUE mapped provider + plan info.

GET /enrollee/{enrollee_id}/claims-check
    Fold 2 — CLAIMS DATA cross-check.
    Returns every claim where the actual provider ≠ mapped provider,
    with provider name, band, date, amount.

POST /bulk-check
    Run both folds across a list of enrollee_ids.

GET /group/{group_name}/wrong-provider-claims
    All wrong-provider claims for every enrollee in a group.

Run (from project root):
    uvicorn apis.wrong_provider.main:app --reload --port 8002
"""

import os
from typing import Optional, List
from datetime import date

import duckdb
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ── Config ───────────────────────────────────────────────────────────────────
DB_PATH = os.getenv("DUCKDB_PATH", "/Users/kenechukwuchukwuka/Downloads/DLT/ai_driven_data.duckdb")
SCHEMA  = "AI DRIVEN DATA"

app = FastAPI(
    title="Wrong Provider Detection API",
    description="Detects enrollees using hospitals they are not supposed to use.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── DB helper ─────────────────────────────────────────────────────────────────
def get_conn():
    return duckdb.connect(DB_PATH, read_only=True)


# ── Pydantic models ───────────────────────────────────────────────────────────
class MappedProvider(BaseModel):
    mapped_providerid: int
    mapped_provider_name: str
    provider_band: Optional[str]


class PlanInfo(BaseModel):
    plan_id: int
    plan_name: str
    plan_code: Optional[str]
    individual_price: Optional[float]
    family_price: Optional[float]


class EnrolleeMemberProviderResult(BaseModel):
    enrollee_id: str
    full_name: str
    member_id: int
    mapped_provider: Optional[MappedProvider]
    plan: Optional[PlanInfo]
    note: Optional[str] = None  # e.g. "Provider not in master list"


class WrongProviderClaim(BaseModel):
    enrollee_id: str
    encounter_date: Optional[date]
    actual_providerid: str
    actual_provider_name: str
    actual_provider_band: Optional[str]
    mapped_providerid: Optional[int]
    mapped_provider_name: Optional[str]
    procedure_code: Optional[str]
    diagnosis_code: Optional[str]
    diagnosis_description: Optional[str]
    approved_amount: float
    pa_number: Optional[str]
    is_wrong_provider: bool
    wrong_reason: Optional[str]


class EnrolleeFullCheck(BaseModel):
    enrollee_id: str
    full_name: str
    member_id: int
    mapped_provider: Optional[MappedProvider]
    plan: Optional[PlanInfo]
    wrong_provider_claims: List[WrongProviderClaim]
    total_wrong_provider_spend: float
    total_claims_spend: float
    wrong_provider_claim_count: int
    total_claim_count: int
    verdict: str  # CLEAN | WRONG_PROVIDER | NO_MAPPING | NOT_FOUND


class BulkCheckRequest(BaseModel):
    enrollee_ids: List[str]


# ── SQL helpers ───────────────────────────────────────────────────────────────

MEMBER_PROVIDER_SQL = f"""
SELECT
    m.enrollee_id,
    mem.firstname || ' ' || mem.lastname                AS fullname,
    mem.memberid                                        AS member_id,
    mp.providerid                                       AS mapped_providerid,
    p.providername                                      AS mapped_provider_name,
    p.bands                                             AS provider_band,
    CAST(mpl.planid AS BIGINT)                          AS plan_id,
    pl.planname,
    pl.plancode,
    gp.individualprice,
    gp.familyprice
FROM "{SCHEMA}"."MEMBER_PROVIDER" mp
JOIN "{SCHEMA}"."MEMBERS"      m   ON CAST(mp.memberid AS VARCHAR) = m.memberid
JOIN "{SCHEMA}"."MEMBER"       mem ON mp.memberid = mem.memberid
JOIN "{SCHEMA}"."MEMBER_PLANS" mpl ON mp.memberid = mpl.memberid
                                   AND mpl.iscurrent = TRUE
JOIN "{SCHEMA}"."PLANS"        pl  ON CAST(mpl.planid AS BIGINT) = pl.planid
LEFT JOIN "{SCHEMA}"."GROUP_PLANS" gp
    ON pl.planid = gp.planid
    AND gp.iscurrent = TRUE
    AND gp.groupid = mem.groupid
LEFT JOIN "{SCHEMA}"."PROVIDERS" p
    ON TRY_CAST(mp.providerid AS BIGINT) = TRY_CAST(p.protariffid AS BIGINT)
WHERE mp.iscurrent = TRUE
  AND UPPER(m.enrollee_id) = UPPER(?)
"""

CLAIMS_CHECK_SQL = f"""
SELECT
    cd.enrollee_id,
    CAST(cd.encounterdatefrom AS DATE)                  AS encounter_date,
    cd.nhisproviderid                                   AS actual_providerid,
    p_actual.providername                               AS actual_provider_name,
    p_actual.bands                                      AS actual_provider_band,
    cd.code                                             AS procedure_code,
    cd.diagnosiscode                                    AS diagnosis_code,
    diag.diagnosisdesc                                  AS diagnosis_description,
    CAST(cd.approvedamount AS FLOAT)                    AS approved_amount,
    CAST(CAST(cd.panumber AS BIGINT) AS VARCHAR)        AS pa_number,
    -- subquery: get the mapped providerid for this member
    mp_sub.mapped_providerid,
    p_mapped.providername                               AS mapped_provider_name
FROM "{SCHEMA}"."CLAIMS DATA" cd
JOIN "{SCHEMA}"."PROVIDERS" p_actual
    ON TRY_CAST(cd.nhisproviderid AS BIGINT) = TRY_CAST(p_actual.providerid AS BIGINT)
LEFT JOIN "{SCHEMA}"."DIAGNOSIS" diag ON cd.diagnosiscode = diag.diagnosiscode
-- Get the member's mapped provider via enrollee_id → memberid
LEFT JOIN (
    SELECT
        m2.enrollee_id,
        mp2.providerid  AS mapped_providerid
    FROM "{SCHEMA}"."MEMBER_PROVIDER" mp2
    JOIN "{SCHEMA}"."MEMBERS" m2 ON CAST(mp2.memberid AS VARCHAR) = m2.memberid
    WHERE mp2.iscurrent = TRUE
) mp_sub ON UPPER(cd.enrollee_id) = UPPER(mp_sub.enrollee_id)
LEFT JOIN "{SCHEMA}"."PROVIDERS" p_mapped
    ON TRY_CAST(mp_sub.mapped_providerid AS BIGINT) = TRY_CAST(p_mapped.protariffid AS BIGINT)
WHERE UPPER(cd.enrollee_id) = UPPER(?)
ORDER BY encounter_date DESC
"""


def _build_member_provider_result(row) -> tuple:
    """Returns (EnrolleeMemberProviderResult, member_id, mapped_providerid)"""
    (
        enrollee_id, fullname, member_id,
        mapped_providerid, mapped_provider_name, provider_band,
        plan_id, planname, plancode,
        individualprice, familyprice
    ) = row

    mapped = None
    note = None
    if mapped_providerid:
        if mapped_provider_name:
            mapped = MappedProvider(
                mapped_providerid=int(mapped_providerid),
                mapped_provider_name=mapped_provider_name,
                provider_band=provider_band,
            )
        else:
            note = f"Provider ID {mapped_providerid} assigned but not in master providers list"
            mapped = MappedProvider(
                mapped_providerid=int(mapped_providerid),
                mapped_provider_name="(Not in master list)",
                provider_band=None,
            )

    plan = None
    if plan_id:
        plan = PlanInfo(
            plan_id=int(plan_id),
            plan_name=planname or "",
            plan_code=plancode,
            individual_price=float(individualprice) if individualprice else None,
            family_price=float(familyprice) if familyprice else None,
        )

    result = EnrolleeMemberProviderResult(
        enrollee_id=enrollee_id,
        full_name=fullname.strip() if fullname else "",
        member_id=int(member_id),
        mapped_provider=mapped,
        plan=plan,
        note=note,
    )
    return result, int(member_id), int(mapped_providerid) if mapped_providerid else None


def _build_wrong_claims(
    claim_rows,
    mapped_providerid: Optional[int]
) -> tuple[List[WrongProviderClaim], float, float, int, int]:
    """
    Returns (wrong_claims, total_wrong_spend, total_spend, wrong_count, total_count)
    A claim is "wrong provider" when:
      - actual_providerid != mapped_providerid  (and both are known)
      - OR no mapping exists at all (no_mapping case)
    """
    wrong_claims = []
    total_wrong_spend = 0.0
    total_spend = 0.0
    total_count = 0
    wrong_count = 0

    for row in claim_rows:
        (
            enrollee_id, encounter_date, actual_providerid,
            actual_provider_name, actual_provider_band,
            procedure_code, diagnosis_code, diagnosis_description,
            approved_amount, pa_number,
            row_mapped_providerid, mapped_provider_name
        ) = row

        amount = float(approved_amount) if approved_amount else 0.0
        total_spend += amount
        total_count += 1

        actual_id_int = int(actual_providerid) if actual_providerid else None
        mapped_id_int = int(row_mapped_providerid) if row_mapped_providerid else None

        is_wrong = False
        wrong_reason = None

        if mapped_id_int is None:
            is_wrong = True
            wrong_reason = "No provider mapping found for enrollee"
        elif actual_id_int != mapped_id_int:
            is_wrong = True
            wrong_reason = (
                f"Claimed at {actual_provider_name or actual_providerid} "
                f"(ID {actual_providerid}) but mapped to "
                f"{mapped_provider_name or mapped_id_int} (ID {mapped_id_int})"
            )

        if is_wrong:
            wrong_count += 1
            total_wrong_spend += amount

        wrong_claims.append(WrongProviderClaim(
            enrollee_id=enrollee_id,
            encounter_date=encounter_date,
            actual_providerid=str(actual_providerid),
            actual_provider_name=actual_provider_name or "",
            actual_provider_band=actual_provider_band,
            mapped_providerid=mapped_id_int,
            mapped_provider_name=mapped_provider_name,
            procedure_code=procedure_code,
            diagnosis_code=diagnosis_code,
            diagnosis_description=diagnosis_description,
            approved_amount=amount,
            pa_number=pa_number if pa_number != "0" else None,
            is_wrong_provider=is_wrong,
            wrong_reason=wrong_reason,
        ))

    return wrong_claims, total_wrong_spend, total_spend, wrong_count, total_count


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "service": "wrong-provider-detection"}


@app.get(
    "/enrollee/{enrollee_id:path}/member-provider",
    response_model=EnrolleeMemberProviderResult,
    summary="Fold 1 — Mapped provider & plan info from MEMBER_PROVIDER",
    tags=["Fold 1 — Member Provider"],
)
def get_member_provider(enrollee_id: str):
    conn = get_conn()
    try:
        rows = conn.execute(MEMBER_PROVIDER_SQL, [enrollee_id]).fetchall()
    finally:
        conn.close()

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"Enrollee '{enrollee_id}' not found or has no current provider mapping.",
        )

    result, _, _ = _build_member_provider_result(rows[0])
    return result


@app.get(
    "/enrollee/{enrollee_id:path}/claims-check",
    response_model=List[WrongProviderClaim],
    summary="Fold 2 — Claims cross-check against mapped provider",
    tags=["Fold 2 — Claims Check"],
)
def get_claims_check(
    enrollee_id: str,
    wrong_only: bool = Query(False, description="If true, return only wrong-provider claims"),
):
    conn = get_conn()
    try:
        rows = conn.execute(CLAIMS_CHECK_SQL, [enrollee_id]).fetchall()
    finally:
        conn.close()

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No claims found for enrollee '{enrollee_id}'.",
        )

    mapped_id = int(rows[0][10]) if rows[0][10] else None
    claims, _, _, _, _ = _build_wrong_claims(rows, mapped_id)

    if wrong_only:
        claims = [c for c in claims if c.is_wrong_provider]

    return claims


@app.get(
    "/enrollee/{enrollee_id:path}",
    response_model=EnrolleeFullCheck,
    summary="Full check — both folds combined",
    tags=["Full Check"],
)
def get_enrollee_full_check(enrollee_id: str):
    conn = get_conn()
    try:
        mp_rows    = conn.execute(MEMBER_PROVIDER_SQL, [enrollee_id]).fetchall()
        claim_rows = conn.execute(CLAIMS_CHECK_SQL,    [enrollee_id]).fetchall()
    finally:
        conn.close()

    if not mp_rows and not claim_rows:
        raise HTTPException(status_code=404, detail=f"Enrollee '{enrollee_id}' not found.")

    mapped_provider   = None
    plan              = None
    full_name         = ""
    member_id         = 0
    mapped_providerid = None

    if mp_rows:
        mp_result, member_id, mapped_providerid = _build_member_provider_result(mp_rows[0])
        mapped_provider = mp_result.mapped_provider
        plan            = mp_result.plan
        full_name       = mp_result.full_name

    if not full_name and claim_rows:
        full_name = claim_rows[0][0]

    wrong_claims, total_wrong_spend, total_spend, wrong_count, total_count = (
        _build_wrong_claims(claim_rows, mapped_providerid)
    )

    if not mp_rows and not claim_rows:
        verdict = "NOT_FOUND"
    elif not mp_rows:
        verdict = "NO_MAPPING"
    elif wrong_count > 0:
        verdict = "WRONG_PROVIDER"
    else:
        verdict = "CLEAN"

    return EnrolleeFullCheck(
        enrollee_id=enrollee_id,
        full_name=full_name,
        member_id=member_id,
        mapped_provider=mapped_provider,
        plan=plan,
        wrong_provider_claims=wrong_claims,
        total_wrong_provider_spend=round(total_wrong_spend, 2),
        total_claims_spend=round(total_spend, 2),
        wrong_provider_claim_count=wrong_count,
        total_claim_count=total_count,
        verdict=verdict,
    )


@app.post(
    "/bulk-check",
    response_model=List[EnrolleeFullCheck],
    summary="Bulk full check for a list of enrollee IDs",
    tags=["Full Check"],
)
def bulk_check(request: BulkCheckRequest):
    results = []
    conn = get_conn()
    try:
        for eid in request.enrollee_ids:
            mp_rows    = conn.execute(MEMBER_PROVIDER_SQL, [eid]).fetchall()
            claim_rows = conn.execute(CLAIMS_CHECK_SQL,    [eid]).fetchall()

            if not mp_rows and not claim_rows:
                continue

            mapped_provider   = None
            plan              = None
            full_name         = eid
            member_id         = 0
            mapped_providerid = None

            if mp_rows:
                mp_result, member_id, mapped_providerid = _build_member_provider_result(mp_rows[0])
                mapped_provider = mp_result.mapped_provider
                plan            = mp_result.plan
                full_name       = mp_result.full_name

            wrong_claims, total_wrong_spend, total_spend, wrong_count, total_count = (
                _build_wrong_claims(claim_rows, mapped_providerid)
            )

            verdict = "NO_MAPPING" if not mp_rows else ("WRONG_PROVIDER" if wrong_count > 0 else "CLEAN")

            results.append(EnrolleeFullCheck(
                enrollee_id=eid,
                full_name=full_name,
                member_id=member_id,
                mapped_provider=mapped_provider,
                plan=plan,
                wrong_provider_claims=wrong_claims,
                total_wrong_provider_spend=round(total_wrong_spend, 2),
                total_claims_spend=round(total_spend, 2),
                wrong_provider_claim_count=wrong_count,
                total_claim_count=total_count,
                verdict=verdict,
            ))
    finally:
        conn.close()

    return results


@app.get(
    "/group/{group_name}/wrong-provider-claims",
    response_model=List[EnrolleeFullCheck],
    summary="All wrong-provider violations across an entire group",
    tags=["Group Scan"],
)
def get_group_wrong_providers(
    group_name: str,
    wrong_only: bool = Query(True, description="If true (default), return only enrollees with violations"),
):
    conn = get_conn()
    try:
        group_sql = f"""
        SELECT DISTINCT m.enrollee_id
        FROM "{SCHEMA}"."MEMBERS" m
        JOIN "{SCHEMA}"."GROUPS" g ON m.groupid = g.groupid
        WHERE UPPER(g.groupname) LIKE UPPER(?)
          AND m.iscurrent = TRUE
        """
        enrollee_rows = conn.execute(group_sql, [f"%{group_name}%"]).fetchall()
        enrollee_ids  = [r[0] for r in enrollee_rows]
    finally:
        conn.close()

    if not enrollee_ids:
        raise HTTPException(
            status_code=404,
            detail=f"No active enrollees found for group matching '{group_name}'.",
        )

    results = []
    conn = get_conn()
    try:
        for eid in enrollee_ids:
            mp_rows    = conn.execute(MEMBER_PROVIDER_SQL, [eid]).fetchall()
            claim_rows = conn.execute(CLAIMS_CHECK_SQL,    [eid]).fetchall()

            if not mp_rows and not claim_rows:
                continue

            mapped_provider   = None
            plan              = None
            full_name         = eid
            member_id         = 0
            mapped_providerid = None

            if mp_rows:
                mp_result, member_id, mapped_providerid = _build_member_provider_result(mp_rows[0])
                mapped_provider = mp_result.mapped_provider
                plan            = mp_result.plan
                full_name       = mp_result.full_name

            wrong_claims, total_wrong_spend, total_spend, wrong_count, total_count = (
                _build_wrong_claims(claim_rows, mapped_providerid)
            )

            verdict = "NO_MAPPING" if not mp_rows else ("WRONG_PROVIDER" if wrong_count > 0 else "CLEAN")

            if wrong_only and verdict not in ("WRONG_PROVIDER", "NO_MAPPING"):
                continue

            results.append(EnrolleeFullCheck(
                enrollee_id=eid,
                full_name=full_name,
                member_id=member_id,
                mapped_provider=mapped_provider,
                plan=plan,
                wrong_provider_claims=[c for c in wrong_claims if c.is_wrong_provider],
                total_wrong_provider_spend=round(total_wrong_spend, 2),
                total_claims_spend=round(total_spend, 2),
                wrong_provider_claim_count=wrong_count,
                total_claim_count=total_count,
                verdict=verdict,
            ))
    finally:
        conn.close()

    return sorted(results, key=lambda x: x.total_wrong_provider_spend, reverse=True)
