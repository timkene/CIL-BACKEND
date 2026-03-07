from pydantic import BaseModel, Field
from datetime import date
from typing import List, Optional


# ── Request ───────────────────────────────────────────────────────────────────
class MLRRequest(BaseModel):
    client_name: str  = Field(..., description="Full or partial group/company name (case-insensitive)")
    start_date:  date = Field(..., description="Contract start date  (YYYY-MM-DD)")
    end_date:    date = Field(..., description="Contract end date    (YYYY-MM-DD)")


# ── Sub-models ────────────────────────────────────────────────────────────────
class ProviderRow(BaseModel):
    rank:           int
    provider_id:    Optional[str]
    provider_name:  Optional[str]
    visit_count:    int
    claim_rows:     int
    total_cost:     float
    pct_of_total:   float

class EnrolleeRow(BaseModel):
    rank:           int
    enrollee_id:    str
    enrollee_name:  Optional[str]
    visit_count:    int
    claim_rows:     int
    total_cost:     float
    pct_of_total:   float

class ProcedureRow(BaseModel):
    rank:           int
    procedure_code: str
    procedure_desc: Optional[str]
    claim_count:    int
    total_cost:     float
    pct_of_total:   float


# ── Main response ─────────────────────────────────────────────────────────────
class MLRSummaryResponse(BaseModel):
    # ── Inputs echoed back ──
    client_name:  str
    start_date:   date
    end_date:     date

    # ── Premium / debit ──
    total_debit_amount:          float

    # ── Medical cost components ──
    actual_claims_cost:          float   # encounterdatefrom anchor
    unclaimed_pa_cost:           float   # authorized PAs with no matching claim
    total_actual_medical_cost:   float   # = actual_claims + unclaimed_pa
    claims_paid_cost:            float   # datesubmitted anchor

    # ── MLR ──
    actual_mlr:          float           # ratio  (e.g. 0.82)
    actual_mlr_pct:      str            # "82.00 %"
    claims_paid_mlr:     float
    claims_paid_mlr_pct: str
    mlr_status:          str            # PROFITABLE | WARNING | LOSS

    # ── PMPM ──
    enrolled_members:                int
    utilized_members:                int    # unique members with ≥1 claim in period
    member_utilization_pct:          float  # utilized / enrolled × 100
    contract_months:                 int
    elapsed_months:                  int    # months elapsed as of today (used for medical PMPM)
    member_months:                   int    # enrolled × elapsed_months
    actual_medical_cost_pmpm:        float
    claims_paid_medical_cost_pmpm:   float
    premium_pmpm:                    float  # based on full contract months

    # ── Top-10 tables ──
    top_10_providers_by_cost:    List[ProviderRow]
    top_10_providers_by_count:   List[ProviderRow]
    top_10_enrollees_by_cost:    List[EnrolleeRow]
    top_10_enrollees_by_count:   List[EnrolleeRow]
    top_10_procedures_by_cost:   List[ProcedureRow]
    top_10_procedures_by_count:  List[ProcedureRow]
