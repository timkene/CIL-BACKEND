from fastapi import APIRouter, HTTPException, Query
from datetime import date

from .models import MLRSummaryResponse
from .service import compute_mlr_summary

router = APIRouter()


@router.get(
    "/summary",
    response_model=MLRSummaryResponse,
    summary="Full MLR summary for a client and contract period",
    description="""
Returns all MLR metrics for a given client within the supplied date range:

| Metric | Anchor |
|--------|--------|
| Actual MLR | `encounterdatefrom` + unclaimed PA |
| Claims-Paid MLR | `datesubmitted` |
| PMPM | enrolled members × contract months |

**MLR status thresholds (Nigerian HMO standard)**
- `PROFITABLE` : MLR ≤ 70 %
- `WARNING`    : 70 % < MLR ≤ 75 %
- `LOSS`       : MLR > 75 %  (25 % overhead: 15 % admin + 10 % commission)
    """,
)
def get_mlr_summary(
    client_name: str = Query(..., description="Full or partial company name (case-insensitive)"),
    start_date:  date = Query(..., description="Contract start date (YYYY-MM-DD)"),
    end_date:    date = Query(..., description="Contract end date   (YYYY-MM-DD)"),
):
    if end_date < start_date:
        raise HTTPException(
            status_code=422,
            detail="end_date must be on or after start_date"
        )
    try:
        return compute_mlr_summary(client_name, start_date, end_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
