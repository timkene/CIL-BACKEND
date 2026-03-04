"""
Hospital Band Analysis API – same endpoints as banding_main.py.
Mounted under /api/v1/banding so the React app can call one server.
"""
from __future__ import annotations

import io
from datetime import datetime
from typing import Optional

import pandas as pd
from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from pydantic import BaseModel

# Banding logic lives in project root
from banding_service import (
    build_reality_tariff,
    calculate_banding_summary_basic,
    get_engines,
    get_provider_tariff,
    invalidate_cache,
    load_provider_list,
)

router = APIRouter()


# ---------------------------------------------------------------------------
# Response models (mirror banding_main.py)
# ---------------------------------------------------------------------------

class BandDetail(BaseModel):
    band: str
    calculated_band: str
    avg: float
    method: str
    band_distribution: dict


class TariffBandResult(BaseModel):
    weighted: BandDetail
    unweighted: BandDetail
    matched_procedures: int
    coverage_pct: float
    fraud_risk: Optional[str] = None


class PricingBehaviour(BaseModel):
    flag: str
    claims_based_procedures: int
    tariff_based_procedures: int
    claims_coverage_pct: float
    overcharging_pct: float
    undercharging_pct: float
    total_overcharge_amount: float
    total_undercharge_amount: float


class RealityBandResult(BaseModel):
    weighted: BandDetail
    unweighted: BandDetail
    matched_procedures: int
    coverage_pct: float
    confidence: float
    pricing_behaviour: PricingBehaviour
    fraud_risk: Optional[str] = None


class BandChanges(BaseModel):
    tariff_weighted_vs_current: bool
    tariff_unweighted_vs_current: bool
    reality_weighted_vs_current: Optional[bool] = None
    reality_unweighted_vs_current: Optional[bool] = None
    tariff_vs_reality_weighted: Optional[bool] = None


class AnalysisResponse(BaseModel):
    provider_name: str
    provider_id: Optional[str]
    current_band: str
    tariff_banding: TariffBandResult
    reality_banding: Optional[RealityBandResult]
    band_changes: BandChanges
    analysis_source: str
    lookback_months: Optional[int]
    analysis_timestamp: str


# ---------------------------------------------------------------------------
# Endpoints (path is relative to prefix /api/v1/banding)
# ---------------------------------------------------------------------------

def _df_to_records(df: pd.DataFrame):
    """Convert DataFrame to JSON-serializable list of dicts (native Python types)."""
    if df is None or df.empty:
        return []
    records = df.to_dict(orient="records")
    out = []
    for r in records:
        row = {}
        for k, v in r.items():
            if hasattr(v, "item") and callable(getattr(v, "item")):
                row[k] = v.item()
            elif pd.isna(v):
                row[k] = None
            elif isinstance(v, pd.Timestamp):
                row[k] = str(v)
            else:
                row[k] = v
        out.append(row)
    return out


@router.get("/providers", summary="List all providers with tariff data")
def list_providers():
    """Returns providerid, providername, current_band for all eligible providers."""
    try:
        df = load_provider_list()
        return _df_to_records(df)
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/analyze",
    response_model=AnalysisResponse,
    summary="Analyze provider banding (existing or new via CSV)",
)
async def analyze_provider(
    provider_name: Optional[str] = Form(None),
    lookback_months: int = Form(6),
    csv_file: Optional[UploadFile] = File(None),
):
    if not provider_name and not csv_file:
        raise HTTPException(status_code=422, detail="Provide either provider_name or csv_file.")

    tariff_engine, reality_engine = get_engines()

    if provider_name and not csv_file:
        try:
            providers_df = load_provider_list()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Could not load provider list: {e}")

        match = providers_df[
            providers_df["providername"].str.lower().str.strip()
            == provider_name.lower().strip()
        ]
        if match.empty:
            raise HTTPException(
                status_code=404,
                detail=f"Provider '{provider_name}' not found. Use GET /providers.",
            )

        provider_row = match.iloc[0]
        prov_id = str(provider_row["providerid"])
        current_band = str(provider_row["current_band"])
        display_name = str(provider_row["providername"])

        tariff_df = get_provider_tariff(prov_id)
        if tariff_df is None or tariff_df.empty:
            raise HTTPException(status_code=404, detail=f"No tariff data found for '{display_name}'")

        reality_df = build_reality_tariff(prov_id, reality_engine.standard_df, lookback_months)
        source = "existing_provider"
    else:
        contents = await csv_file.read()
        try:
            tariff_df = pd.read_csv(io.BytesIO(contents), thousands=",")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Could not parse CSV: {e}")

        required_cols = {"procedurecode", "tariffamount"}
        missing = required_cols - set(tariff_df.columns.str.lower().str.strip())
        if missing:
            raise HTTPException(
                status_code=400,
                detail=f"CSV missing required columns: {missing}. Found: {list(tariff_df.columns)}",
            )
        tariff_df.columns = tariff_df.columns.str.lower().str.strip()

        display_name = provider_name or (csv_file.filename or "Unknown")
        prov_id = None
        current_band = "N/A"
        reality_df = pd.DataFrame()
        source = "csv_upload"

    t_result = tariff_engine.analyze(tariff_df, provider_id=prov_id)
    if not t_result["success"]:
        raise HTTPException(status_code=500, detail=f"Tariff banding failed: {t_result.get('error')}")

    tariff_banding = TariffBandResult(
        weighted=BandDetail(**t_result["weighted"]),
        unweighted=BandDetail(**t_result["unweighted"]),
        matched_procedures=t_result["matched_procedures"],
        coverage_pct=t_result["coverage_pct"],
        fraud_risk=t_result["fraud"].get("fraud_risk") if t_result["fraud"].get("available") else None,
    )

    reality_banding: Optional[RealityBandResult] = None
    if not reality_df.empty:
        r_result = reality_engine.analyze(reality_df, provider_name=display_name)
        if r_result["success"]:
            pb = r_result["pricing_behaviour"]
            reality_banding = RealityBandResult(
                weighted=BandDetail(**r_result["weighted"]),
                unweighted=BandDetail(**r_result["unweighted"]),
                matched_procedures=r_result["matched_procedures"],
                coverage_pct=r_result["coverage_pct"],
                confidence=r_result["confidence"],
                pricing_behaviour=PricingBehaviour(**pb),
                fraud_risk=r_result["fraud"].get("fraud_risk") if r_result["fraud"].get("available") else None,
            )

    tw_band = t_result["weighted"]["band"]
    tu_band = t_result["unweighted"]["band"]
    rw_band = reality_banding.weighted.band if reality_banding else None
    ru_band = reality_banding.unweighted.band if reality_banding else None

    band_changes = BandChanges(
        tariff_weighted_vs_current=(tw_band != current_band),
        tariff_unweighted_vs_current=(tu_band != current_band),
        reality_weighted_vs_current=(rw_band != current_band) if rw_band else None,
        reality_unweighted_vs_current=(ru_band != current_band) if ru_band else None,
        tariff_vs_reality_weighted=(tw_band != rw_band) if rw_band else None,
    )

    return AnalysisResponse(
        provider_name=display_name,
        provider_id=prov_id,
        current_band=current_band,
        tariff_banding=tariff_banding,
        reality_banding=reality_banding,
        band_changes=band_changes,
        analysis_source=source,
        lookback_months=lookback_months if source == "existing_provider" else None,
        analysis_timestamp=datetime.now().isoformat(),
    )


@router.post("/cache/clear", summary="Clear engine cache")
def clear_cache():
    invalidate_cache()
    return {"message": "Cache cleared. Engines will reload on next request."}


@router.get(
    "/summary",
    summary="Compute full banding summary for all providers (heavy; for batch use)",
)
def get_banding_summary(lookback_months: int = 6):
    """
    Compute tariff + reality banding for ALL providers with tariff data.

    This endpoint is primarily for batch / admin use and may be slow,
    because it runs the banding engines for every provider.

    Typical production pattern:
    - A nightly job calls this once (or calls calculate_banding_summary_basic
      directly), writes the result into a DuckDB table such as
      \"AI DRIVEN DATA\".\"HOSPITAL_BANDING_SUMMARY\".
    - The Hospital Band UI and other APIs then read from that summary table
      for instant results instead of recomputing on each search.

    For now, this just returns the computed summary as JSON records.
    """
    try:
        df = calculate_banding_summary_basic(lookback_months=lookback_months)
        return _df_to_records(df)
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
