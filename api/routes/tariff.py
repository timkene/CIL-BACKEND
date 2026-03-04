"""
Tariff Analysis API Routes

Wraps the banding engines from:
- adv_hosp_band2.py  (EnhancedBandingEngine - standard tariff)
- adv_hosp_claims_band.py (RealityBandingEngine - claims-based / reality-adjusted)
"""

from typing import Optional, List, Dict, Any
import os

import pandas as pd
from fastapi import APIRouter, HTTPException
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, Field

from core.database import DB_PATH

# Import engines from existing Streamlit apps (logic only)
from adv_hosp_band2 import DuckDBDataLoader, EnhancedBandingEngine  # type: ignore
from adv_hosp_claims_band import (  # type: ignore
    RealityAdjustedTariffBuilder,
    RealityBandingEngine,
)

router = APIRouter()


REALITY_TARIFF_PATH = os.getenv(
    "REALITY_TARIFF_PATH",
    "/Users/kenechukwuchukwuka/Downloads/REALITY TARIFF.xlsx",
)


class TariffRow(BaseModel):
    """Single tariff row for standard tariff analysis."""

    procedurecode: str = Field(..., description="Procedure code e.g. DRG1081")
    tariffamount: float = Field(..., gt=0, description="Hospital tariff price (> 0)")


class TariffStandardRequest(BaseModel):
    """
    Standard tariff analysis (EnhancedBandingEngine from adv_hosp_band2.py).

    - mode='csv': provide tariff_rows (list of TariffRow) and optional provider_id.
    - mode='provider': provide provider_id; tariff is loaded from DB attachment.
    """

    mode: str = Field(..., pattern="^(csv|provider)$")
    provider_id: Optional[str] = Field(
        None, description="Provider ID (required when mode='provider')"
    )
    tariff_rows: Optional[List[TariffRow]] = Field(
        None,
        description="Tariff rows (required when mode='csv'). Must include procedurecode and tariffamount.",
    )
    include_quality: bool = Field(
        True, description="Include quality layer (if metrics available)."
    )
    include_fraud: bool = Field(
        True, description="Include fraud/P90-P95 layer (if claims stats available)."
    )


class TariffClaimsRequest(BaseModel):
    """
    Claims-based tariff analysis (RealityBandingEngine from adv_hosp_claims_band.py).
    """

    provider_id: str = Field(..., description="Provider ID with claims + tariff")
    lookback_months: int = Field(
        6,
        ge=1,
        le=24,
        description="How many months back to look at claims for reality pricing.",
    )


class ExistingRecategorizationRequest(BaseModel):
    """
    Combined view: standard tariff vs reality-adjusted (existing recategorization).
    """

    provider_id: str = Field(..., description="Provider ID")
    lookback_months: int = Field(
        6,
        ge=1,
        le=24,
        description="Months of claims to use for reality-adjusted pricing.",
    )


def _load_standard_tariff() -> pd.DataFrame:
    """Load the standard REALITY_TARIFF.xlsx used by both tariff engines."""
    try:
        df = pd.read_excel(REALITY_TARIFF_PATH)
        return df
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Could not load standard tariff from '{REALITY_TARIFF_PATH}': {e}",
        )


def _run_standard_analysis(req: TariffStandardRequest) -> Dict[str, Any]:
    """
    Run EnhancedBandingEngine (adv_hosp_band2.py) for:
    - CSV tariff (mode='csv')
    - Provider-attached tariff from DB (mode='provider')
    """
    standard_df = _load_standard_tariff()

    # Initialize data loader for claims/quality + tariff-from-DB
    loader = DuckDBDataLoader(db_path=DB_PATH)

    claims_stats = loader.load_claims_stats() if req.include_fraud else pd.DataFrame()
    quality_metrics = (
        loader.load_quality_metrics() if req.include_quality else pd.DataFrame()
    )

    engine_std = EnhancedBandingEngine(
        standard_tariff_df=standard_df,
        claims_stats=claims_stats,
        quality_metrics=quality_metrics,
    )

    # Build tariff_df based on mode
    tariff_df: Optional[pd.DataFrame] = None
    tariff_name = "uploaded_tariff"
    provider_id: Optional[str] = None

    if req.mode == "csv":
        if not req.tariff_rows:
            raise HTTPException(
                status_code=400,
                detail="tariff_rows is required when mode='csv'. "
                "Each row must include procedurecode and tariffamount (>0).",
            )
        rows = [r.dict() for r in req.tariff_rows]
        tariff_df = pd.DataFrame(rows)
        provider_id = req.provider_id
    else:  # mode == "provider"
        if not req.provider_id:
            raise HTTPException(
                status_code=400,
                detail="provider_id is required when mode='provider'.",
            )
        provider_id = req.provider_id
        tariff_df = loader.get_hospital_tariff(provider_id)
        tariff_name = f"Provider {provider_id}"

    if tariff_df is None or tariff_df.empty:
        # Try to get provider name for better error message
        provider_name = provider_id
        try:
            loader_temp = DuckDBDataLoader(db_path=DB_PATH)
            providers_df = loader_temp.load_hospital_list()
            provider_row = providers_df[providers_df["providerid"] == provider_id]
            if not provider_row.empty:
                provider_name = str(provider_row.iloc[0]["providername"])
        except Exception:
            pass

        raise HTTPException(
            status_code=404,
            detail=(
                f"Provider '{provider_name}' (ID: {provider_id}) does not have tariff data attached. "
                "Please use a provider from the dropdown (only providers with tariffs are shown), "
                "or upload a CSV file instead."
            ),
        )

    # Run dual banding analysis
    result = engine_std.analyze_tariff_dual(
        tariff_df=tariff_df,
        tariff_name=tariff_name,
        provider_id=provider_id,
    )

    if not result.get("success", False):
        raise HTTPException(
            status_code=400,
            detail=result.get("error", "Standard tariff analysis failed."),
        )

    return result


def _run_claims_analysis(req: TariffClaimsRequest) -> Dict[str, Any]:
    """
    Run RealityBandingEngine (adv_hosp_claims_band.py) for a provider.
    """
    provider_id = req.provider_id
    lookback = req.lookback_months

    standard_df = _load_standard_tariff()

    # Build reality-adjusted tariff from claims + official tariff
    tariff_builder = RealityAdjustedTariffBuilder(db_path=DB_PATH)

    # Get provider info (name + official band) for context
    providers_df = tariff_builder.get_providers_with_tariffs()
    if providers_df.empty:
        raise HTTPException(
            status_code=404,
            detail="No providers with tariffs found in database.",
        )

    provider_rows = providers_df[providers_df["providerid"] == provider_id]
    if provider_rows.empty:
        raise HTTPException(
            status_code=404,
            detail=f"Provider {provider_id} not found in tariffs list.",
        )

    provider_info = provider_rows.iloc[0]
    provider_name = str(provider_info["providername"])
    official_band = str(provider_info.get("official_band", "Unspecified"))

    reality_tariff = tariff_builder.build_reality_adjusted_tariff(
        providerid=provider_id,
        standard_procedures=standard_df,
        lookback_months=lookback,
    )

    if reality_tariff.empty:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No reality-adjusted tariff data for provider {provider_id}. "
                "This may mean no claims in lookback window or missing tariff."
            ),
        )

    engine_reality = RealityBandingEngine(standard_tariff_df=standard_df)
    result = engine_reality.analyze_reality_tariff(
        reality_tariff_df=reality_tariff,
        provider_name=provider_name,
    )

    if not result.get("success", False):
        raise HTTPException(
            status_code=400,
            detail=result.get("error", "Claims-based tariff analysis failed."),
        )

    result_with_ctx: Dict[str, Any] = {
        "provider_id": provider_id,
        "provider_name": provider_name,
        "official_band": official_band,
        **result,
    }
    return result_with_ctx


@router.get("/providers-with-tariffs")
async def get_providers_with_tariffs():
    """
    Get list of providers that have tariff data attached.
    Used for populating provider dropdowns in tariff analysis tabs.
    """
    try:
        loader = DuckDBDataLoader(db_path=DB_PATH)
        providers_df = loader.load_hospital_list()

        if providers_df.empty:
            return {
                "success": True,
                "data": [],
                "message": "No providers with tariff data found",
            }

        providers_list = [
            {
                "provider_id": str(row["providerid"]),
                "provider_name": str(row["providername"]),
                "current_band": str(row.get("current_band", "Unspecified")),
            }
            for _, row in providers_df.iterrows()
        ]

        return {
            "success": True,
            "data": providers_list,
            "count": len(providers_list),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/standard-analyze")
async def tariff_standard_analyze(req: TariffStandardRequest):
    """
    TARIFF_STANDARD_ANALYSIS
    Backend for React Tab 1.
    """
    result = _run_standard_analysis(req)
    return jsonable_encoder({"success": True, "analysis": result})


@router.post("/claims-analyze")
async def tariff_claims_analyze(req: TariffClaimsRequest):
    """
    TARIFF_CLAIMS_ANALYSIS
    Backend for React Tab 2.
    """
    result = _run_claims_analysis(req)
    return jsonable_encoder({"success": True, "analysis": result})


@router.post("/existing-recategorization")
async def tariff_existing_recategorization(req: ExistingRecategorizationRequest):
    """
    EXISTING RECATEGORIZATION
    Backend for React Tab 3.
    Combine:
    - Standard tariff band (EnhancedBandingEngine)
    - Reality-adjusted band (RealityBandingEngine)
    """
    std_req = TariffStandardRequest(
        mode="provider",
        provider_id=req.provider_id,
        tariff_rows=None,
        include_quality=True,
        include_fraud=True,
    )
    standard_result = _run_standard_analysis(std_req)

    claims_req = TariffClaimsRequest(
        provider_id=req.provider_id,
        lookback_months=req.lookback_months,
    )
    claims_result = _run_claims_analysis(claims_req)

    combined = {
        "success": True,
        "provider_id": claims_result.get("provider_id", req.provider_id),
        "provider_name": claims_result.get("provider_name"),
        "official_band": claims_result.get("official_band"),
        "standard_analysis": standard_result,
        "claims_analysis": claims_result,
    }
    return jsonable_encoder(combined)

