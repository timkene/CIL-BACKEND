"""
main.py  –  Hospital Band Analysis API
=======================================
Run locally:
    uvicorn main:app --reload --port 8000

Render (Procfile):
    web: uvicorn main:app --host 0.0.0.0 --port $PORT
"""

from __future__ import annotations

import io
from datetime import datetime
from typing import Optional

import pandas as pd
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from banding_service import (
    build_reality_tariff,
    get_engines,
    get_provider_tariff,
    invalidate_cache,
    load_provider_list,
)

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Hospital Band Analysis API",
    description=(
        "Returns current_band, tariff_band (Script 1 – published tariff) "
        "and reality_band (Script 2 – claims-adjusted) for a provider."
    ),
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Response models
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
    analysis_source: str          # "existing_provider" | "csv_upload"
    lookback_months: Optional[int]
    analysis_timestamp: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/")
def root():
    return {"message": "Hospital Band Analysis API is running. See /docs."}


@app.get("/providers", summary="List all providers with tariff data")
def list_providers():
    """Returns providerid, providername, current_band for all eligible providers."""
    try:
        df = load_provider_list()
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post(
    "/analyze",
    response_model=AnalysisResponse,
    summary="Analyze provider banding (existing or new via CSV)",
)
async def analyze_provider(
    provider_name: Optional[str] = Form(None, description="Provider name from /providers list"),
    lookback_months: int = Form(6, description="Claims lookback for reality banding (3–24)"),
    csv_file: Optional[UploadFile] = File(None, description="CSV with procedurecode + tariffamount columns"),
):
    """
    **Two modes:**

    1. **Existing provider** – supply `provider_name`.  Pulls official tariff from DB
       and builds reality-adjusted tariff from claims history.

    2. **New provider (CSV upload)** – supply `csv_file` (+ optional `provider_name` label).
       Only tariff banding is returned; reality banding is not available without claims history.

    Both modes return `current_band`, `tariff_banding` (Script 1) and `reality_banding` (Script 2 or null).
    """
    if not provider_name and not csv_file:
        raise HTTPException(status_code=422, detail="Provide either provider_name or csv_file.")

    tariff_engine, reality_engine = get_engines()
    is_new_provider = csv_file is not None and not provider_name

    # ------------------------------------------------------------------ #
    # A) Existing provider – pull tariff from DB                          #
    # ------------------------------------------------------------------ #
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

        # Official tariff
        tariff_df = get_provider_tariff(prov_id)
        if tariff_df is None or tariff_df.empty:
            raise HTTPException(status_code=404, detail=f"No tariff data found for '{display_name}'")

        # Reality tariff (claims-based)
        reality_df = build_reality_tariff(prov_id, reality_engine.standard_df, lookback_months)

        source = "existing_provider"

    # ------------------------------------------------------------------ #
    # B) New provider via CSV upload                                       #
    # ------------------------------------------------------------------ #
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
                detail=f"CSV missing required columns: {missing}. "
                       f"Found: {list(tariff_df.columns)}",
            )
        tariff_df.columns = tariff_df.columns.str.lower().str.strip()

        display_name = provider_name or (csv_file.filename or "Unknown")
        prov_id = None
        current_band = "N/A"
        reality_df = pd.DataFrame()       # No claims history for new providers
        source = "csv_upload"

    # ------------------------------------------------------------------ #
    # Script 1 – Tariff Banding                                           #
    # ------------------------------------------------------------------ #
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

    # ------------------------------------------------------------------ #
    # Script 2 – Reality Banding                                          #
    # ------------------------------------------------------------------ #
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

    # ------------------------------------------------------------------ #
    # Band change flags                                                    #
    # ------------------------------------------------------------------ #
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


@app.post("/cache/clear", summary="Clear engine cache (reload tariff + claims stats)")
def clear_cache():
    invalidate_cache()
    return {"message": "Cache cleared. Engines will reload on next request."}