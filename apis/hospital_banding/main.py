"""
Hospital Banding API
====================
Dual-methodology provider band analysis:

  Standard Band  → provider's official tariff in DB vs band thresholds
                   (what they publish)
  Reality Band   → actual approved claim amounts vs band thresholds
                   (what they actually bill)

Both use the same REALITY TARIFF band thresholds from the CSV.
Both return weighted (TCOC) and unweighted (unit-price) bands.

Extra insights returned:
  - Quality score (readmission rate, denial rate, high-cost outliers)
  - Fraud risk (procedures above P90 / P95 / 2×P95)
  - Pricing behaviour (systematic overcharging / discounting vs official tariff)
  - Band comparison + recommendation

Port: 8003 (ai_client=8000, fraud=8001, vetting=8002)

Run:
  uvicorn fastapi.hospital_banding.main:app --port 8003 --reload
  # or
  python -m fastapi.hospital_banding.main
"""

import os
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

import pandas as pd
import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from .data_loader import (
    DB_PATH, TARIFF_PATH,
    get_conn,
    load_standard_tariff,
    load_claims_stats,
    load_quality_metrics,
    get_providers_list,
    get_provider_official_tariff,
    build_reality_tariff,
)
from .engines import analyze_standard, analyze_reality, build_comparison
from .models import DualBandResponse, ProviderListItem

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(name)s  %(message)s")
logger = logging.getLogger(__name__)

# ─── startup state ────────────────────────────────────────────────────────────
# Loaded once at startup, reused across requests.
_state: dict = {
    "standard_df":     None,   # raw DataFrame from REALITY TARIFF CSV
    "std_dict":        None,   # {normalized_code: row_dict}  for O(1) lookups
    "thresholds":      None,   # {band: mean_threshold}
    "claims_stats":    None,   # global P90/P95 per procedure
    "quality_metrics": None,   # provider quality scores
}


def _build_std_dict(standard_df: pd.DataFrame, claims_stats: pd.DataFrame):
    """
    Prepare std_dict (lookup) and thresholds from the standard tariff DataFrame.
    If claims_stats is available, use claims count as `effective_frequency` so
    the weighted (TCOC) band reflects real procedure volume.
    """
    df = standard_df.copy()

    def _norm(v):
        import math
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return ""
        return str(v).strip().lower().replace(" ", "")

    df["procedurecode"] = df["procedurecode"].apply(_norm)

    for col in ["band_a", "band_b", "band_c", "band_d", "band_special"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "frequency" not in df.columns:
        df["frequency"] = 1.0
    df["effective_frequency"] = df["frequency"]

    # Upgrade frequency from real claims counts if available
    if claims_stats is not None and not claims_stats.empty:
        freq_map = claims_stats.set_index("procedurecode")["count"].to_dict()
        df["effective_frequency"] = df["procedurecode"].map(freq_map).fillna(
            df["effective_frequency"]
        )

    std_dict   = {row["procedurecode"]: row.to_dict() for _, row in df.iterrows()}
    thresholds = {
        "D":       float(df["band_d"].mean()),
        "C":       float(df["band_c"].mean()),
        "B":       float(df["band_b"].mean()),
        "A":       float(df["band_a"].mean()),
        "Special": float(df["band_special"].mean()),
    }
    return std_dict, thresholds


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Pre-load standard tariff, claims stats, and quality metrics at startup."""
    logger.info("── Hospital Banding API startup ──")

    # 1. Standard tariff (local file)
    try:
        _state["standard_df"] = load_standard_tariff()
        logger.info(f"Standard tariff: {len(_state['standard_df'])} procedures")
    except Exception as exc:
        logger.error(f"Failed to load standard tariff: {exc}")

    # 2. Claims stats + quality metrics (DuckDB)
    try:
        conn = get_conn()
        _state["claims_stats"] = load_claims_stats(conn)
        logger.info(f"Claims stats: {len(_state['claims_stats'])} procedures")
        _state["quality_metrics"] = load_quality_metrics(conn)
        logger.info(f"Quality metrics: {len(_state['quality_metrics'])} providers")
        conn.close()
    except Exception as exc:
        logger.warning(f"Could not pre-load DB data: {exc}")
        _state["claims_stats"]    = pd.DataFrame()
        _state["quality_metrics"] = pd.DataFrame()

    # 3. Build lookup structures
    if _state["standard_df"] is not None:
        cs = _state["claims_stats"]
        _state["std_dict"], _state["thresholds"] = _build_std_dict(
            _state["standard_df"],
            cs if cs is not None else pd.DataFrame(),
        )
        logger.info("Banding engine ready")

    yield
    logger.info("── Hospital Banding API shutdown ──")


# ─── app ──────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Hospital Banding API",
    description=(
        "Dual-methodology provider band analysis.\n\n"
        "**Standard Band** — uses the provider's official tariff from the database "
        "(what they publish). Shows weighted (TCOC) + unweighted (unit-price) bands "
        "plus quality score and fraud risk.\n\n"
        "**Reality Band** — uses actual approved claim amounts from the last N months "
        "(what they actually bill), with official tariff as fallback for unclaimed "
        "procedures. Exposes overcharging/discounting vs published rates.\n\n"
        "**Comparison** — side-by-side with the current DB band and a plain-language "
        "recommendation."
    ),
    version="1.0.0",
    contact={"name": "KLAIRE Analytics"},
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── endpoints ────────────────────────────────────────────────────────────────

@app.get("/health", tags=["System"])
def health():
    """Health check — confirms what data is loaded and where."""
    cs  = _state["claims_stats"]
    qm  = _state["quality_metrics"]
    std = _state["standard_df"]
    return {
        "status":    "ok",
        "version":   "1.0.0",
        "timestamp": datetime.now().isoformat(),
        "loaded": {
            "standard_tariff":            std is not None,
            "standard_tariff_procedures": len(std) if std is not None else 0,
            "claims_stats":               cs is not None and not cs.empty,
            "claims_stats_procedures":    len(cs) if cs is not None else 0,
            "quality_metrics":            qm is not None and not qm.empty,
            "quality_providers":          len(qm) if qm is not None else 0,
        },
        "config": {
            "db_path":             DB_PATH,
            "standard_tariff_path": TARIFF_PATH,
        },
    }


@app.get("/providers", tags=["Providers"], response_model=list[ProviderListItem])
def list_providers(
    search: Optional[str] = Query(
        None, description="Partial name match (case-insensitive)"
    ),
    limit: int = Query(200, ge=1, le=1000),
):
    """
    List all providers that have active tariff data in the database.
    Use `search` to filter by name, e.g. `?search=lagos`.
    """
    try:
        conn = get_conn()
        df   = get_providers_list(conn)
        conn.close()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"DB error: {exc}")

    if df.empty:
        return []

    if search:
        df = df[df["providername"].str.contains(search, case=False, na=False)]

    df = df.head(limit)
    return [
        ProviderListItem(
            provider_id=str(r["providerid"]),
            provider_name=str(r["providername"]),
            current_band=str(r["current_band"]),
        )
        for _, r in df.iterrows()
    ]


@app.get(
    "/analyze/{provider_id}",
    tags=["Analysis"],
    response_model=DualBandResponse,
    summary="Dual band analysis for a single provider",
)
def analyze_provider(
    provider_id: str,
    lookback_months: int = Query(
        6, ge=1, le=24,
        description="Months of claims history to use for the reality band (1–24)",
    ),
):
    """
    Run dual band analysis for **one provider** and return both bands plus comparisons.

    | Band | Source | What it shows |
    |------|--------|---------------|
    | **Standard** | Official DB tariff | What the provider publishes |
    | **Reality**  | Actual approved claims | What the provider actually bills |

    Both bands are computed using two sub-methods:
    - **Weighted (TCOC)**: log-frequency weighted average — reflects true cost burden
    - **Unweighted**: simple average — reflects unit-price negotiations

    Extra fields:
    - `standard_analysis.quality`  — quality score, readmission rate, denial rate
    - `standard_analysis.fraud`    — procedures above P90 / P95 / 2×P95 from real claims
    - `reality_analysis.fraud`     — same fraud check on actual claim amounts
    - `reality_analysis.pricing_behavior` — overcharging / discounting vs official tariff
    - `comparison.recommendation`  — plain-language action item
    """
    # Guard: reference data must be loaded
    if _state["std_dict"] is None:
        raise HTTPException(
            status_code=503,
            detail="Standard tariff not loaded. Check /health for details.",
        )

    std_dict       = _state["std_dict"]
    thresholds     = _state["thresholds"]
    total_std      = len(std_dict)
    claims_stats   = _state["claims_stats"]   or pd.DataFrame()
    quality_metrics = _state["quality_metrics"] or pd.DataFrame()

    # ── Fetch provider info + data from DB ──
    try:
        conn = get_conn()

        providers_df  = get_providers_list(conn)
        provider_row  = providers_df[
            providers_df["providerid"].astype(str) == str(provider_id)
        ]
        if provider_row.empty:
            conn.close()
            raise HTTPException(
                status_code=404,
                detail=f"Provider '{provider_id}' not found. Use GET /providers to list valid IDs.",
            )

        info         = provider_row.iloc[0]
        provider_name = str(info["providername"])
        current_band  = str(info["current_band"])

        official_tariff = get_provider_official_tariff(conn, provider_id)
        if official_tariff is None or official_tariff.empty:
            conn.close()
            raise HTTPException(
                status_code=404,
                detail=f"No tariff data found for provider {provider_id}.",
            )

        reality_tariff = build_reality_tariff(
            conn, provider_id, _state["standard_df"], lookback_months
        )
        conn.close()

    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Database error: {exc}")

    # ── Run standard analysis ──
    standard_result = analyze_standard(
        official_tariff_df=official_tariff,
        std_dict=std_dict,
        thresholds=thresholds,
        total_std_procs=total_std,
        claims_stats=claims_stats,
        quality_metrics=quality_metrics,
        provider_id=provider_id,
    )

    # ── Run reality analysis ──
    if reality_tariff.empty:
        raise HTTPException(
            status_code=422,
            detail=(
                f"No claim data found for provider {provider_id} "
                f"in the last {lookback_months} months. "
                "Try a larger lookback_months value."
            ),
        )

    reality_result = analyze_reality(
        reality_df=reality_tariff,
        std_dict=std_dict,
        thresholds=thresholds,
        total_std_procs=total_std,
        claims_stats=claims_stats,
        lookback_months=lookback_months,
    )

    comparison = build_comparison(current_band, standard_result, reality_result)

    return DualBandResponse(
        provider_id=provider_id,
        provider_name=provider_name,
        analysis_date=datetime.now().isoformat(),
        standard_analysis=standard_result,
        reality_analysis=reality_result,
        comparison=comparison,
    )


@app.get(
    "/batch",
    tags=["Analysis"],
    summary="Quick band summary for up to 20 providers",
)
def batch_summary(
    provider_ids: str = Query(
        ...,
        description="Comma-separated provider IDs, e.g. '123,456,789' (max 20)",
    ),
    lookback_months: int = Query(6, ge=1, le=24),
):
    """
    Lightweight batch endpoint — returns just the four key bands for each provider
    without the full procedure-level detail. Useful for portfolio overview.

    Returns a list of objects with:
      provider_id, provider_name, current_db_band,
      standard_weighted, standard_unweighted,
      reality_weighted,  reality_unweighted,
      behavior_flag, recommendation
    """
    if _state["std_dict"] is None:
        raise HTTPException(status_code=503, detail="Standard tariff not loaded.")

    ids = [p.strip() for p in provider_ids.split(",") if p.strip()][:20]
    if not ids:
        raise HTTPException(status_code=400, detail="No provider IDs provided.")

    std_dict        = _state["std_dict"]
    thresholds      = _state["thresholds"]
    total_std       = len(std_dict)
    claims_stats    = _state["claims_stats"]    or pd.DataFrame()
    quality_metrics = _state["quality_metrics"] or pd.DataFrame()

    results = []
    try:
        conn = get_conn()
        providers_df = get_providers_list(conn)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"DB error: {exc}")

    for pid in ids:
        row = providers_df[providers_df["providerid"].astype(str) == pid]
        if row.empty:
            results.append({"provider_id": pid, "error": "not found"})
            continue

        info         = row.iloc[0]
        provider_name = str(info["providername"])
        current_band  = str(info["current_band"])

        try:
            official = get_provider_official_tariff(conn, pid)
            reality  = build_reality_tariff(conn, pid, _state["standard_df"], lookback_months)
        except Exception as exc:
            results.append({"provider_id": pid, "provider_name": provider_name, "error": str(exc)})
            continue

        if official is None or official.empty:
            results.append({"provider_id": pid, "provider_name": provider_name, "error": "no tariff"})
            continue

        std_r = analyze_standard(
            official, std_dict, thresholds, total_std,
            claims_stats, quality_metrics, pid,
        )

        if reality.empty:
            results.append({
                "provider_id":       pid,
                "provider_name":     provider_name,
                "current_db_band":   current_band,
                "standard_weighted": std_r.weighted_band,
                "standard_unweighted": std_r.unweighted_band,
                "reality_weighted":  "N/A — no claims",
                "reality_unweighted": "N/A",
                "behavior_flag":     "N/A",
                "recommendation":    "No claims data for reality analysis.",
            })
            continue

        real_r = analyze_reality(
            reality, std_dict, thresholds, total_std,
            claims_stats, lookback_months,
        )
        cmp = build_comparison(current_band, std_r, real_r)

        results.append({
            "provider_id":        pid,
            "provider_name":      provider_name,
            "current_db_band":    current_band,
            "standard_weighted":  std_r.weighted_band,
            "standard_unweighted": std_r.unweighted_band,
            "reality_weighted":   real_r.weighted_band,
            "reality_unweighted": real_r.unweighted_band,
            "quality_tier":       std_r.quality.quality_tier,
            "fraud_risk":         std_r.fraud.fraud_risk,
            "behavior_flag":      real_r.pricing_behavior.behavior_flag,
            "recommendation":     cmp.recommendation,
        })

    conn.close()
    return {"count": len(results), "lookback_months": lookback_months, "results": results}


# ─── run ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8003))
    host = os.getenv("HOST", "0.0.0.0")
    print(f"\n{'='*60}")
    print(f"  Hospital Banding API v1.0.0")
    print(f"  → http://{host}:{port}")
    print(f"  → Docs: http://{host}:{port}/docs")
    print(f"  → DB:   {DB_PATH}")
    print(f"  → Tariff: {TARIFF_PATH}")
    print(f"{'='*60}\n")
    uvicorn.run("apis.hospital_banding.main:app", host=host, port=port, reload=True)
