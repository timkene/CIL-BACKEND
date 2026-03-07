"""
KLAIRE AI Renewal Analysis API
===============================
FastAPI application that generates comprehensive renewal analysis reports
for Clearline International Limited's HMO portfolio.

Endpoints:
  POST /report/generate      — full PDF + JSON, background job
  GET  /report/status/{id}   — check job status
  GET  /report/download/{id} — download generated PDF
  POST /report/analyze       — instant JSON analysis (no PDF)
  GET  /health               — health check
  GET  /groups               — list available groups

Standards Applied:
  - PMPM-based actuarial pricing (ACA / Massachusetts GIC standard)
  - Nigerian HMO market norms (10x–25x limit:premium)
  - SRS methodology (Symmetry ETG framework)
  - RAND Health Insurance Experiment chronic disease evidence
  - NAIC MLR calculation standards
"""

import os
import sys
import uuid

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass
import asyncio
import traceback
from datetime import datetime
from typing import Optional
from pathlib import Path

from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import uvicorn

from .data_collector import collect_data, RenewalData
from .narrator import generate_all_narratives
from .pdf_generator import generate_pdf

# ──────────────────────────────────────────────────────────────────────────────
# APP SETUP
# ──────────────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="KLAIRE AI Renewal Analysis API",
    description=(
        "Generates comprehensive HMO renewal analysis reports for Clearline International Limited. "
        "Uses PMPM-based actuarial pricing (industry standard), SRS methodology, and Claude AI "
        "for intelligent narrative generation."
    ),
    version="2.0.0",
    contact={"name": "KLAIRE AI Analytics", "email": "analytics@clearline.ng"},
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory job store (replace with Redis in production)
JOB_STORE: dict[str, dict] = {}
OUTPUT_DIR = Path(os.environ.get("REPORT_OUTPUT_DIR", "/tmp/klaire_reports"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/data/ai_driven.duckdb")


# ──────────────────────────────────────────────────────────────────────────────
# REQUEST / RESPONSE MODELS
# ──────────────────────────────────────────────────────────────────────────────
class ReportRequest(BaseModel):
    group_name: str = Field(
        ...,
        description="Group name pattern (case-insensitive LIKE match)",
        example="KIZITO MARITIME"
    )
    current_start: str = Field(
        ...,
        description="Current contract start date (YYYY-MM-DD)",
        example="2024-11-01"
    )
    current_end: str = Field(
        ...,
        description="Current contract end date (YYYY-MM-DD)",
        example="2025-10-31"
    )
    prev_start: str = Field(
        ...,
        description="Previous contract start date (YYYY-MM-DD)",
        example="2023-11-01"
    )
    prev_end: str = Field(
        ...,
        description="Previous contract end date (YYYY-MM-DD)",
        example="2024-10-31"
    )
    db_path: Optional[str] = Field(
        None,
        description="Override DuckDB path (uses DUCKDB_PATH env var if not provided)"
    )
    include_ai_narratives: bool = Field(
        True,
        description="Generate AI-powered narrative sections (requires ANTHROPIC_API_KEY)"
    )


class JobStatus(BaseModel):
    job_id: str
    status: str  # PENDING | RUNNING | COMPLETE | FAILED
    group_name: str
    started_at: str
    completed_at: Optional[str]
    progress: str
    error: Optional[str]
    download_url: Optional[str]


class AnalysisResponse(BaseModel):
    group_name: str
    analysis_date: str

    # Core financials
    active_members: int
    total_debit: float
    cash_received: float
    payment_rate: float
    claims_total: float
    pa_total_authorized: float
    annualized_claims: float

    # MLR metrics
    projected_mlr: float
    ytd_mlr: float
    cash_mlr: float
    prev_mlr: float

    # PMPM analysis
    prev_pmpm: float
    curr_pmpm: float
    actuarial_premium: float
    actuarial_adequacy: str

    # SRS
    srs_classification: str
    top5_pct: float
    chronic_pct: float

    # Recommendations
    recommended_increase_pct: float
    renewal_strategy: str

    # Narratives (if generated)
    executive_bullets: list[str]
    srs_narrative: str
    premium_narrative: str
    provider_narrative: str


# ──────────────────────────────────────────────────────────────────────────────
# HELPER FUNCTIONS
# ──────────────────────────────────────────────────────────────────────────────
def _calc_increase_pct(data: RenewalData) -> float:
    """
    Determine recommended premium increase using PMPM actuarial bands.
    Based on Nigerian HMO market norms and NAIC MLR guidance.
    """
    if data.projected_mlr >= 100:
        return 30.0
    elif data.projected_mlr >= 85 and data.srs_classification == "EPISODIC":
        return 17.5
    elif data.projected_mlr >= 85:
        return 22.5
    elif data.projected_mlr >= 75 and data.srs_classification == "STRUCTURAL":
        return 20.0
    elif data.projected_mlr >= 75:
        return 12.5
    elif data.projected_mlr >= 65:
        return 5.0
    else:
        return 0.0


def _build_analysis_response(data: RenewalData, narratives: dict) -> AnalysisResponse:
    """Construct the JSON analysis response from collected data."""
    payment_rate = round(data.cash_received / data.total_debit * 100, 1) if data.total_debit else 0.0
    main_prem = data.plans[0]["premium"] if data.plans else 0.0
    delta = main_prem - data.actuarial_premium
    adequacy = f"₦{abs(delta):,.0f} {'ABOVE' if delta >= 0 else 'BELOW'} actuarial minimum"
    inc = _calc_increase_pct(data)

    # AI-generated strategy is in narratives["renewal_strategy"] (from Opus)
    # Fallback: hardcoded template if AI failed
    ai_strategy = narratives.get("renewal_strategy", "")
    if not ai_strategy:
        srs_templates = {
            "EPISODIC": (
                f"EPISODIC portfolio — {data.top5_pct:.1f}% of claims concentrated in top 5 members. "
                f"Strategy: {inc:.0f}% premium increase + surgical sub-limit (₦400K/event). "
                "High-cost events may not repeat — verify member persistence before maximum hike."
            ),
            "STRUCTURAL": (
                f"STRUCTURAL portfolio — chronic disease burden {data.chronic_pct:.1f}% of claims. "
                f"Strategy: {inc:.0f}% premium increase + CDMP enrolment + sub-limits. "
                "RAND HI Experiment: CDMP reduces chronic claims 18–22% over 2 years."
            ),
            "MIXED": (
                f"MIXED portfolio — moderate concentration, {data.chronic_pct:.1f}% chronic load. "
                f"Strategy: {inc:.0f}% premium increase, monitor closely for next 6 months."
            ),
        }
        ai_strategy = srs_templates.get(data.srs_classification, "")

    return AnalysisResponse(
        group_name=data.group_name,
        analysis_date=data.analysis_date,
        active_members=data.active_members,
        total_debit=data.total_debit,
        cash_received=data.cash_received,
        payment_rate=payment_rate,
        claims_total=data.claims_total,
        pa_total_authorized=data.pa_total_authorized,
        annualized_claims=data.annualized_claims,
        projected_mlr=data.projected_mlr,
        ytd_mlr=data.ytd_mlr,
        cash_mlr=data.cash_mlr,
        prev_mlr=data.prev_mlr,
        prev_pmpm=data.prev_pmpm,
        curr_pmpm=data.curr_pmpm,
        actuarial_premium=data.actuarial_premium,
        actuarial_adequacy=adequacy,
        srs_classification=data.srs_classification,
        top5_pct=data.top5_pct,
        chronic_pct=data.chronic_pct,
        recommended_increase_pct=inc,
        renewal_strategy=ai_strategy,
        executive_bullets=narratives.get("executive_bullets", []),
        srs_narrative=narratives.get("srs_narrative", ""),
        premium_narrative=narratives.get("premium_narrative", ""),
        provider_narrative=narratives.get("provider_narrative", ""),
    )


# ──────────────────────────────────────────────────────────────────────────────
# BACKGROUND JOB RUNNER
# ──────────────────────────────────────────────────────────────────────────────
async def _run_report_job(job_id: str, req: ReportRequest):
    """Background task: collect data → generate AI narratives → build PDF."""
    JOB_STORE[job_id]["status"] = "RUNNING"

    db_path = req.db_path or DUCKDB_PATH

    try:
        # Step 1: Data collection
        JOB_STORE[job_id]["progress"] = "Collecting data from DuckDB…"
        loop = asyncio.get_event_loop()
        data: RenewalData = await loop.run_in_executor(
            None,
            lambda: collect_data(
                req.group_name,
                req.current_start,
                req.current_end,
                req.prev_start,
                req.prev_end,
                db_path,
            ),
        )

        # Step 2: AI narratives
        if req.include_ai_narratives and os.environ.get("ANTHROPIC_API_KEY"):
            JOB_STORE[job_id]["progress"] = "Generating AI narrative sections…"
            narratives = await loop.run_in_executor(
                None, lambda: generate_all_narratives(data)
            )
        else:
            JOB_STORE[job_id]["progress"] = "Skipping AI narratives (no API key)…"
            narratives = {}

        # Step 3: PDF generation
        JOB_STORE[job_id]["progress"] = "Building PDF report…"
        pdf_bytes = await loop.run_in_executor(
            None, lambda: generate_pdf(data, narratives)
        )

        # Step 4: Save PDF
        safe_name = "".join(c if c.isalnum() else "_" for c in req.group_name)[:40]
        filename = f"renewal_{safe_name}_{job_id[:8]}.pdf"
        filepath = OUTPUT_DIR / filename
        filepath.write_bytes(pdf_bytes)

        # Step 5: Build JSON analysis
        analysis = _build_analysis_response(data, narratives)

        JOB_STORE[job_id].update({
            "status": "COMPLETE",
            "progress": "Report generated successfully.",
            "completed_at": datetime.now().isoformat(),
            "filename": filename,
            "filepath": str(filepath),
            "analysis": analysis.model_dump(),
            "download_url": f"/report/download/{job_id}",
        })

    except Exception as e:
        JOB_STORE[job_id].update({
            "status": "FAILED",
            "progress": "Job failed.",
            "completed_at": datetime.now().isoformat(),
            "error": str(e),
            "traceback": traceback.format_exc(),
        })


# ──────────────────────────────────────────────────────────────────────────────
# ENDPOINTS
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["System"])
def health_check():
    """Health check — verifies API is running and environment is configured."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    db_path = os.environ.get("DUCKDB_PATH", DUCKDB_PATH)
    db_exists = Path(db_path).exists()

    return {
        "status": "ok",
        "version": "2.0.0",
        "timestamp": datetime.now().isoformat(),
        "environment": {
            "anthropic_api_key_set": bool(api_key),
            "duckdb_path": db_path,
            "duckdb_accessible": db_exists,
            "output_dir": str(OUTPUT_DIR),
        },
        "standards_applied": [
            "PMPM-based actuarial pricing (ACA / Massachusetts GIC)",
            "Nigerian HMO market norms (10x–25x limit:premium)",
            "SRS classification (Symmetry ETG methodology)",
            "RAND HI Experiment chronic disease evidence",
            "NAIC MLR calculation standards",
            "3:1 framework applied as renewal adjustment ratio (NOT absolute rule)",
        ],
    }


@app.post(
    "/report/generate",
    response_model=JobStatus,
    status_code=202,
    tags=["Reports"],
    summary="Generate full renewal analysis report (PDF + JSON)",
)
async def generate_report(req: ReportRequest, background_tasks: BackgroundTasks):
    """
    Starts a background job to generate a comprehensive renewal analysis report.

    The job:
    1. Queries DuckDB for all group financial, claims, PA, member, and provider data
    2. Calculates MLR, PMPM, SRS classification, actuarial premium
    3. Calls Claude AI to generate executive summary, SRS narrative, premium recommendation, provider analysis
    4. Builds a multi-section professional PDF matching the Kizito report structure

    Returns a job_id — poll GET /report/status/{job_id} for progress.
    Download the PDF at GET /report/download/{job_id} when status = COMPLETE.
    """
    job_id = str(uuid.uuid4())
    JOB_STORE[job_id] = {
        "job_id": job_id,
        "status": "PENDING",
        "group_name": req.group_name,
        "started_at": datetime.now().isoformat(),
        "completed_at": None,
        "progress": "Job queued.",
        "error": None,
        "download_url": None,
    }

    background_tasks.add_task(_run_report_job, job_id, req)

    return JobStatus(**JOB_STORE[job_id])


@app.get(
    "/report/status/{job_id}",
    response_model=JobStatus,
    tags=["Reports"],
    summary="Check report generation job status",
)
def get_job_status(job_id: str):
    """Poll this endpoint after submitting a report job. Status: PENDING | RUNNING | COMPLETE | FAILED"""
    if job_id not in JOB_STORE:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found.")
    job = JOB_STORE[job_id]
    return JobStatus(
        job_id=job_id,
        status=job["status"],
        group_name=job["group_name"],
        started_at=job["started_at"],
        completed_at=job.get("completed_at"),
        progress=job["progress"],
        error=job.get("error"),
        download_url=job.get("download_url"),
    )


@app.get(
    "/report/download/{job_id}",
    tags=["Reports"],
    summary="Download generated PDF report",
    response_class=FileResponse,
)
def download_report(job_id: str):
    """Download the PDF report once status = COMPLETE."""
    if job_id not in JOB_STORE:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found.")

    job = JOB_STORE[job_id]
    if job["status"] != "COMPLETE":
        raise HTTPException(
            status_code=409,
            detail=f"Report not ready. Status: {job['status']}. Progress: {job['progress']}",
        )

    filepath = job.get("filepath")
    if not filepath or not Path(filepath).exists():
        raise HTTPException(status_code=410, detail="PDF file not found on server.")

    return FileResponse(
        path=filepath,
        media_type="application/pdf",
        filename=job["filename"],
        headers={"Content-Disposition": f'attachment; filename="{job["filename"]}"'},
    )


@app.get(
    "/report/analysis/{job_id}",
    response_model=AnalysisResponse,
    tags=["Reports"],
    summary="Get JSON analysis from completed job",
)
def get_analysis(job_id: str):
    """Retrieve the structured JSON analysis from a completed report job."""
    if job_id not in JOB_STORE:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found.")

    job = JOB_STORE[job_id]
    if job["status"] != "COMPLETE":
        raise HTTPException(
            status_code=409,
            detail=f"Analysis not ready. Status: {job['status']}."
        )

    return job["analysis"]


@app.post(
    "/report/analyze",
    response_model=AnalysisResponse,
    tags=["Reports"],
    summary="Instant JSON analysis (no PDF generation)",
)
async def analyze_only(req: ReportRequest):
    """
    Synchronous endpoint — returns JSON analysis without generating a PDF.
    Faster than /report/generate for programmatic use.

    Still calls Claude AI for narrative sections if ANTHROPIC_API_KEY is set.
    Use include_ai_narratives=false to skip AI and return just the metrics.
    """
    db_path = req.db_path or DUCKDB_PATH

    try:
        loop = asyncio.get_event_loop()
        data = await loop.run_in_executor(
            None,
            lambda: collect_data(
                req.group_name,
                req.current_start,
                req.current_end,
                req.prev_start,
                req.prev_end,
                db_path,
            ),
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Data collection error: {e}")

    if req.include_ai_narratives and os.environ.get("ANTHROPIC_API_KEY"):
        loop = asyncio.get_event_loop()
        narratives = await loop.run_in_executor(
            None, lambda: generate_all_narratives(data)
        )
    else:
        narratives = {}

    return _build_analysis_response(data, narratives)


@app.get(
    "/groups",
    tags=["Data"],
    summary="List all active groups in the database",
)
def list_groups(
    search: Optional[str] = Query(None, description="Filter groups by name pattern"),
    db_path_override: Optional[str] = Query(None, description="Override DB path"),
):
    """
    Returns all active groups from the GROUPS table.
    Useful for discovering group names before submitting a report request.
    """
    import duckdb
    db_path = db_path_override or DUCKDB_PATH
    if not Path(db_path).exists():
        raise HTTPException(
            status_code=503,
            detail=f"Database not found at {db_path}. Set DUCKDB_PATH environment variable."
        )

    try:
        conn = duckdb.connect(db_path, read_only=True)
        where = f"WHERE UPPER(groupname) LIKE UPPER('%{search}%')" if search else ""
        rows = conn.execute(f"""
            SELECT g.groupid, g.groupname,
                   COUNT(m.memberid) as active_members
            FROM "AI DRIVEN DATA"."GROUPS" g
            LEFT JOIN "AI DRIVEN DATA"."MEMBER" m
                ON m.groupid = g.groupid
                AND m.isterminated = false AND m.isdeleted = false
            {where}
            GROUP BY g.groupid, g.groupname
            ORDER BY active_members DESC
            LIMIT 100
        """).fetchall()
        conn.close()
        return {"groups": [{"id": r[0], "name": r[1], "active_members": r[2]} for r in rows]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@app.get(
    "/jobs",
    tags=["System"],
    summary="List all report generation jobs (current session)",
)
def list_jobs():
    """Returns all jobs in the current session (in-memory, resets on server restart)."""
    return {
        "total": len(JOB_STORE),
        "jobs": [
            {
                "job_id": jid,
                "group_name": j["group_name"],
                "status": j["status"],
                "started_at": j["started_at"],
                "progress": j["progress"],
            }
            for jid, j in JOB_STORE.items()
        ],
    }


# ──────────────────────────────────────────────────────────────────────────────
# RUN
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    host = os.environ.get("HOST", "0.0.0.0")
    print(f"\n{'='*60}")
    print(f"  KLAIRE AI Renewal Analysis API v2.0.0")
    print(f"  Clearline International Limited")
    print(f"{'='*60}")
    print(f"  → http://{host}:{port}")
    print(f"  → Docs: http://{host}:{port}/docs")
    print(f"  → DUCKDB_PATH: {DUCKDB_PATH}")
    print(f"  → ANTHROPIC_API_KEY: {'SET' if os.environ.get('ANTHROPIC_API_KEY') else 'NOT SET'}")
    print(f"{'='*60}\n")
    uvicorn.run("apis.ai_client.main:app", host=host, port=port, reload=True)