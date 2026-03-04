#!/usr/bin/env python3
"""
CLEARLINE AI VETTING API
=========================

FastAPI service wrapping the comprehensive vetting engine.
Other systems call this to validate PA requests.

Endpoints:
    POST /api/v1/validate          - Submit PA for validation
    GET  /api/v1/requests/{id}     - Get request status/details
    GET  /api/v1/pending           - Get pending review queue (for agent)
    POST /api/v1/review/{id}       - Agent confirms/rejects + stores learning
    GET  /api/v1/stats             - System statistics
    GET  /api/v1/history           - Request history with filters
    GET  /api/v1/health            - Health check

Decision States:
    AUTO_APPROVED    - All rules passed from master/learning (no AI involved)
    AUTO_DENIED      - Master denial or high-confidence learned denial (≥3 uses)
    PENDING_REVIEW   - AI was involved; human must confirm to store learning
    HUMAN_APPROVED   - Agent confirmed approval (learning stored)
    HUMAN_DENIED     - Agent confirmed denial (learning stored)

Run:
    python vetting_api.py
    → API at http://localhost:8000
    → Docs at http://localhost:8000/docs

Author: Casey's AI Assistant
Date: February 2026
Version: 1.0
"""

import os
import json
import uuid
import logging
from datetime import datetime, date
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

DB_PATH = os.getenv("DB_PATH", "ai_driven_data.duckdb")

# ============================================================================
# PYDANTIC REQUEST/RESPONSE MODELS
# ============================================================================

class PARequest(BaseModel):
    """Hospital submits this to request pre-authorization"""
    procedure_code: str = Field(..., description="Procedure/drug code e.g. DRG1106")
    diagnosis_code: str = Field(..., description="ICD-10 diagnosis code e.g. B509")
    enrollee_id: str = Field(..., description="Enrollee ID e.g. CL/OCTA/723449/2023-A")
    encounter_date: Optional[str] = Field(None, description="YYYY-MM-DD, defaults to today")
    hospital_name: Optional[str] = Field(None, description="Requesting hospital")
    notes: Optional[str] = Field(None, description="Additional clinical notes")


class ReviewAction(BaseModel):
    """Agent submits to confirm/override AI decision"""
    action: str = Field(..., description="CONFIRM or OVERRIDE")
    override_decision: Optional[str] = Field(None, description="If OVERRIDE: APPROVE or DENY")
    reviewed_by: str = Field(default="Casey", description="Reviewer name")
    notes: Optional[str] = Field(None, description="Review notes")


class RuleDetail(BaseModel):
    rule_name: str
    passed: bool
    source: str
    confidence: int
    reasoning: str
    details: Dict[str, Any] = {}


class ValidationResponse(BaseModel):
    request_id: str
    status: str
    decision: str           # APPROVE or DENY (final or AI recommendation)
    confidence: int
    reasoning: str
    enrollee_id: str
    enrollee_age: Optional[int] = None
    enrollee_gender: Optional[str] = None
    procedure_code: str
    procedure_name: Optional[str] = None
    diagnosis_code: str
    diagnosis_name: Optional[str] = None
    encounter_date: str
    hospital_name: Optional[str] = None
    rules: List[RuleDetail] = []
    summary: Dict[str, Any] = {}
    created_at: str
    reviewed_at: Optional[str] = None
    reviewed_by: Optional[str] = None


# ============================================================================
# QUEUE TABLE + HELPERS
# ============================================================================

def create_queue_table(conn):
    """Create the vetting queue table"""
    conn.execute('CREATE SCHEMA IF NOT EXISTS "PROCEDURE_DIAGNOSIS"')
    conn.execute("""
        CREATE TABLE IF NOT EXISTS "PROCEDURE_DIAGNOSIS"."vetting_queue" (
            request_id VARCHAR PRIMARY KEY,
            procedure_code VARCHAR NOT NULL,
            diagnosis_code VARCHAR NOT NULL,
            enrollee_id VARCHAR NOT NULL,
            encounter_date VARCHAR NOT NULL,
            hospital_name VARCHAR,
            notes VARCHAR,
            enrollee_age INTEGER,
            enrollee_gender VARCHAR,
            procedure_name VARCHAR,
            diagnosis_name VARCHAR,
            status VARCHAR NOT NULL,
            decision VARCHAR NOT NULL,
            confidence INTEGER,
            reasoning TEXT,
            rules_json TEXT,
            summary_json TEXT,
            reviewed_at TIMESTAMP,
            reviewed_by VARCHAR,
            review_notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


def determine_status(validation) -> str:
    """
    Map engine output → queue status:
    
    AUTO_APPROVED  → all pass, no AI → instant approval
    AUTO_DENIED    → master denial or learned denial (≥3 uses) → instant deny
    PENDING_REVIEW → AI was involved → agent must confirm to store learning
    """
    if validation.auto_deny:
        return "AUTO_DENIED"
    if not validation.requires_human_review:
        return "AUTO_APPROVED" if validation.overall_decision == "APPROVE" else "AUTO_DENIED"
    return "PENDING_REVIEW"


def rules_to_list(rule_results) -> list:
    """Serialize rule results"""
    return [
        {
            "rule_name": r.rule_name,
            "passed": r.passed,
            "source": r.source,
            "confidence": r.confidence,
            "reasoning": r.reasoning,
            "details": _safe(r.details) if r.details else {}
        }
        for r in rule_results
    ]


def _safe(obj) -> Any:
    """Make JSON-serializable"""
    if isinstance(obj, dict):
        return {k: _safe(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_safe(i) for i in obj]
    elif isinstance(obj, (datetime, date)):
        return str(obj)
    return obj


def row_to_response(row: dict) -> ValidationResponse:
    """Convert DB row → API response"""
    rules_raw = json.loads(row.get('rules_json') or '[]')
    summary = json.loads(row.get('summary_json') or '{}')
    return ValidationResponse(
        request_id=row['request_id'],
        status=row['status'],
        decision=row['decision'],
        confidence=row.get('confidence') or 0,
        reasoning=row.get('reasoning', ''),
        enrollee_id=row['enrollee_id'],
        enrollee_age=row.get('enrollee_age'),
        enrollee_gender=row.get('enrollee_gender'),
        procedure_code=row['procedure_code'],
        procedure_name=row.get('procedure_name'),
        diagnosis_code=row['diagnosis_code'],
        diagnosis_name=row.get('diagnosis_name'),
        encounter_date=row['encounter_date'],
        hospital_name=row.get('hospital_name'),
        rules=[RuleDetail(**r) for r in rules_raw],
        summary=summary,
        created_at=str(row.get('created_at', '')),
        reviewed_at=str(row['reviewed_at']) if row.get('reviewed_at') else None,
        reviewed_by=row.get('reviewed_by')
    )


# ============================================================================
# ENGINE SINGLETON
# ============================================================================

engine = None
db_conn = None


def get_engine():
    global engine, db_conn
    if engine is None:
        from vetting_comprehensive import ComprehensiveVettingEngine
        engine = ComprehensiveVettingEngine(DB_PATH)
        db_conn = engine.conn
        create_queue_table(db_conn)
        logger.info(f"✅ Vetting engine initialized (DB: {DB_PATH})")
    return engine


# ============================================================================
# FASTAPI APP
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    get_engine()
    yield
    logger.info("Shutting down vetting API")

app = FastAPI(
    title="Clearline AI Vetting API",
    description="Pre-Authorization validation with AI-powered learning",
    version="1.0.0",
    lifespan=lifespan
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)


# ============================================================================
# ENDPOINTS
# ============================================================================

@app.get("/api/v1/health")
def health_check():
    get_engine()
    return {"status": "healthy", "database": DB_PATH, "timestamp": datetime.now().isoformat()}


@app.post("/api/v1/validate", response_model=ValidationResponse)
def validate_pa(request: PARequest):
    """
    Submit a PA request for AI validation.
    
    Returns immediately:
    - AUTO_APPROVED / AUTO_DENIED → final decision
    - PENDING_REVIEW → queued; `decision` field = AI recommendation
    """
    eng = get_engine()
    encounter_date = request.encounter_date or date.today().strftime('%Y-%m-%d')
    request_id = str(uuid.uuid4())[:12]
    
    # Run validation
    try:
        validation = eng.validate_comprehensive(
            procedure_code=request.procedure_code,
            diagnosis_code=request.diagnosis_code,
            enrollee_id=request.enrollee_id,
            encounter_date=encounter_date
        )
    except Exception as e:
        logger.error(f"Validation error: {e}")
        raise HTTPException(status_code=500, detail=f"Validation engine error: {str(e)}")
    
    status = determine_status(validation)
    
    # Context
    enrollee_ctx = eng.base_engine.get_enrollee_context(request.enrollee_id, encounter_date)
    proc_info = eng._resolve_procedure_info(request.procedure_code)
    diag_info = eng._resolve_diagnosis_info(request.diagnosis_code)
    rules_list = rules_to_list(validation.rule_results)
    summary = validation.get_summary()
    now = datetime.now().isoformat()
    
    # Store in queue
    try:
        db_conn.execute("""
            INSERT INTO "PROCEDURE_DIAGNOSIS"."vetting_queue" 
            (request_id, procedure_code, diagnosis_code, enrollee_id, encounter_date,
             hospital_name, notes,
             enrollee_age, enrollee_gender, procedure_name, diagnosis_name,
             status, decision, confidence, reasoning, rules_json, summary_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            request_id, request.procedure_code.upper(), request.diagnosis_code.upper(),
            request.enrollee_id, encounter_date,
            request.hospital_name, request.notes,
            enrollee_ctx.age, enrollee_ctx.gender,
            proc_info.get('name', 'Unknown'), diag_info.get('name', 'Unknown'),
            status, validation.overall_decision, validation.overall_confidence,
            validation.overall_reasoning, json.dumps(rules_list), json.dumps(summary), now
        ])
    except Exception as e:
        logger.error(f"Queue insert error: {e}")
    
    return ValidationResponse(
        request_id=request_id, status=status,
        decision=validation.overall_decision, confidence=validation.overall_confidence,
        reasoning=validation.overall_reasoning,
        enrollee_id=request.enrollee_id,
        enrollee_age=enrollee_ctx.age, enrollee_gender=enrollee_ctx.gender,
        procedure_code=request.procedure_code.upper(),
        procedure_name=proc_info.get('name', 'Unknown'),
        diagnosis_code=request.diagnosis_code.upper(),
        diagnosis_name=diag_info.get('name', 'Unknown'),
        encounter_date=encounter_date, hospital_name=request.hospital_name,
        rules=[RuleDetail(**r) for r in rules_list], summary=summary,
        created_at=now
    )


@app.get("/api/v1/requests/{request_id}", response_model=ValidationResponse)
def get_request(request_id: str):
    """Get full details for a specific request"""
    result = db_conn.execute(
        'SELECT * FROM "PROCEDURE_DIAGNOSIS"."vetting_queue" WHERE request_id = ?',
        [request_id]
    ).fetchone()
    if not result:
        raise HTTPException(status_code=404, detail=f"Request {request_id} not found")
    columns = [d[0] for d in db_conn.description]
    return row_to_response(dict(zip(columns, result)))


@app.get("/api/v1/pending")
def get_pending(limit: int = Query(50, le=200), offset: int = Query(0)):
    """Get PENDING_REVIEW queue for agent"""
    results = db_conn.execute("""
        SELECT request_id, procedure_code, diagnosis_code, enrollee_id,
               encounter_date, enrollee_age, enrollee_gender,
               procedure_name, diagnosis_name,
               decision AS ai_recommendation, confidence, reasoning,
               hospital_name, created_at
        FROM "PROCEDURE_DIAGNOSIS"."vetting_queue"
        WHERE status = 'PENDING_REVIEW'
        ORDER BY created_at ASC LIMIT ? OFFSET ?
    """, [limit, offset]).fetchdf()
    
    count = db_conn.execute(
        'SELECT COUNT(*) FROM "PROCEDURE_DIAGNOSIS"."vetting_queue" WHERE status = \'PENDING_REVIEW\''
    ).fetchone()[0]
    
    return {"total_pending": count, "requests": results.to_dict(orient='records')}


@app.post("/api/v1/review/{request_id}")
def review_request(request_id: str, review: ReviewAction):
    """
    Agent reviews a PENDING request.
    
    CONFIRM  → agrees with AI → re-runs validation → stores learning
    OVERRIDE → disagrees with AI → NO learning stored
    """
    eng = get_engine()
    
    result = db_conn.execute("""
        SELECT * FROM "PROCEDURE_DIAGNOSIS"."vetting_queue"
        WHERE request_id = ? AND status = 'PENDING_REVIEW'
    """, [request_id]).fetchone()
    
    if not result:
        raise HTTPException(404, f"Request {request_id} not found or already reviewed")
    
    columns = [d[0] for d in db_conn.description]
    row = dict(zip(columns, result))
    ai_rec = row.get('decision', 'DENY')
    now = datetime.now().isoformat()
    
    if review.action == "CONFIRM":
        final_decision = ai_rec
        final_status = "HUMAN_APPROVED" if final_decision == "APPROVE" else "HUMAN_DENIED"
        
        # Re-run to get fresh validation object for learning
        validation = eng.validate_comprehensive(
            procedure_code=row['procedure_code'],
            diagnosis_code=row['diagnosis_code'],
            enrollee_id=row['enrollee_id'],
            encounter_date=row['encounter_date']
        )
        
        stored = {}
        if validation.can_store_ai_approvals:
            stored = eng.store_ai_validated_rules(
                procedure_code=row['procedure_code'],
                diagnosis_code=row['diagnosis_code'],
                validation=validation,
                approved_by=review.reviewed_by
            )
        
        db_conn.execute("""
            UPDATE "PROCEDURE_DIAGNOSIS"."vetting_queue"
            SET status=?, reviewed_at=?, reviewed_by=?, review_notes=?
            WHERE request_id=?
        """, [final_status, now, review.reviewed_by,
              review.notes or f"Confirmed AI {ai_rec}", request_id])
        
        return {
            "request_id": request_id,
            "final_decision": final_decision,
            "status": final_status,
            "learning_stored": stored,
            "message": f"✅ Confirmed AI {ai_rec}. "
                       f"{'Learning stored for ' + str(len(stored)) + ' rule(s).' if stored else 'No new learning.'}"
        }
    
    elif review.action == "OVERRIDE":
        if not review.override_decision or review.override_decision not in ("APPROVE", "DENY"):
            raise HTTPException(400, "OVERRIDE requires override_decision: APPROVE or DENY")
        
        final_decision = review.override_decision
        final_status = "HUMAN_APPROVED" if final_decision == "APPROVE" else "HUMAN_DENIED"
        
        db_conn.execute("""
            UPDATE "PROCEDURE_DIAGNOSIS"."vetting_queue"
            SET status=?, decision=?, reviewed_at=?, reviewed_by=?, review_notes=?
            WHERE request_id=?
        """, [final_status, final_decision, now, review.reviewed_by,
              review.notes or f"Overrode AI {ai_rec} → {final_decision}", request_id])
        
        return {
            "request_id": request_id,
            "final_decision": final_decision,
            "status": final_status,
            "learning_stored": {},
            "message": f"⚠️ Overrode AI {ai_rec} → {final_decision}. No learning stored."
        }
    
    raise HTTPException(400, "action must be CONFIRM or OVERRIDE")


@app.get("/api/v1/stats")
def get_stats():
    """System statistics"""
    stats = {}
    
    q = db_conn.execute("""
        SELECT status, COUNT(*) as count FROM "PROCEDURE_DIAGNOSIS"."vetting_queue" GROUP BY status
    """).fetchdf()
    stats['queue'] = q.set_index('status')['count'].to_dict() if not q.empty else {}
    
    today = date.today().isoformat()
    t = db_conn.execute("""
        SELECT status, COUNT(*) as count FROM "PROCEDURE_DIAGNOSIS"."vetting_queue"
        WHERE CAST(created_at AS DATE) = CAST(? AS DATE) GROUP BY status
    """, [today]).fetchdf()
    stats['today'] = t.set_index('status')['count'].to_dict() if not t.empty else {}
    
    learning = {}
    for tbl in ['ai_human_procedure_age', 'ai_human_procedure_gender',
                'ai_human_diagnosis_age', 'ai_human_diagnosis_gender',
                'ai_human_procedure_diagnosis', 'ai_human_procedure_class']:
        try:
            r = db_conn.execute(
                f'SELECT COUNT(*), COALESCE(SUM(usage_count),0) FROM "PROCEDURE_DIAGNOSIS"."{tbl}"'
            ).fetchone()
            learning[tbl] = {'entries': r[0], 'total_usage': int(r[1])}
        except:
            learning[tbl] = {'entries': 0, 'total_usage': 0}
    stats['learning'] = learning
    stats['learning_summary'] = {
        'total_entries': sum(v['entries'] for v in learning.values()),
        'total_ai_calls_saved': sum(v['total_usage'] for v in learning.values())
    }
    
    total = sum(stats['queue'].values()) if stats['queue'] else 0
    auto = stats['queue'].get('AUTO_APPROVED', 0) + stats['queue'].get('AUTO_DENIED', 0)
    stats['automation_rate'] = round(auto / total * 100, 1) if total > 0 else 0.0
    
    return stats


@app.get("/api/v1/history")
def get_history(
    status: Optional[str] = Query(None),
    enrollee_id: Optional[str] = Query(None),
    limit: int = Query(50, le=200),
    offset: int = Query(0)
):
    """Request history with optional filters"""
    query = """
        SELECT request_id, procedure_code, procedure_name,
               diagnosis_code, diagnosis_name,
               enrollee_id, enrollee_age, enrollee_gender,
               status, decision, confidence, reasoning,
               hospital_name, created_at, reviewed_at, reviewed_by
        FROM "PROCEDURE_DIAGNOSIS"."vetting_queue" WHERE 1=1
    """
    params = []
    if status:
        query += " AND status = ?"; params.append(status)
    if enrollee_id:
        query += " AND enrollee_id = ?"; params.append(enrollee_id)
    query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    
    return {"requests": db_conn.execute(query, params).fetchdf().to_dict(orient='records')}


if __name__ == "__main__":
    import uvicorn
    print("=" * 60)
    print("🚀 CLEARLINE AI VETTING API")
    print("=" * 60)
    print(f"📦 Database: {DB_PATH}")
    print(f"📄 API Docs: http://localhost:8000/docs")
    print(f"🏥 Hospital:  streamlit run hospital_app.py --server.port 8501")
    print(f"🛡️  Agent:     streamlit run agent_app.py --server.port 8502")
    print("=" * 60)
    uvicorn.run(app, host="0.0.0.0", port=8000)