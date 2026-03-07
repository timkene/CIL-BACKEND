"""
Clearline Analytics — MLR API
==============================
Run:
    uvicorn apis.mlr.main:app --reload --port 8004

Swagger UI:   http://localhost:8004/docs
ReDoc:        http://localhost:8004/redoc
"""
import os
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from .router import router as mlr_router

app = FastAPI(
    title="Clearline Analytics — MLR API",
    description="Healthcare analytics API for Clearline International HMO",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(mlr_router, prefix="/mlr", tags=["MLR Analysis"])


@app.get("/", tags=["Health"])
def root():
    return {"status": "ok", "message": "Clearline MLR API is running"}


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8004))
    host = os.getenv("HOST", "0.0.0.0")
    print(f"\n{'='*60}")
    print(f"  Clearline MLR API v1.0.0")
    print(f"  → http://{host}:{port}")
    print(f"  → Docs: http://{host}:{port}/docs")
    print(f"{'='*60}\n")
    uvicorn.run("apis.mlr.main:app", host=host, port=port, reload=True)
