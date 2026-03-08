"""
Fraud Scoring API - Configuration
==================================
Central config for DB path, scoring thresholds, and API settings.
"""
import os

# ── Database ──────────────────────────────────────────────────────────────────
DB_PATH   = "/Users/kenechukwuchukwuka/Downloads/DLT/ai_driven_data.duckdb"
DB_SCHEMA = "AI DRIVEN DATA"
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# ── Scoring weights (max possible score = 10) ─────────────────────────────────
SCORE_WEIGHTS = {
    "vpe":            1,
    "cpe":            2,
    "cpv":            2,
    "drug_ratio":     2,
    "dx_repeat":      1,
    "short_interval": 2,
}

ALERT_THRESHOLD       = 5
WATCHLIST_THRESHOLD   = 3
MIN_PEER_ENROLLEES    = 5
SHORT_INTERVAL_DAYS   = 14
DX_REPEAT_DAYS        = 14

DRUG_RATIO_HIGH       = 55.0
DRUG_RATIO_MEDIUM     = 40.0
DX_REPEAT_THRESHOLD   = 30.0

NETWORK_CPE_MIN_GROUPS      = 3     # min groups a provider must serve for network benchmark
NETWORK_CPE_RATIO_THRESHOLD = 1.5   # cpe_ratio above this → GROUP_TARGETED

DRUG_CODE_PREFIXES    = ("DRG", "MED", "BRG", "PRE")
EXCLUDE_NAME_KEYWORDS = ("pharmac", "chemist", "laborator", "lab ", "diagnostic centre")

# ── AI Medical Analyst ────────────────────────────────────────────────────────
ANTHROPIC_MODEL  = "claude-sonnet-4-20250514"
AI_MAX_TOKENS    = 1500