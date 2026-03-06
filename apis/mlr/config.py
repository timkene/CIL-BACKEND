import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Database ──────────────────────────────────────────────────────────────────
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN", "")
MOTHERDUCK_DSN   = f"md:ai_driven_data?motherduck_token={MOTHERDUCK_TOKEN}"
SCHEMA           = "AI DRIVEN DATA"

# ── MLR thresholds (Nigerian HMO standard) ────────────────────────────────────
OVERHEAD_RATE        = 0.25   # 25 % total overhead (15% admin + 10% commission)
MLR_BREAK_EVEN       = 0.75   # >75 % = loss territory
MLR_WARNING          = 0.70   # 70–75 % = watch zone
