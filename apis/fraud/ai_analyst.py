"""
Fraud Scoring API - AI Medical Analyst
=======================================
Calls Claude API to provide medical judgment on flagged signals.
Covers 5 medical intelligence touchpoints:
  1. Diagnosis repeat clinical validity
  2. Drug ratio interpretation by facility type
  3. CPE plausibility given case mix
  4. Short interval clinical justification
  5. Overall risk narrative + recommended actions
"""

import httpx
import json
from typing import Optional, List
from .models import AICommentary
from .config import ANTHROPIC_MODEL, AI_MAX_TOKENS, ANTHROPIC_API_KEY


ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"


async def get_ai_commentary(
    provider_name: str,
    band: str,
    state: Optional[str],
    raw_metrics: dict,
    peer_benchmarks: list,
    top_dx_repeats: list,
    short_interval_pct: float,
    short_interval_buckets: list,
    metric_scores: list,
    total_score: int,
    alert_status: str,
    period: dict,
) -> Optional[AICommentary]:
    """
    Send structured fraud signal data to Claude API.
    Returns AICommentary with medical assessments and recommended actions.
    """

    # Build peer benchmark summary string
    bench_text = "\n".join(
        f"  {b['metric']}: median={b['median']:,.0f}, "
        f"Q1={b['q1']:,.0f}, Q3={b['q3']:,.0f}, "
        f"Tukey threshold={b['tukey_threshold']:,.0f}"
        for b in peer_benchmarks
    )

    # Build score summary
    score_text = "\n".join(
        f"  {s['metric']}: value={s['value']}, threshold={s['threshold']}, "
        f"breached={s['breached']}, score={s['score']}/{s['max_score']}"
        for s in metric_scores
    )

    # Top repeated diagnoses (with description so AI cannot misread ICD codes)
    dx_text = "\n".join(
        f"  {d['diagnosis_code']} — {d.get('diagnosis_desc', 'Unknown')}: "
        f"repeated {d['repeat_count']} times within 14 days"
        for d in top_dx_repeats
    ) or "  None"

    # Short interval breakdown
    si_text = "\n".join(
        f"  {b['bucket']}: {b['enrollee_count']} enrollees, mean gap {b['mean_gap_days']} days"
        for b in short_interval_buckets
    ) or "  None"

    prompt = f"""You are a healthcare fraud detection specialist for a Nigerian HMO (Health Maintenance Organization).
You are reviewing a provider fraud score report and must provide clinical and actuarial medical judgment.

=== PROVIDER DETAILS ===
Name:   {provider_name}
Band:   {band}  (Band A=highest tier, Band D=lowest tier in Nigeria HMO network)
State:  {state or 'Unknown'}
Period: {period['start_date']} to {period['end_date']}

=== RAW METRICS ===
Total Cost:       ₦{raw_metrics['total_cost']:,.2f}
Unique Enrollees: {raw_metrics['unique_enrollees']}
Total Visits:     {raw_metrics['total_visits']} (PA-based: {raw_metrics['pa_visits']}, No-PA: {raw_metrics['no_pa_visits']})
CPE:              ₦{raw_metrics['cpe']:,.2f}
CPV:              ₦{raw_metrics['cpv']:,.2f}
VPE:              {raw_metrics['vpe']}
Drug Ratio:       {raw_metrics['drug_ratio_pct']}%

=== PEER BAND BENCHMARKS (Band {band}) ===
{bench_text}

=== FRAUD METRIC SCORES ===
{score_text}
TOTAL SCORE: {total_score}/10  →  STATUS: {alert_status}

=== DIAGNOSIS REPEAT (same enrollee, different visit, within 14 days) ===
{dx_text}

=== SHORT VISIT INTERVAL BREAKDOWN ===
{si_text}
% of multi-visit enrollees with avg gap < 14 days: {short_interval_pct}%

=== YOUR TASK ===
Provide medical intelligence in the following JSON format ONLY. No preamble, no markdown, just raw JSON:
{{
  "dx_repeat_assessment": "2-3 sentences: Are these repeated diagnoses clinically plausible? Which ones are red flags and why? Consider Nigerian disease burden (malaria, hypertension, URI are common). Flag diagnoses that should not recur within 14 days vs those that could.",
  "cost_intensity_assessment": "2-3 sentences: Is the CPE/CPV elevated cost plausible given the case mix (diagnoses seen), band level, and facility type? What types of billing inflation would explain this pattern?",
  "short_interval_assessment": "2-3 sentences: Are the visit frequency patterns clinically justified? What legitimate reasons exist for very short intervals (e.g. wound care, IV infusion, antenatal) vs suspicious reasons? Flag the <3 day bucket especially.",
  "overall_risk_narrative": "3-4 sentences: Synthesize all signals into a coherent fraud risk narrative. What is the most likely fraud mechanism if any? How confident are you in the risk level?",
  "recommended_actions": ["action 1", "action 2", "action 3"]
}}

Focus on Nigerian HMO context: malaria repeated within 14 days is suspicious (treatment course is 3 days), hypertension repeating is expected (chronic), sepsis repeating within 14 days is clinically implausible, URI within 7 days may indicate false billing."""

    payload = {
        "model":      ANTHROPIC_MODEL,
        "max_tokens": AI_MAX_TOKENS,
        "messages":   [{"role": "user", "content": prompt}],
    }

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                ANTHROPIC_URL,
                json=payload,
                headers={
                    "Content-Type":      "application/json",
                    "x-api-key":         ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                },
            )
            resp.raise_for_status()
            data = resp.json()

        # Extract text from response
        text = ""
        for block in data.get("content", []):
            if block.get("type") == "text":
                text += block.get("text", "")

        # Clean and parse JSON
        text = text.strip()
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        text = text.strip().rstrip("```").strip()

        parsed = json.loads(text)
        return AICommentary(
            dx_repeat_assessment       = parsed.get("dx_repeat_assessment", ""),
            cost_intensity_assessment  = parsed.get("cost_intensity_assessment", ""),
            short_interval_assessment  = parsed.get("short_interval_assessment", ""),
            overall_risk_narrative     = parsed.get("overall_risk_narrative", ""),
            recommended_actions        = parsed.get("recommended_actions", []),
        )

    except Exception as e:
        # Return a degraded commentary rather than crashing
        return AICommentary(
            dx_repeat_assessment       = f"AI commentary unavailable: {str(e)}",
            cost_intensity_assessment  = "",
            short_interval_assessment  = "",
            overall_risk_narrative     = "Manual clinical review recommended.",
            recommended_actions        = ["Manual review required — AI commentary failed"],
        )