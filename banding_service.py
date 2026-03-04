"""
banding_service.py
==================
Pure Python banding engine extracted from adv_hosp_band2.py and adv_hosp_claims_band.py.
No Streamlit. Suitable for use inside FastAPI endpoints.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import duckdb
import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Configuration  (override via environment variables)
# ---------------------------------------------------------------------------
DB_PATH = os.getenv(
    "DUCKDB_PATH",
    "/Users/kenechukwuchukwuka/Downloads/DLT/ai_driven_data.duckdb",
)
TARIFF_PATH = os.getenv(
    "REALITY_TARIFF_PATH",
    "/Users/kenechukwuchukwuka/Downloads/REALITY TARIFF.xlsx",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize_code(value) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return ""
    return str(value).strip().lower().replace(" ", "")


def _get_conn(read_only: bool = True) -> duckdb.DuckDBPyConnection:
    """Use main app's pooled connection when USE_SHARED_DB_CONNECTION=1 to avoid DuckDB 'different configuration' error."""
    if os.getenv("USE_SHARED_DB_CONNECTION"):
        from core.database import get_db_connection
        conn = get_db_connection(read_only=read_only)
        # Wrap so conn.close() is a no-op (pooled connection must not be closed by banding)
        class _NoCloseConn:
            def __init__(self, c):
                self._c = c
            def close(self):
                pass
            def __getattr__(self, name):
                return getattr(self._c, name)
        return _NoCloseConn(conn)
    if not Path(DB_PATH).exists():
        raise FileNotFoundError(f"DuckDB not found: {DB_PATH}")
    return duckdb.connect(DB_PATH, read_only=read_only)


# ---------------------------------------------------------------------------
# Standard tariff loader
# ---------------------------------------------------------------------------

def load_standard_tariff() -> pd.DataFrame:
    """Load REALITY_TARIFF.xlsx and validate required columns."""
    path = Path(TARIFF_PATH)
    if not path.exists():
        raise FileNotFoundError(f"REALITY_TARIFF not found: {TARIFF_PATH}")
    df = pd.read_excel(path)
    required = ["procedurecode", "band_a", "band_b", "band_c", "band_d", "band_special"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"REALITY_TARIFF missing columns: {missing}")
    return df


# ---------------------------------------------------------------------------
# DB queries
# ---------------------------------------------------------------------------

def load_claims_stats() -> pd.DataFrame:
    """Load per-procedure percentiles + count from claims (for frequency weighting & fraud)."""
    try:
        conn = _get_conn()
        query = """
        WITH cleaned AS (
            SELECT LOWER(TRIM(code)) AS procedurecode, chargeamount
            FROM "AI DRIVEN DATA"."CLAIMS DATA"
            WHERE code IS NOT NULL AND chargeamount > 0
        ),
        pct AS (
            SELECT procedurecode,
                APPROX_QUANTILE(chargeamount, 0.01) AS p01,
                APPROX_QUANTILE(chargeamount, 0.90) AS p90,
                APPROX_QUANTILE(chargeamount, 0.95) AS p95,
                APPROX_QUANTILE(chargeamount, 0.99) AS p99,
                COUNT(*) AS total_count
            FROM cleaned GROUP BY procedurecode
        )
        SELECT c.procedurecode, p.p90, p.p95,
               AVG(c.chargeamount) AS mean,
               STDDEV(c.chargeamount) AS std,
               COUNT(*) AS count
        FROM cleaned c
        JOIN pct p ON c.procedurecode = p.procedurecode
        WHERE c.chargeamount >= p.p01 AND c.chargeamount <= p.p99
        GROUP BY c.procedurecode, p.p90, p.p95
        HAVING COUNT(*) >= 5
        """
        df = conn.execute(query).fetchdf()
        conn.close()
        return df
    except Exception as e:
        print(f"[WARN] claims_stats load failed: {e}")
        return pd.DataFrame()


def load_provider_list() -> pd.DataFrame:
    """Return all providers that have tariff data: providerid, providername, current_band."""
    conn = _get_conn()
    query = """
    SELECT DISTINCT
        CAST(p.providerid AS VARCHAR)              AS providerid,
        TRIM(p.providername)                       AS providername,
        COALESCE(NULLIF(TRIM(p.bands), ''), 'Unspecified') AS current_band
    FROM "AI DRIVEN DATA"."PROVIDERS" p
    INNER JOIN "AI DRIVEN DATA"."PROVIDERS_TARIFF" pt ON p.protariffid = pt.protariffid
    INNER JOIN "AI DRIVEN DATA"."TARIFF" t ON pt.tariffid = t.tariffid
    WHERE t.tariffamount > 0 AND t.procedurecode IS NOT NULL
      AND LOWER(p.providername) NOT LIKE '%nhis%'
    ORDER BY p.providername
    """
    df = conn.execute(query).fetchdf()
    conn.close()
    return df


def calculate_banding_summary_basic(lookback_months: int = 6) -> pd.DataFrame:
    """
    Compute tariff + reality banding for ALL providers with tariff data.

    This is the core \"summary\" computation for the Hospital Band Analysis module,
    analogous to calculate_mlr_basic / calculate_client_summary_basic.

    Intended usage:
    - A nightly (or on‑demand) batch job calls this function once, then writes the
      resulting DataFrame into a DuckDB summary table such as
      \"AI DRIVEN DATA\".\"HOSPITAL_BANDING_SUMMARY\".
    - The /banding/analyze endpoint (or a new summary endpoint) can later be
      switched to read from that summary table for instant results instead of
      recomputing per provider.

    Returns a pandas DataFrame with one row per provider containing:
    - providerid, providername, current_band
    - tariff_weighted_band, tariff_unweighted_band
    - reality_weighted_band, reality_unweighted_band (where available)
    - fraud_risk_tariff, fraud_risk_reality
    - pricing_behaviour_flag, claims_coverage_pct, overcharging_pct, undercharging_pct
    - matched_procedures_tariff, coverage_pct_tariff
    - matched_procedures_reality, coverage_pct_reality, confidence_reality
    - lookback_months, summary_timestamp

    NOTE: This function does NOT write to the database itself; callers are
    expected to persist the DataFrame as needed.
    """
    # Lazily construct engines (loads standard tariff + claims stats once)
    tariff_engine, reality_engine = get_engines()

    providers_df = load_provider_list()
    if providers_df is None or providers_df.empty:
        return pd.DataFrame(
            columns=[
                "providerid",
                "providername",
                "current_band",
                "tariff_weighted_band",
                "tariff_unweighted_band",
                "reality_weighted_band",
                "reality_unweighted_band",
                "fraud_risk_tariff",
                "fraud_risk_reality",
                "pricing_behaviour_flag",
                "claims_coverage_pct",
                "overcharging_pct",
                "undercharging_pct",
                "matched_procedures_tariff",
                "coverage_pct_tariff",
                "matched_procedures_reality",
                "coverage_pct_reality",
                "confidence_reality",
                "lookback_months",
                "summary_timestamp",
            ]
        )

    rows = []
    now_iso = datetime.now().isoformat()

    for _, row in providers_df.iterrows():
        prov_id = str(row.get("providerid", "") or "")
        display_name = str(row.get("providername", "") or "")
        current_band = str(row.get("current_band", "") or "Unspecified")

        try:
            # Official tariff for this provider
            tariff_df = get_provider_tariff(prov_id)
            if tariff_df is None or tariff_df.empty:
                continue

            # Tariff-based banding (Script 1)
            t_result = tariff_engine.analyze(tariff_df, provider_id=prov_id)
            if not t_result.get("success", False):
                continue

            tw = t_result.get("weighted", {}) or {}
            tu = t_result.get("unweighted", {}) or {}
            fraud_tariff = t_result.get("fraud", {}) or {}

            # Reality-based tariff (claims) and banding (Script 2)
            reality_df = build_reality_tariff(
                prov_id, tariff_engine.standard_df, lookback_months
            )
            if reality_df is not None and not reality_df.empty:
                r_result = reality_engine.analyze(
                    reality_df, provider_name=display_name
                )
            else:
                r_result = {"success": False}

            if r_result.get("success", False):
                rw = r_result.get("weighted", {}) or {}
                ru = r_result.get("unweighted", {}) or {}
                pb = r_result.get("pricing_behaviour", {}) or {}
                fraud_reality = r_result.get("fraud", {}) or {}
            else:
                rw = {}
                ru = {}
                pb = {}
                fraud_reality = {}

            rows.append(
                {
                    "providerid": prov_id,
                    "providername": display_name,
                    "current_band": current_band,
                    "tariff_weighted_band": tw.get("band"),
                    "tariff_unweighted_band": tu.get("band"),
                    "reality_weighted_band": rw.get("band"),
                    "reality_unweighted_band": ru.get("band"),
                    "fraud_risk_tariff": fraud_tariff.get("fraud_risk")
                    if fraud_tariff.get("available")
                    else None,
                    "fraud_risk_reality": fraud_reality.get("fraud_risk")
                    if fraud_reality.get("available")
                    else None,
                    "pricing_behaviour_flag": pb.get("flag"),
                    "claims_coverage_pct": pb.get("claims_coverage_pct"),
                    "overcharging_pct": pb.get("overcharging_pct"),
                    "undercharging_pct": pb.get("undercharging_pct"),
                    "matched_procedures_tariff": t_result.get("matched_procedures"),
                    "coverage_pct_tariff": t_result.get("coverage_pct"),
                    "matched_procedures_reality": r_result.get(
                        "matched_procedures"
                    )
                    if r_result.get("success")
                    else None,
                    "coverage_pct_reality": r_result.get("coverage_pct")
                    if r_result.get("success")
                    else None,
                    "confidence_reality": r_result.get("confidence")
                    if r_result.get("success")
                    else None,
                    "lookback_months": lookback_months,
                    "summary_timestamp": now_iso,
                }
            )
        except Exception as exc:  # pragma: no cover - defensive
            print(f"[WARN] banding summary failed for provider {prov_id}: {exc}")
            continue

    return pd.DataFrame(rows) if rows else pd.DataFrame()


def get_provider_tariff(provider_id: str) -> Optional[pd.DataFrame]:
    """Fetch official tariff for a provider from the DB."""
    try:
        conn = _get_conn()
        query = """
        SELECT LOWER(TRIM(t.procedurecode)) AS procedurecode,
               CAST(t.tariffamount AS DOUBLE)  AS tariffamount
        FROM "AI DRIVEN DATA"."PROVIDERS" p
        INNER JOIN "AI DRIVEN DATA"."PROVIDERS_TARIFF" pt ON p.protariffid = pt.protariffid
        INNER JOIN "AI DRIVEN DATA"."TARIFF" t ON pt.tariffid = t.tariffid
        WHERE CAST(p.providerid AS VARCHAR) = ?
          AND t.tariffamount > 0 AND t.procedurecode IS NOT NULL
        ORDER BY t.procedurecode
        """
        df = conn.execute(query, [provider_id.strip()]).fetchdf()
        conn.close()
        if df.empty:
            return None
        df["procedurecode"] = df["procedurecode"].apply(_normalize_code)
        return df
    except Exception as e:
        print(f"[WARN] get_provider_tariff failed: {e}")
        return None


def build_reality_tariff(
    provider_id: str, standard_procedures: pd.DataFrame, lookback_months: int = 6
) -> pd.DataFrame:
    """
    Build hybrid tariff using actual claims (Script 2 logic).
    - For procedures WITH claims: AVG(approvedamount) from last N months
    - For procedures WITHOUT claims: official tariff price
    """
    try:
        conn = _get_conn()
        cutoff = (datetime.now() - timedelta(days=lookback_months * 30)).strftime("%Y-%m-%d")

        claims_query = f"""
        SELECT LOWER(TRIM(c.code)) AS procedurecode,
               COUNT(*)                           AS claims_count,
               AVG(c.approvedamount)              AS avg_approved
        FROM "AI DRIVEN DATA"."CLAIMS DATA" c
        WHERE c.nhisproviderid = '{provider_id}'
          AND c.datesubmitted >= DATE '{cutoff}'
          AND c.code IS NOT NULL
          AND c.approvedamount > 0
        GROUP BY LOWER(TRIM(c.code))
        """
        claims_pricing = conn.execute(claims_query).fetchdf()

        # Get provider's tariff ID
        tid_result = conn.execute(
            f"""SELECT DISTINCT pt.tariffid
                FROM "AI DRIVEN DATA"."PROVIDERS" p
                INNER JOIN "AI DRIVEN DATA"."PROVIDERS_TARIFF" pt ON p.protariffid = pt.protariffid
                WHERE p.providerid = '{provider_id}'
                LIMIT 1"""
        ).fetchdf()

        if tid_result.empty:
            conn.close()
            return pd.DataFrame()

        tariffid = tid_result["tariffid"].iloc[0]

        official_pricing = conn.execute(
            f"""SELECT LOWER(TRIM(t.procedurecode)) AS procedurecode,
                       t.tariffamount               AS official_price
                FROM "AI DRIVEN DATA"."TARIFF" t
                WHERE t.tariffid = {tariffid} AND t.tariffamount > 0"""
        ).fetchdf()
        conn.close()

        # Build hybrid
        std = standard_procedures.copy()
        std["procedurecode"] = std["procedurecode"].apply(_normalize_code)
        rows = []

        for _, sp in std.iterrows():
            code = sp["procedurecode"]
            desc = sp.get("proceduredesc", "N/A")

            cm = claims_pricing[claims_pricing["procedurecode"] == code]
            if not cm.empty:
                cavg = float(cm["avg_approved"].iloc[0])
                ccount = int(cm["claims_count"].iloc[0])
                om = official_pricing[official_pricing["procedurecode"] == code]
                oprice = float(om["official_price"].iloc[0]) if not om.empty else None
                diff = (cavg - oprice) if oprice else None
                diff_pct = ((cavg - oprice) / oprice * 100) if oprice and oprice > 0 else None
                rows.append(
                    dict(procedurecode=code, proceduredesc=desc,
                         reality_price=cavg, price_source="CLAIMS",
                         claims_count=ccount, official_tariff_price=oprice,
                         price_difference=diff, price_difference_pct=diff_pct)
                )
            else:
                om = official_pricing[official_pricing["procedurecode"] == code]
                if not om.empty:
                    oprice = float(om["official_price"].iloc[0])
                    rows.append(
                        dict(procedurecode=code, proceduredesc=desc,
                             reality_price=oprice, price_source="TARIFF",
                             claims_count=0, official_tariff_price=oprice,
                             price_difference=0.0, price_difference_pct=0.0)
                    )

        return pd.DataFrame(rows) if rows else pd.DataFrame()

    except Exception as e:
        print(f"[WARN] build_reality_tariff failed: {e}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Banding Engine (shared logic)
# ---------------------------------------------------------------------------

class _BaseBandingEngine:
    def __init__(self, standard_tariff_df: pd.DataFrame, claims_stats: pd.DataFrame = None):
        self.standard_df = standard_tariff_df.copy()
        self.claims_stats = claims_stats
        self.thresholds: Dict = {}
        self.std_dict: Dict = {}
        self._prepare()
        self._calc_thresholds()

    def _prepare(self):
        self.standard_df["procedurecode"] = self.standard_df["procedurecode"].apply(_normalize_code)
        for col in ["band_a", "band_b", "band_c", "band_d", "band_special"]:
            if col in self.standard_df.columns:
                self.standard_df[col] = pd.to_numeric(self.standard_df[col], errors="coerce")
        if "frequency" not in self.standard_df.columns:
            self.standard_df["frequency"] = 1
        self.standard_df["effective_frequency"] = self.standard_df["frequency"]
        if self.claims_stats is not None and not self.claims_stats.empty:
            freq_map = self.claims_stats.set_index("procedurecode")["count"].to_dict()
            for idx, row in self.standard_df.iterrows():
                if row["procedurecode"] in freq_map:
                    self.standard_df.at[idx, "effective_frequency"] = freq_map[row["procedurecode"]]
        self.std_dict = {r["procedurecode"]: r for _, r in self.standard_df.iterrows()}

    def _calc_thresholds(self):
        self.thresholds = {
            "D": float(self.standard_df["band_d"].mean()),
            "C": float(self.standard_df["band_c"].mean()),
            "B": float(self.standard_df["band_b"].mean()),
            "A": float(self.standard_df["band_a"].mean()),
            "Special": float(self.standard_df["band_special"].mean()),
        }

    def _proc_band(self, price: float, sp: Dict) -> str:
        if price <= sp["band_d"]:   return "D"
        if price <= sp["band_c"]:   return "C"
        if price <= sp["band_b"]:   return "B"
        if price <= sp["band_a"]:   return "A"
        if price <= sp["band_special"]: return "Special"
        return "check"

    def _band_from_avg(self, avg: float) -> str:
        if avg <= self.thresholds["D"]:       return "D"
        if avg <= self.thresholds["C"]:       return "C"
        if avg <= self.thresholds["B"]:       return "B"
        if avg <= self.thresholds["A"]:       return "A"
        if avg <= self.thresholds["Special"]: return "Special"
        return "check"

    def _resolve_band(self, band_weights: Dict, calculated_band: str, check_pct: float) -> Tuple[str, str]:
        """Dominant ≥60% → use dominant; else weighted avg; gaming protection if check > 30%."""
        valid = {b: w for b, w in band_weights.items() if b != "check"}
        total = sum(valid.values())
        if total == 0:
            return calculated_band, "Weighted Average"
        dominant = max(valid, key=valid.get)
        dominant_pct = valid[dominant] / total * 100
        if check_pct > 30:
            return calculated_band, "Gaming Protection"
        if dominant_pct >= 60:
            return dominant, f"Dominant Band ({dominant_pct:.0f}%)"
        return calculated_band, "Weighted Average"

    def _fraud_assessment(self, tariff_df: pd.DataFrame) -> Dict:
        """P90/P95 fraud risk assessment using claims statistics."""
        if self.claims_stats is None or self.claims_stats.empty:
            return {"available": False}
        claims_dict = {r["procedurecode"]: r for _, r in self.claims_stats.iterrows()}
        above_p90, above_p95, extreme = [], [], []
        for _, row in tariff_df.iterrows():
            code = row.get("procedurecode") or row.get("procedurecode")
            price = row.get("tariffamount") or row.get("reality_price", 0)
            if code not in claims_dict:
                continue
            cd = claims_dict[code]
            p90, p95 = cd.get("p90"), cd.get("p95")
            if pd.notna(p90) and price > p90:
                above_p90.append({"procedurecode": code, "price": price, "p90": p90})
            if pd.notna(p95) and price > p95:
                above_p95.append({"procedurecode": code, "price": price, "p95": p95,
                                   "excess_pct": (price - p95) / p95 * 100 if p95 > 0 else 0})
                if price > p95 * 2:
                    extreme.append({"procedurecode": code, "price": price, "p95": p95})
        total = len(tariff_df)
        p95_pct = len(above_p95) / total * 100 if total else 0
        p90_pct = len(above_p90) / total * 100 if total else 0
        if len(extreme) > 0 or p95_pct > 20:
            risk = "HIGH"
        elif p95_pct > 10 or p90_pct > 40:
            risk = "MEDIUM"
        elif p90_pct > 20:
            risk = "LOW"
        else:
            risk = "MINIMAL"
        return {
            "available": True,
            "fraud_risk": risk,
            "above_p90_count": len(above_p90),
            "above_p95_count": len(above_p95),
            "extreme_outliers": len(extreme),
            "above_p95_pct": round(p95_pct, 1),
            "above_p90_pct": round(p90_pct, 1),
        }


# ---------------------------------------------------------------------------
# Script 1: Tariff-Based Banding  (adv_hosp_band2 logic)
# ---------------------------------------------------------------------------

class TariffBandingEngine(_BaseBandingEngine):
    """
    Bands a hospital using its PUBLISHED tariff (official or CSV-uploaded).
    Dual output: weighted (TCOC) + unweighted (unit price).
    """

    def analyze(self, tariff_df: pd.DataFrame, provider_id: str = None) -> Dict:
        tariff_df = tariff_df.copy()
        tariff_df["procedurecode"] = tariff_df["procedurecode"].apply(_normalize_code)
        tariff_df["tariffamount"] = pd.to_numeric(tariff_df["tariffamount"], errors="coerce")
        tariff_df = tariff_df.dropna(subset=["tariffamount"])
        tariff_df = tariff_df[tariff_df["tariffamount"] > 0]

        if tariff_df.empty:
            return {"success": False, "error": "No valid procedure rows"}

        # --- Weighted ---
        w_total_price, w_total_freq = 0.0, 0.0
        w_band_freq = {b: 0.0 for b in ["A", "B", "C", "D", "Special", "check"]}
        # --- Unweighted ---
        u_total_price, u_count = 0.0, 0
        u_band_count = {b: 0 for b in ["A", "B", "C", "D", "Special", "check"]}
        matched = 0

        for _, row in tariff_df.iterrows():
            code = row["procedurecode"]
            price = row["tariffamount"]
            if code not in self.std_dict:
                continue
            matched += 1
            sp = self.std_dict[code]
            freq = float(sp.get("effective_frequency", 1))
            log_freq = np.log1p(freq)
            band = self._proc_band(price, sp)

            w_total_price += price * log_freq
            w_total_freq += log_freq
            w_band_freq[band] += log_freq

            u_total_price += price
            u_count += 1
            u_band_count[band] += 1

        if w_total_freq == 0 or u_count == 0:
            return {"success": False, "error": "No matching procedures in standard tariff"}

        w_avg = w_total_price / w_total_freq
        w_calc_band = self._band_from_avg(w_avg)
        total_wf = sum(w_band_freq.values())
        w_pcts = {b: w / total_wf * 100 for b, w in w_band_freq.items()} if total_wf > 0 else {}
        w_final, w_method = self._resolve_band(w_band_freq, w_calc_band, w_pcts.get("check", 0))

        u_avg = u_total_price / u_count
        u_calc_band = self._band_from_avg(u_avg)
        u_pcts = {b: c / u_count * 100 for b, c in u_band_count.items()} if u_count > 0 else {}
        u_final, u_method = self._resolve_band(u_band_count, u_calc_band, u_pcts.get("check", 0))

        # Fraud assessment
        fraud = self._fraud_assessment(tariff_df)

        return {
            "success": True,
            "matched_procedures": matched,
            "coverage_pct": round(matched / len(self.std_dict) * 100, 1) if self.std_dict else 0,
            "weighted": {
                "band": w_final,
                "calculated_band": w_calc_band,
                "avg": round(w_avg, 2),
                "method": w_method,
                "band_distribution": {k: round(v, 1) for k, v in w_pcts.items()},
            },
            "unweighted": {
                "band": u_final,
                "calculated_band": u_calc_band,
                "avg": round(u_avg, 2),
                "method": u_method,
                "band_distribution": {k: round(v, 1) for k, v in u_pcts.items()},
            },
            "fraud": fraud,
        }


# ---------------------------------------------------------------------------
# Script 2: Reality-Adjusted Banding  (adv_hosp_claims_band logic)
# ---------------------------------------------------------------------------

class RealityBandingEngine(_BaseBandingEngine):
    """
    Bands a hospital using ACTUAL BILLING from claims (hybrid tariff).
    Dual output: weighted (TCOC) + unweighted; plus pricing behaviour flag.
    """

    def analyze(self, reality_tariff_df: pd.DataFrame, provider_name: str = "") -> Dict:
        if reality_tariff_df.empty:
            return {"success": False, "error": "Empty reality tariff"}

        w_total_price, w_total_freq = 0.0, 0.0
        w_band_freq = {b: 0.0 for b in ["A", "B", "C", "D", "Special", "check"]}
        u_total_price, u_count = 0.0, 0
        u_band_count = {b: 0 for b in ["A", "B", "C", "D", "Special", "check"]}

        matched = 0
        claims_based = 0
        tariff_based = 0
        overcharging = 0
        undercharging = 0
        total_overcharge = 0.0
        total_undercharge = 0.0

        for _, row in reality_tariff_df.iterrows():
            code = row["procedurecode"]
            rp = row["reality_price"]
            source = row.get("price_source", "TARIFF")
            diff = row.get("price_difference", 0) or 0

            if code not in self.std_dict:
                continue
            matched += 1
            sp = self.std_dict[code]

            if source == "CLAIMS":
                claims_based += 1
                if diff > 0:
                    overcharging += 1
                    total_overcharge += diff
                elif diff < 0:
                    undercharging += 1
                    total_undercharge += abs(diff)
            else:
                tariff_based += 1

            freq = float(sp.get("effective_frequency", 1))
            log_freq = np.log1p(freq)
            band = self._proc_band(rp, sp)

            w_total_price += rp * log_freq
            w_total_freq += log_freq
            w_band_freq[band] += log_freq

            u_total_price += rp
            u_count += 1
            u_band_count[band] += 1

        if w_total_freq == 0 or u_count == 0:
            return {"success": False, "error": "No matching procedures"}

        # Weighted
        w_avg = w_total_price / w_total_freq
        w_calc = self._band_from_avg(w_avg)
        total_wf = sum(w_band_freq.values())
        w_pcts = {b: w / total_wf * 100 for b, w in w_band_freq.items()} if total_wf > 0 else {}
        w_final, w_method = self._resolve_band(w_band_freq, w_calc, w_pcts.get("check", 0))

        # Unweighted
        u_avg = u_total_price / u_count
        u_calc = self._band_from_avg(u_avg)
        u_pcts = {b: c / u_count * 100 for b, c in u_band_count.items()} if u_count > 0 else {}
        u_final, u_method = self._resolve_band(u_band_count, u_calc, u_pcts.get("check", 0))

        # Pricing behaviour
        opct = overcharging / claims_based * 100 if claims_based > 0 else 0
        upct = undercharging / claims_based * 100 if claims_based > 0 else 0
        if opct > 50:       behavior = "SYSTEMATIC_OVERCHARGING"
        elif upct > 50:     behavior = "GENEROUS_DISCOUNTER"
        elif opct > 25:     behavior = "MODERATE_OVERCHARGING"
        elif upct > 25:     behavior = "MODERATE_DISCOUNTING"
        else:               behavior = "ALIGNED_WITH_TARIFF"

        # Confidence
        vol_conf = min(1.0, np.log1p(w_total_freq) / np.log1p(10000))
        cov_conf = min(1.0, matched / len(self.std_dict)) if self.std_dict else 0
        confidence = 0.60 * vol_conf + 0.40 * cov_conf

        fraud = self._fraud_assessment(
            reality_tariff_df.rename(columns={"reality_price": "tariffamount"})
        )

        return {
            "success": True,
            "provider_name": provider_name,
            "matched_procedures": matched,
            "coverage_pct": round(matched / len(self.std_dict) * 100, 1) if self.std_dict else 0,
            "confidence": round(confidence, 3),
            "weighted": {
                "band": w_final,
                "calculated_band": w_calc,
                "avg": round(w_avg, 2),
                "method": w_method,
                "band_distribution": {k: round(v, 1) for k, v in w_pcts.items()},
            },
            "unweighted": {
                "band": u_final,
                "calculated_band": u_calc,
                "avg": round(u_avg, 2),
                "method": u_method,
                "band_distribution": {k: round(v, 1) for k, v in u_pcts.items()},
            },
            "pricing_behaviour": {
                "flag": behavior,
                "claims_based_procedures": claims_based,
                "tariff_based_procedures": tariff_based,
                "claims_coverage_pct": round(claims_based / matched * 100, 1) if matched else 0,
                "overcharging_count": overcharging,
                "overcharging_pct": round(opct, 1),
                "undercharging_count": undercharging,
                "undercharging_pct": round(upct, 1),
                "total_overcharge_amount": round(total_overcharge, 2),
                "total_undercharge_amount": round(total_undercharge, 2),
            },
            "fraud": fraud,
        }


# ---------------------------------------------------------------------------
# Singleton cache (loaded once per process lifetime)
# ---------------------------------------------------------------------------

_ENGINE_CACHE: Dict = {}

def get_engines() -> Tuple[TariffBandingEngine, RealityBandingEngine]:
    """Return cached engine instances.  Thread-safe enough for single-worker FastAPI."""
    if "tariff" not in _ENGINE_CACHE:
        std = load_standard_tariff()
        stats = load_claims_stats()
        _ENGINE_CACHE["tariff"] = TariffBandingEngine(std, stats)
        _ENGINE_CACHE["reality"] = RealityBandingEngine(std, stats)
    return _ENGINE_CACHE["tariff"], _ENGINE_CACHE["reality"]


def invalidate_cache():
    _ENGINE_CACHE.clear()