"""
streamlit_tester.py
====================
Local Streamlit UI to test the Hospital Band Analysis API.

Run in two terminals:
    Terminal 1:  uvicorn main:app --reload --port 8000
    Terminal 2:  streamlit run streamlit_tester.py
"""

import io
import json

import pandas as pd
import requests
import streamlit as st

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Hospital Band Analysis Tester",
    page_icon="🏥",
    layout="wide",
)

API_BASE = st.sidebar.text_input("API Base URL", value="http://localhost:8000")

# ---------------------------------------------------------------------------
# Colour helpers
# ---------------------------------------------------------------------------
BAND_COLORS = {
    "A": "#e74c3c",
    "B": "#e67e22",
    "C": "#f1c40f",
    "D": "#2ecc71",
    "Special": "#9b59b6",
    "check": "#95a5a6",
    "N/A": "#bdc3c7",
}
BEHAVIOR_COLORS = {
    "SYSTEMATIC_OVERCHARGING": "#e74c3c",
    "MODERATE_OVERCHARGING":   "#e67e22",
    "ALIGNED_WITH_TARIFF":     "#2ecc71",
    "MODERATE_DISCOUNTING":    "#3498db",
    "GENEROUS_DISCOUNTER":     "#1abc9c",
}
FRAUD_COLORS = {
    "HIGH":    "#e74c3c",
    "MEDIUM":  "#e67e22",
    "LOW":     "#f1c40f",
    "MINIMAL": "#2ecc71",
}


def band_badge(band: str) -> str:
    color = BAND_COLORS.get(band, "#95a5a6")
    return (
        f'<span style="background:{color};color:white;padding:4px 14px;'
        f'border-radius:12px;font-weight:bold;font-size:1.1rem">'
        f'Band {band}</span>'
    )


def color_metric(label, value, color="#1f77b4"):
    st.markdown(
        f"""
        <div style="background:{color};border-radius:10px;padding:1rem;
                    text-align:center;color:white;margin-bottom:0.5rem">
            <div style="font-size:0.85rem;opacity:0.85">{label}</div>
            <div style="font-size:1.6rem;font-weight:bold">{value}</div>
        </div>""",
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# Load provider list
# ---------------------------------------------------------------------------
@st.cache_data(ttl=120)
def fetch_providers(base_url: str):
    r = requests.get(f"{base_url}/providers", timeout=15)
    r.raise_for_status()
    return pd.DataFrame(r.json())


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
st.markdown("## 🏥 Hospital Band Analysis — API Tester")
st.markdown(
    "Test the `/analyze` endpoint. Select an **existing provider** or **upload a CSV** for a new one."
)

mode = st.radio("Analysis Mode", ["Existing Provider", "New Provider (CSV Upload)"], horizontal=True)

# ── Sidebar ──────────────────────────────────────────────────────────────────
st.sidebar.markdown("### ⚙️ Settings")
lookback = st.sidebar.select_slider(
    "Reality Banding: Claims Lookback (months)",
    options=[3, 6, 9, 12, 18, 24],
    value=6,
)
st.sidebar.markdown("---")
if st.sidebar.button("🗑️ Clear API Cache"):
    r = requests.post(f"{API_BASE}/cache/clear")
    st.sidebar.success(r.json().get("message", "Done"))

# ── Input section ─────────────────────────────────────────────────────────────
selected_name = None
csv_bytes = None

if mode == "Existing Provider":
    with st.spinner("Loading provider list..."):
        try:
            prov_df = fetch_providers(API_BASE)
            names = sorted(prov_df["providername"].unique().tolist())
            selected_name = st.selectbox("Select Provider", names)

            if selected_name:
                row = prov_df[prov_df["providername"] == selected_name].iloc[0]
                c1, c2, c3 = st.columns(3)
                c1.metric("Provider ID", row["providerid"])
                c2.metric("Current Band (DB)", row["current_band"])
                c3.metric("Mode", "Existing")
        except Exception as e:
            st.error(f"Could not load providers: {e}")
            st.stop()

else:
    col_a, col_b = st.columns([2, 1])
    with col_a:
        uploaded = st.file_uploader(
            "Upload tariff CSV",
            type=["csv"],
            help="Must contain **procedurecode** and **tariffamount** columns",
        )
        if uploaded:
            csv_bytes = uploaded.read()
            preview = pd.read_csv(io.BytesIO(csv_bytes), thousands=",")
            st.dataframe(preview.head(5), use_container_width=True)
    with col_b:
        selected_name = st.text_input(
            "Provider Label (optional)", placeholder="e.g. New Hospital Abuja"
        )
        st.caption("Required columns: `procedurecode`, `tariffamount`")

# ── Analyze button ────────────────────────────────────────────────────────────
st.markdown("---")
analyze_btn = st.button("🔍 Run Analysis", type="primary", use_container_width=True)

if analyze_btn:
    if mode == "Existing Provider" and not selected_name:
        st.warning("Please select a provider.")
        st.stop()
    if mode == "New Provider (CSV Upload)" and not csv_bytes:
        st.warning("Please upload a CSV file.")
        st.stop()

    # ── Call API ──────────────────────────────────────────────────────────────
    with st.spinner("Analysing... this may take a moment for reality banding."):
        try:
            if mode == "Existing Provider":
                resp = requests.post(
                    f"{API_BASE}/analyze",
                    data={"provider_name": selected_name, "lookback_months": lookback},
                    timeout=120,
                )
            else:
                label = selected_name or ""
                resp = requests.post(
                    f"{API_BASE}/analyze",
                    data={"provider_name": label, "lookback_months": lookback},
                    files={"csv_file": ("tariff.csv", csv_bytes, "text/csv")},
                    timeout=120,
                )
        except requests.exceptions.ConnectionError:
            st.error("❌ Cannot connect to API. Is `uvicorn main:app` running?")
            st.stop()

    if resp.status_code != 200:
        st.error(f"API Error {resp.status_code}: {resp.text}")
        st.stop()

    data = resp.json()

    # ════════════════════════════════════════════════════════════════════════
    # Results
    # ════════════════════════════════════════════════════════════════════════
    st.success(f"✅ Analysis complete for **{data['provider_name']}**")
    st.markdown("---")

    # ── Top summary row ───────────────────────────────────────────────────────
    st.subheader("📊 Band Summary")
    cb = data["current_band"]
    tb_w = data["tariff_banding"]["weighted"]["band"]
    tb_u = data["tariff_banding"]["unweighted"]["band"]
    rb_w = data["reality_banding"]["weighted"]["band"] if data.get("reality_banding") else "—"
    rb_u = data["reality_banding"]["unweighted"]["band"] if data.get("reality_banding") else "—"

    cols = st.columns(5)
    labels_vals = [
        ("Current Band\n(DB)", cb),
        ("Tariff Band\n(Weighted)", tb_w),
        ("Tariff Band\n(Unweighted)", tb_u),
        ("Reality Band\n(Weighted)", rb_w),
        ("Reality Band\n(Unweighted)", rb_u),
    ]
    for col, (lbl, val) in zip(cols, labels_vals):
        with col:
            st.markdown(f"**{lbl}**")
            st.markdown(band_badge(val) if val != "—" else "**—**", unsafe_allow_html=True)

    # ── Band change alerts ────────────────────────────────────────────────────
    bc = data["band_changes"]
    alerts = []
    if bc.get("tariff_weighted_vs_current"):
        alerts.append(f"⚠️ Tariff weighted band **{tb_w}** ≠ current **{cb}**")
    if bc.get("tariff_unweighted_vs_current"):
        alerts.append(f"⚠️ Tariff unweighted band **{tb_u}** ≠ current **{cb}**")
    if bc.get("reality_weighted_vs_current"):
        alerts.append(f"⚠️ Reality weighted band **{rb_w}** ≠ current **{cb}**")
    if bc.get("tariff_vs_reality_weighted") is True:
        alerts.append(f"⚠️ Tariff vs Reality mismatch: **{tb_w}** vs **{rb_w}**")

    if alerts:
        for a in alerts:
            st.warning(a)
    else:
        st.success("✅ All bands consistent – no changes detected")

    st.markdown("---")

    # ── Detail tabs ──────────────────────────────────────────────────────────
    tabs = st.tabs(["📋 Tariff Banding (Script 1)", "🔍 Reality Banding (Script 2)", "📄 Raw JSON"])

    # ---- Tab 1: Tariff Banding -----------------------------------------------
    with tabs[0]:
        tb = data["tariff_banding"]
        st.markdown("### Tariff-Based Banding  *(published tariff / CSV)*")
        st.caption("Uses frequency-weighted TCOC approach (Script 1 logic)")

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Weighted Band",   tb["weighted"]["band"])
        c2.metric("Unweighted Band", tb["unweighted"]["band"])
        c3.metric("Matched Procedures", tb["matched_procedures"])
        c4.metric("Tariff Coverage", f"{tb['coverage_pct']}%")

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### Weighted (TCOC)")
            w = tb["weighted"]
            st.markdown(f"- **Final Band:** {w['band']}")
            st.markdown(f"- **Calculated Band:** {w['calculated_band']}")
            st.markdown(f"- **Weighted Avg Price:** ₦{w['avg']:,.0f}")
            st.markdown(f"- **Method:** {w['method']}")
            dist_df = pd.DataFrame(
                {"Band": list(w["band_distribution"].keys()),
                 "% Weight": list(w["band_distribution"].values())}
            ).sort_values("Band")
            st.dataframe(dist_df, use_container_width=True, hide_index=True)

        with col2:
            st.markdown("#### Unweighted (Unit Price)")
            u = tb["unweighted"]
            st.markdown(f"- **Final Band:** {u['band']}")
            st.markdown(f"- **Calculated Band:** {u['calculated_band']}")
            st.markdown(f"- **Simple Avg Price:** ₦{u['avg']:,.0f}")
            st.markdown(f"- **Method:** {u['method']}")
            dist_df = pd.DataFrame(
                {"Band": list(u["band_distribution"].keys()),
                 "% Count": list(u["band_distribution"].values())}
            ).sort_values("Band")
            st.dataframe(dist_df, use_container_width=True, hide_index=True)

        if tb.get("fraud_risk"):
            fc = FRAUD_COLORS.get(tb["fraud_risk"], "#bdc3c7")
            st.markdown(
                f'<div style="background:{fc};color:white;padding:0.8rem;border-radius:8px;margin-top:1rem">'
                f'🚨 Fraud Risk (vs P90/P95 benchmarks): <strong>{tb["fraud_risk"]}</strong></div>',
                unsafe_allow_html=True,
            )

    # ---- Tab 2: Reality Banding ----------------------------------------------
    with tabs[1]:
        st.markdown("### Reality-Adjusted Banding  *(claims-based)*")
        st.caption("Uses actual billing behaviour from claims history (Script 2 logic)")

        rb = data.get("reality_banding")
        if not rb:
            st.info(
                "Reality banding not available for this analysis. "
                "This is expected for CSV uploads (no claims history) "
                "or providers with no claims in the lookback period."
            )
        else:
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Weighted Band",     rb["weighted"]["band"])
            c2.metric("Unweighted Band",   rb["unweighted"]["band"])
            c3.metric("Confidence",        f"{rb['confidence']*100:.0f}%")
            c4.metric("Matched Procs",     rb["matched_procedures"])
            c5.metric("Coverage",          f"{rb['coverage_pct']}%")

            st.markdown("---")
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("#### Weighted (TCOC)")
                w = rb["weighted"]
                st.markdown(f"- **Final Band:** {w['band']}")
                st.markdown(f"- **Calculated Band:** {w['calculated_band']}")
                st.markdown(f"- **Weighted Avg Price:** ₦{w['avg']:,.0f}")
                st.markdown(f"- **Method:** {w['method']}")
                dist_df = pd.DataFrame(
                    {"Band": list(w["band_distribution"].keys()),
                     "% Weight": list(w["band_distribution"].values())}
                ).sort_values("Band")
                st.dataframe(dist_df, use_container_width=True, hide_index=True)

            with col2:
                st.markdown("#### Unweighted (Unit Price)")
                u = rb["unweighted"]
                st.markdown(f"- **Final Band:** {u['band']}")
                st.markdown(f"- **Calculated Band:** {u['calculated_band']}")
                st.markdown(f"- **Simple Avg Price:** ₦{u['avg']:,.0f}")
                st.markdown(f"- **Method:** {u['method']}")
                dist_df = pd.DataFrame(
                    {"Band": list(u["band_distribution"].keys()),
                     "% Count": list(u["band_distribution"].values())}
                ).sort_values("Band")
                st.dataframe(dist_df, use_container_width=True, hide_index=True)

            # Pricing behaviour
            st.markdown("---")
            st.markdown("#### 💰 Pricing Behaviour")
            pb = rb["pricing_behaviour"]
            bflag = pb["flag"]
            bcolor = BEHAVIOR_COLORS.get(bflag, "#95a5a6")
            st.markdown(
                f'<div style="background:{bcolor};color:white;padding:1rem;border-radius:10px;'
                f'font-size:1.1rem;font-weight:bold;text-align:center">{bflag}</div>',
                unsafe_allow_html=True,
            )

            bp1, bp2, bp3, bp4 = st.columns(4)
            bp1.metric("Claims-Based Procs",  pb["claims_based_procedures"])
            bp2.metric("Claims Coverage",     f"{pb['claims_coverage_pct']}%")
            bp3.metric("Overcharging",        f"{pb['overcharging_pct']}%",
                       delta=f"+₦{pb['total_overcharge_amount']:,.0f} total",
                       delta_color="inverse")
            bp4.metric("Undercharging",       f"{pb['undercharging_pct']}%",
                       delta=f"-₦{pb['total_undercharge_amount']:,.0f} total",
                       delta_color="normal")

            if rb.get("fraud_risk"):
                fc = FRAUD_COLORS.get(rb["fraud_risk"], "#bdc3c7")
                st.markdown(
                    f'<div style="background:{fc};color:white;padding:0.8rem;border-radius:8px;margin-top:1rem">'
                    f'🚨 Fraud Risk (reality prices vs P90/P95): <strong>{rb["fraud_risk"]}</strong></div>',
                    unsafe_allow_html=True,
                )

    # ---- Tab 3: Raw JSON -------------------------------------------------------
    with tabs[2]:
        st.code(json.dumps(data, indent=2), language="json")