"""
COMPLETE UI TABS - ALL 8 TABS
==============================

This file contains all Streamlit UI tab implementations.
Copy these functions into your contract_analyzer_complete_hybrid.py

All 8 tabs:
1. 🤖 AI Executive Summary
2. 🔮 AI Predictions  
3. 💰 Financial Analysis
4. 📊 Utilization Analysis
5. 🏥 Conditions Breakdown
6. 🏥 Provider Analysis + Banding
7. 🔍 Anomaly Detection
8. 💼 Negotiation Strategy

INTEGRATION: Copy all functions from this file into contract_analyzer_complete_hybrid.py
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime


def render_ai_executive_summary(analysis_data: dict, ai_summary, cost_tracking: dict):
    """Tab 1: AI Executive Summary with Brutal Truth"""

    st.markdown("# 🤖 AI Executive Summary")

    # Handle both string and dict formats
    if isinstance(ai_summary, str):
        summary_text = ai_summary
    elif isinstance(ai_summary, dict):
        if 'error' in ai_summary:
            st.error(f"❌ Failed to generate AI summary: {ai_summary.get('error', 'Unknown error')}")
            return
        summary_text = ai_summary.get('summary', 'No summary available')
    elif not ai_summary:
        st.error("❌ Failed to generate AI summary: No data available")
        return
    else:
        st.error("❌ Invalid summary format")
        return

    # Cost indicator
    cost = cost_tracking.get('summary', 0) if cost_tracking else 0
    st.info(f"💰 AI Cost for this section: ₦{cost:.2f}")

    # Display the AI-generated summary (it's already formatted in markdown)
    st.markdown(summary_text)

    # Add a divider
    st.markdown("---")

    # Quick action buttons
    st.markdown("### ⚡ Quick Actions")
    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("📧 Email to Management", key="email_summary"):
            st.info("Email feature coming in Phase 2")

    with col2:
        if st.button("💾 Export Summary", key="export_summary"):
            # Create downloadable text file
            st.download_button(
                "Download Summary",
                summary_text,
                f"executive_summary_{analysis_data['company_name']}.md",
                "text/markdown"
            )

    with col3:
        if st.button("🔄 Regenerate AI Summary", key="regen_summary"):
            st.info("Clear cache and rerun to regenerate")


def render_ai_predictions(analysis_data: dict, ai_predictions: dict, cost_tracking: dict):
    """Tab 2: AI Future Predictions"""
    
    st.markdown("# 🔮 AI Future Predictions")
    
    if not ai_predictions or 'error' in ai_predictions:
        st.error(f"❌ Failed to generate predictions: {ai_predictions.get('error', 'Unknown error')}")
        return
    
    # Cost indicator
    cost = cost_tracking.get('predictions_cost', 0)
    st.info(f"💰 AI Cost for this section: ₦{cost:.2f}")
    
    # Main prediction
    st.markdown("## 📈 End-of-Contract MLR Projection")
    
    predicted_mlr = ai_predictions.get('predicted_mlr', 0)
    current_mlr = analysis_data['mlr'].get('bv_mlr', 0)
    confidence = ai_predictions.get('confidence', 'Unknown')
    
    # Big metric display
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(
            "Current MLR",
            f"{current_mlr:.1f}%" if current_mlr else "N/A",
            delta=None
        )
    
    with col2:
        delta_mlr = predicted_mlr - current_mlr if current_mlr else 0
        st.metric(
            "Projected MLR",
            f"{predicted_mlr:.1f}%",
            delta=f"{delta_mlr:+.1f}%",
            delta_color="inverse"
        )
    
    with col3:
        st.metric(
            "Confidence Level",
            confidence,
            delta=None
        )
    
    # Visualization
    st.markdown("### 📊 MLR Trajectory")
    
    fig = go.Figure()
    
    # Current MLR bar
    fig.add_trace(go.Bar(
        x=['Current MLR'],
        y=[current_mlr],
        name='Current',
        marker_color='lightblue'
    ))
    
    # Projected MLR bar
    fig.add_trace(go.Bar(
        x=['Projected MLR'],
        y=[predicted_mlr],
        name='Projected',
        marker_color='red' if predicted_mlr > 85 else 'orange' if predicted_mlr > 75 else 'green'
    ))
    
    # Target line
    fig.add_hline(y=75, line_dash="dash", line_color="green", annotation_text="Target 75%")
    
    fig.update_layout(
        title="MLR Projection",
        yaxis_title="MLR %",
        showlegend=False,
        height=400
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Reasoning
    st.markdown("### 🧠 AI Reasoning")
    st.markdown(ai_predictions.get('reasoning', 'No reasoning provided'))
    
    # Key assumptions
    st.markdown("### 📋 Key Assumptions")
    assumptions = ai_predictions.get('assumptions', [])
    if assumptions:
        for assumption in assumptions:
            st.markdown(f"- {assumption}")
    else:
        st.info("No specific assumptions provided")
    
    # Risk factors
    st.markdown("### ⚠️ Risk Factors")
    risk_factors = ai_predictions.get('risk_factors', [])
    if risk_factors:
        for risk in risk_factors:
            st.markdown(f"- 🚨 {risk}")
    else:
        st.success("✅ No major risk factors identified")


def render_financial_analysis(analysis_data: dict):
    """Tab 3: Financial Analysis"""
    
    st.markdown("# 💰 Financial Analysis")
    
    financial = analysis_data['financial']
    mlr = analysis_data['mlr']
    contract = analysis_data['contract']
    
    # Key metrics
    st.markdown("## 📊 Key Financial Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total Debit",
            f"₦{financial['debit']:,.0f}"
        )
    
    with col2:
        st.metric(
            "Cash Received",
            f"₦{financial['cash']:,.0f}",
            delta=f"{financial['payment_rate']:.1f}% paid"
        )
    
    with col3:
        st.metric(
            "Outstanding",
            f"₦{financial['outstanding']:,.0f}",
            delta=f"-₦{financial['outstanding']:,.0f}",
            delta_color="inverse"
        )
    
    with col4:
        st.metric(
            "Commission",
            f"₦{financial['commission']:,.0f}",
            delta=f"{financial['commission_rate']*100:.0f}%"
        )
    
    # MLR metrics
    st.markdown("## 🎯 MLR Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        bv_mlr = mlr.get('bv_mlr')
        if bv_mlr:
            st.metric(
                "BV-MLR",
                f"{bv_mlr:.1f}%",
                delta=f"{bv_mlr - mlr['target_mlr']:+.1f}%",
                delta_color="inverse"
            )
        else:
            st.metric("BV-MLR", "N/A")
    
    with col2:
        cash_mlr = mlr.get('cash_mlr')
        if cash_mlr:
            st.metric(
                "CASH-MLR",
                f"{cash_mlr:.1f}%",
                delta=f"{cash_mlr - mlr['target_mlr']:+.1f}%",
                delta_color="inverse"
            )
        else:
            st.metric("CASH-MLR", "N/A")
    
    with col3:
        st.metric(
            "PMPM",
            f"₦{mlr['pmpm']:,.0f}",
            delta=f"{mlr['pmpm_variance']:+.1f}%",
            delta_color="inverse"
        )
    
    with col4:
        st.metric(
            "Monthly Burn",
            f"₦{mlr['monthly_burn']:,.0f}",
            delta=f"{contract['months_elapsed']} months"
        )
    
    # Financial breakdown chart
    st.markdown("## 📈 Cost Breakdown")
    
    breakdown_data = {
        'Category': ['Claims Paid', 'Unclaimed PA', 'Commission'],
        'Amount': [
            mlr['claims_cost'],
            mlr['unclaimed_pa'],
            mlr['commission']
        ]
    }
    
    fig = px.pie(
        breakdown_data,
        values='Amount',
        names='Category',
        title='Medical Cost + Commission Breakdown',
        color_discrete_sequence=['#FF6B6B', '#4ECDC4', '#45B7D1']
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Payment analysis
    st.markdown("## 💳 Payment Analysis")
    
    payment_rate = financial['payment_rate']
    
    if payment_rate < 10:
        st.error("🔴 CRITICAL: Near-zero payment - immediate escalation required")
    elif payment_rate < 50:
        st.warning("🟡 WARNING: Poor payment rate - requires urgent attention")
    elif payment_rate < 80:
        st.info("⚠️ CAUTION: Below target payment rate")
    else:
        st.success("✅ GOOD: Payment rate above target")
    
    # Payment breakdown
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=payment_rate,
        title={'text': "Payment Rate"},
        gauge={
            'axis': {'range': [0, 100]},
            'bar': {'color': "darkblue"},
            'steps': [
                {'range': [0, 50], 'color': "red"},
                {'range': [50, 80], 'color': "yellow"},
                {'range': [80, 100], 'color': "green"}
            ],
            'threshold': {
                'line': {'color': "black", 'width': 4},
                'thickness': 0.75,
                'value': 80
            }
        }
    ))
    
    fig.update_layout(height=300)
    st.plotly_chart(fig, use_container_width=True)


def render_utilization_analysis(analysis_data: dict):
    """Tab 4: Utilization Analysis"""
    
    st.markdown("# 📊 Utilization Analysis")
    
    claims = analysis_data['claims']
    enrollment = analysis_data['enrollment']
    concentration = analysis_data['concentration']
    contract = analysis_data['contract']
    
    # Overview metrics
    st.markdown("## 🎯 Utilization Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total Claims",
            f"{claims['total_claims']:,}"
        )
    
    with col2:
        st.metric(
            "Unique Claimants",
            f"{claims['unique_claimed']:,}",
            delta=f"{enrollment['total']:,} enrolled"
        )
    
    with col3:
        utilization_rate = (claims['unique_claimed'] / enrollment['total'] * 100) if enrollment['total'] > 0 else 0
        st.metric(
            "Utilization Rate",
            f"{utilization_rate:.1f}%"
        )
    
    with col4:
        st.metric(
            "Avg Claim Size",
            f"₦{claims['avg_cost']:,.0f}"
        )
    
    # Member concentration
    st.markdown("## 👥 Member Concentration Analysis")
    
    conc_type = concentration.get('type', 'UNKNOWN')
    top_5_pct = concentration.get('top_5_pct', 0)
    top_10_pct = concentration.get('top_10_pct', 0)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown(f"### Pattern Type: **{conc_type}**")
        
        if conc_type == "EPISODIC":
            st.success("✅ EPISODIC: Concentrated in few members (fixable)")
            st.markdown("""
            **What this means:**
            - High costs driven by a few individuals
            - Likely one-off events (surgeries, deliveries)
            - **Strategy:** Keep premium, implement case management
            """)
        elif conc_type == "STRUCTURAL":
            st.error("🔴 STRUCTURAL: Widespread utilization (systemic)")
            st.markdown("""
            **What this means:**
            - Costs spread across many members
            - Likely chronic conditions or poor health habits
            - **Strategy:** Premium increase required
            """)
    
    with col2:
        # Concentration pie chart
        conc_data = {
            'Group': ['Top 5 Members', 'Top 6-10 Members', 'Rest of Population'],
            'Percentage': [
                top_5_pct,
                top_10_pct - top_5_pct,
                100 - top_10_pct
            ]
        }
        
        fig = px.pie(
            conc_data,
            values='Percentage',
            names='Group',
            title='Cost Concentration',
            color_discrete_sequence=['#FF6B6B', '#FFA500', '#4ECDC4']
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    # Top 10 members table
    st.markdown("## 🔝 Top 10 High-Cost Members")
    
    top_members = concentration.get('top_members', [])
    
    if top_members:
        df = pd.DataFrame(top_members)
        df.index = range(1, len(df) + 1)
        
        # Format currency
        df['total_cost'] = df['total_cost'].apply(lambda x: f"₦{x:,.0f}")
        df['pct_of_total'] = df['pct_of_total'].apply(lambda x: f"{x:.1f}%")
        
        st.dataframe(df, use_container_width=True)
    else:
        st.info("No member data available")


def render_conditions_breakdown(analysis_data: dict):
    """Tab 5: Conditions Breakdown"""
    
    st.markdown("# 🏥 Conditions Analysis")
    
    conditions = analysis_data['conditions']
    
    # Key percentages
    st.markdown("## 📊 Condition Category Percentages")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        mat_pct = conditions.get('maternity_pct', 0)
        if mat_pct > 25:
            st.metric("Maternity", f"{mat_pct:.1f}%", delta="🔴 High", delta_color="off")
        elif mat_pct > 20:
            st.metric("Maternity", f"{mat_pct:.1f}%", delta="⚠️ Watch", delta_color="off")
        else:
            st.metric("Maternity", f"{mat_pct:.1f}%", delta="✅ Normal", delta_color="off")
    
    with col2:
        chronic_pct = conditions.get('chronic_pct', 0)
        if chronic_pct > 30:
            st.metric("Chronic", f"{chronic_pct:.1f}%", delta="🔴 High", delta_color="off")
        elif chronic_pct > 20:
            st.metric("Chronic", f"{chronic_pct:.1f}%", delta="⚠️ Watch", delta_color="off")
        else:
            st.metric("Chronic", f"{chronic_pct:.1f}%", delta="✅ Normal", delta_color="off")
    
    with col3:
        prev_pct = conditions.get('preventable_pct', 0)
        if prev_pct > 40:
            st.metric("Preventable", f"{prev_pct:.1f}%", delta="🔴 High", delta_color="off")
        elif prev_pct > 25:
            st.metric("Preventable", f"{prev_pct:.1f}%", delta="⚠️ Watch", delta_color="off")
        else:
            st.metric("Preventable", f"{prev_pct:.1f}%", delta="✅ Normal", delta_color="off")
    
    with col4:
        cat_pct = conditions.get('catastrophic_pct', 0)
        if cat_pct > 15:
            st.metric("Catastrophic", f"{cat_pct:.1f}%", delta="🔴 High", delta_color="off")
        elif cat_pct > 10:
            st.metric("Catastrophic", f"{cat_pct:.1f}%", delta="⚠️ Watch", delta_color="off")
        else:
            st.metric("Catastrophic", f"{cat_pct:.1f}%", delta="✅ Normal", delta_color="off")
    
    # Detailed breakdown
    st.markdown("## 📋 Detailed Condition Breakdown")
    
    categories = conditions.get('categories', [])
    
    if categories:
        df = pd.DataFrame(categories)
        
        # Sort by cost
        df = df.sort_values('total_cost', ascending=False)
        
        # Format
        df['total_cost'] = df['total_cost'].apply(lambda x: f"₦{x:,.0f}")
        df['pct'] = df['pct'].apply(lambda x: f"{x:.1f}%")
        
        st.dataframe(df, use_container_width=True)
        
        # Visualization
        st.markdown("## 📊 Cost Distribution by Condition")
        
        # Prepare data for chart
        chart_df = pd.DataFrame(categories)
        chart_df = chart_df.sort_values('total_cost', ascending=False).head(10)
        
        fig = px.bar(
            chart_df,
            x='category',
            y='total_cost',
            title='Top 10 Conditions by Cost',
            labels={'total_cost': 'Total Cost (₦)', 'category': 'Condition Category'},
            color='total_cost',
            color_continuous_scale='Reds'
        )
        
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No condition data available")
    
    # Strategic recommendations
    st.markdown("## 💡 Strategic Recommendations")
    
    if prev_pct > 40:
        st.warning("""
        🟡 **HIGH PREVENTABLE CONDITIONS**
        
        - Consider wellness programs (malaria prevention: ₦2,000 vs ₦35,000 treatment = 94% savings)
        - Implement health education campaigns
        - Partner with preventive care providers
        - ROI: Typically 4-6 month payback period
        """)
    
    if chronic_pct > 30:
        st.error("""
        🔴 **HIGH CHRONIC DISEASE BURDEN**
        
        - This population requires ongoing care (not one-off)
        - Chronic costs will persist and likely increase
        - Premium adjustment REQUIRED to maintain profitability
        - Consider disease management programs as part of renewal
        """)
    
    if mat_pct > 25:
        st.warning("""
        🟡 **ELEVATED MATERNITY COSTS**
        
        - Normal range: 15-20%
        - Above 25% suggests adverse selection
        - Consider maternity limits or separate pricing
        - Review enrollment demographics
        """)


def render_provider_analysis(analysis_data: dict, banding_results: dict = None):
    """Tab 6: Provider Analysis + Hospital Banding"""
    
    st.markdown("# 🏥 Provider Analysis & Banding")
    
    providers = analysis_data['providers']
    
    # Top providers
    st.markdown("## 🔝 Top 10 Providers by Cost")
    
    top_providers = providers.get('top_providers', [])
    
    if top_providers:
        df = pd.DataFrame(top_providers)
        df.index = range(1, len(df) + 1)

        # Format
        df['total_cost'] = df['total_cost'].apply(lambda x: f"₦{x:,.0f}")
        df['pct'] = df['pct'].apply(lambda x: f"{x:.1f}%")

        # Add emoji indicators based on percentage
        df['Status'] = df['pct'].apply(lambda x:
            "🔴" if float(x.strip('%')) > 30 else
            "🟡" if float(x.strip('%')) > 20 else "✅"
        )

        # Select columns to display (including band if available)
        display_columns = ['providername', 'total_cost', 'pct', 'Status']

        # Check if we have band data from provider_bands analysis
        provider_bands = analysis_data.get('provider_bands', {})
        if provider_bands.get('success') and provider_bands.get('providers_with_bands'):
            # Create a mapping of provider names to bands
            band_mapping = {
                p['providername']: p['band']
                for p in provider_bands['providers_with_bands']
            }

            # Add band column to dataframe
            df['Band'] = df['providername'].map(band_mapping).fillna('UNKNOWN')

            # Insert Band column before Status
            display_columns = ['providername', 'Band', 'total_cost', 'pct', 'Status']

        st.dataframe(
            df[display_columns].rename(columns={
                'providername': 'Provider',
                'total_cost': 'Total Cost',
                'pct': '% of Total'
            }),
            use_container_width=True
        )
    else:
        st.info("No provider data available")

    # === PROVIDER BAND DISTRIBUTION ===
    st.markdown("---")
    st.markdown("## 🎯 Provider Band Distribution")

    provider_bands = analysis_data.get('provider_bands', {})

    if provider_bands.get('success'):
        band_dist = provider_bands.get('band_distribution', {})

        if band_dist:
            # Summary metrics
            col1, col2, col3 = st.columns(3)

            with col1:
                st.metric("Total Hospitals", provider_bands.get('total_providers', 0))

            with col2:
                st.metric("Dominant Band", provider_bands.get('dominant_band', 'N/A'))

            with col3:
                # Calculate high-band percentage (Band A + Special)
                high_band_pct = (
                    band_dist.get('a', {}).get('pct', 0) +
                    band_dist.get('special', {}).get('pct', 0)
                )
                st.metric("High-Band Usage", f"{high_band_pct:.1f}%")

            # Band distribution table
            st.markdown("### Band Breakdown")

            band_summary = []
            for band_key, band_data in band_dist.items():
                band_summary.append({
                    'Band': band_key.upper(),
                    'Hospital Count': band_data.get('count', 0),
                    'Total Cost': f"₦{band_data.get('total_cost', 0):,.0f}",
                    '% of Total': f"{band_data.get('pct', 0):.1f}%"
                })

            if band_summary:
                # Sort by cost percentage (descending)
                band_df = pd.DataFrame(band_summary)
                band_df = band_df.sort_values(
                    by='% of Total',
                    key=lambda x: x.str.rstrip('%').astype(float),
                    ascending=False
                )

                st.dataframe(band_df, use_container_width=True)

                # Pie chart visualization
                fig = px.pie(
                    band_df,
                    names='Band',
                    values=[int(row['Hospital Count']) for _, row in band_df.iterrows()],
                    title='Hospital Count by Band',
                    hole=0.4
                )
                st.plotly_chart(fig, use_container_width=True)

                # Strategic insights based on band mix
                st.markdown("### 💡 Band Mix Insights")

                # Check for high-band concentration
                if high_band_pct > 50:
                    st.error(f"""
                    🔴 **HIGH PREMIUM BAND CONCENTRATION**

                    - {high_band_pct:.1f}% of costs at Band A/Special (most expensive hospitals)
                    - This drives up premium requirements significantly
                    - **Action:** Shift to Band B/C providers for 20-40% cost savings
                    """)
                elif high_band_pct > 30:
                    st.warning(f"""
                    🟡 **ELEVATED PREMIUM BAND USAGE**

                    - {high_band_pct:.1f}% at Band A/Special providers
                    - Consider encouraging Band B/C alternatives
                    - Potential savings: 15-25%
                    """)
                else:
                    st.success("""
                    ✅ **COST-EFFECTIVE BAND MIX**

                    - Good distribution across provider bands
                    - Appropriate use of premium facilities
                    """)
            else:
                st.info("No band distribution data available")
        else:
            st.info("Band distribution data not available")
    else:
        error_msg = provider_bands.get('error', 'Unknown error')
        st.warning(f"⚠️ Provider band analysis unavailable: {error_msg}")

    # Hospital Banding Results
    st.markdown("---")
    st.markdown("## 🏥 Hospital Band Analysis")
    
    if banding_results and banding_results.get('success'):
        st.success("✅ Hospital banding analysis complete")
        
        banding_data = banding_results.get('results', [])
        
        if banding_data:
            st.markdown("### 🎯 Band Assignments & Overcharging")
            
            for result in banding_data:
                provider_name = result.get('provider_name', 'Unknown')
                current_band = result.get('assigned_band', 'N/A')
                should_be_band = result.get('correct_band', 'N/A')
                overcharge_pct = result.get('overcharge_pct', 0)
                annual_impact = result.get('annual_impact', 0)
                
                with st.expander(f"🏥 {provider_name} - Band {current_band}"):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.markdown(f"""
                        **Current Band:** {current_band}
                        **Should Be:** {should_be_band}
                        **Overcharging:** {overcharge_pct:.1f}%
                        """)
                    
                    with col2:
                        st.markdown(f"""
                        **Annual Impact:** ₦{annual_impact:,.0f}
                        **Status:** {"🔴 ACTION REQUIRED" if overcharge_pct > 20 else "✅ Acceptable"}
                        """)
                    
                    if overcharge_pct > 20:
                        st.error(f"""
                        **🚨 CRITICAL OVERCHARGING**
                        
                        This provider is charging {overcharge_pct:.1f}% above their band.
                        
                        **Recommended Actions:**
                        1. Demand immediate band reclassification
                        2. Retroactive adjustment for past 6 months
                        3. If non-compliant, remove from network
                        4. Potential recovery: ₦{annual_impact/2:,.0f} (6 months)
                        """)
        else:
            st.info("No banding analysis data available")
    elif banding_results and not banding_results.get('success'):
        st.warning(f"⚠️ Banding analysis incomplete: {banding_results.get('error', 'Unknown error')}")
    else:
        st.info("💡 Hospital banding analysis not performed. Click 'Run Banding Analysis' to check provider tariff compliance.")


def render_anomaly_detection(analysis_data: dict, ai_anomalies, cost_tracking: dict):
    """Tab 7: Anomaly Detection"""

    st.markdown("# 🔍 Anomaly Detection & Fraud Analysis")

    fraud = analysis_data['fraud']

    # Cost indicator
    if cost_tracking:
        cost = cost_tracking.get('anomalies', 0)
        st.info(f"💰 AI Cost for this section: ₦{cost:.2f}")

    # Fraud risk level
    st.markdown("## 🚨 Fraud Risk Assessment")

    risk_level = fraud.get('risk_level', 'UNKNOWN')
    unknown_pct = fraud.get('unknown_pct', 0)
    unknown_amount = fraud.get('unknown_amount', 0)

    if risk_level == "HIGH RISK":
        st.error(f"🔴 **HIGH RISK**: {unknown_pct:.1f}% unknown providers (₦{unknown_amount:,.0f})")
        st.markdown("""
        **CRITICAL ACTIONS REQUIRED:**
        - Immediate audit of all unknown provider claims
        - Suspend payments to unknown providers
        - Demand provider verification within 7 days
        - Consider termination if >30% cannot be verified
        """)
    elif risk_level == "MEDIUM RISK":
        st.warning(f"🟡 **MEDIUM RISK**: {unknown_pct:.1f}% unknown providers (₦{unknown_amount:,.0f})")
        st.markdown("""
        **RECOMMENDED ACTIONS:**
        - Schedule provider verification audit
        - Implement stricter provider onboarding
        - Monthly monitoring required
        """)
    else:
        st.success(f"✅ **LOW RISK**: {unknown_pct:.1f}% unknown providers (₦{unknown_amount:,.0f})")

    # Same-day claims
    st.markdown("## ⚡ Same-Day Multiple Claims (Medical Impossibilities)")

    same_day_count = fraud.get('same_day_count', 0)
    same_day_instances = fraud.get('same_day_instances', [])

    if same_day_count > 0:
        st.error(f"🔴 Found {same_day_count} instances of 5+ claims on same day")

        if same_day_instances:
            df = pd.DataFrame(same_day_instances)
            df = df.sort_values('claims_same_day', ascending=False)
            st.dataframe(df, use_container_width=True)

            st.markdown("""
            **⚠️ These patterns are medically impossible and indicate potential fraud:**
            - Multiple major procedures same day
            - Provider claiming for non-existent visits
            - Patient identity theft

            **ACTION:** Investigate each instance immediately
            """)
    else:
        st.success("✅ No same-day suspicious patterns detected")

    # AI-detected anomalies
    # Handle both string and dict formats
    if isinstance(ai_anomalies, str):
        st.markdown("---")
        st.markdown("## 🤖 AI-Detected Anomalies")
        st.markdown(ai_anomalies)
    elif isinstance(ai_anomalies, dict):
        if 'error' in ai_anomalies:
            st.warning(f"⚠️ AI anomaly detection failed: {ai_anomalies.get('error')}")
        else:
            st.markdown("---")
            st.markdown("## 🤖 AI-Detected Anomalies")
            st.markdown(ai_anomalies.get('analysis', 'No anomalies detected'))
    elif ai_anomalies:
        st.warning("⚠️ Invalid anomaly data format")


def render_negotiation_strategy(analysis_data: dict, ai_strategy, cost_tracking: dict):
    """Tab 8: Negotiation Strategy"""

    st.markdown("# 💼 Negotiation Strategy & Playbook")

    # Handle both string and dict formats
    if isinstance(ai_strategy, str):
        strategy_text = ai_strategy
    elif isinstance(ai_strategy, dict):
        if 'error' in ai_strategy:
            st.error(f"❌ Failed to generate strategy: {ai_strategy.get('error', 'Unknown error')}")
            return
        strategy_text = ai_strategy.get('strategy', 'No strategy available')
    elif not ai_strategy:
        st.error("❌ Failed to generate strategy: No data available")
        return
    else:
        st.error("❌ Invalid strategy format")
        return

    # Cost indicator
    cost = cost_tracking.get('strategy', 0) if cost_tracking else 0
    st.info(f"💰 AI Cost for this section: ₦{cost:.2f}")

    # Display AI-generated strategy
    st.markdown(strategy_text)
    
    # Quick reference checklist
    st.markdown("---")
    st.markdown("## ✅ Pre-Meeting Checklist")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        **Documents to Prepare:**
        - [ ] This complete analysis report
        - [ ] Claims breakdown by provider
        - [ ] Top 10 high-cost members (anonymized)
        - [ ] Condition breakdown charts
        - [ ] Payment history
        - [ ] Comparison to similar companies
        """)
    
    with col2:
        st.markdown("""
        **Meeting Strategy:**
        - [ ] Start with appreciation
        - [ ] Present data objectively
        - [ ] Focus on partnership
        - [ ] Offer solutions, not just problems
        - [ ] Be prepared to walk away if needed
        - [ ] Document everything
        """)
    
    # Export negotiation package
    st.markdown("---")
    if st.button("📦 Export Complete Negotiation Package"):
        st.info("Export feature coming in Phase 2 - will include PDF report + Excel data")


def render_risk_dashboard(analysis_data: dict, risk_score: dict):
    """Tab 9: Comprehensive Risk Score Dashboard"""

    st.markdown("# ⚠️ Comprehensive Renewal Risk Assessment")

    if not risk_score or not risk_score.get('success'):
        st.warning(f"⚠️ Risk scoring unavailable: {risk_score.get('error', 'No data')}")
        return

    total_score = risk_score.get('total_risk_score', 50)
    risk_category = risk_score.get('risk_category', 'UNKNOWN')
    renewal_action = risk_score.get('renewal_action', 'INSUFFICIENT_DATA')
    premium_change = risk_score.get('recommended_premium_change', 0)
    component_scores = risk_score.get('component_scores', {})

    # Risk Score Gauge
    st.markdown("## 🎯 Overall Risk Score")

    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=total_score,
        title={'text': "Risk Score (0-100)"},
        delta={'reference': 30, 'increasing': {'color': "red"}, 'decreasing': {'color': "green"}},
        gauge={
            'axis': {'range': [0, 100]},
            'bar': {'color': "darkblue"},
            'steps': [
                {'range': [0, 30], 'color': "lightgreen"},
                {'range': [30, 60], 'color': "yellow"},
                {'range': [60, 80], 'color': "orange"},
                {'range': [80, 100], 'color': "red"}
            ],
            'threshold': {
                'line': {'color': "black", 'width': 4},
                'thickness': 0.75,
                'value': total_score
            }
        }
    ))

    fig.update_layout(height=300)
    st.plotly_chart(fig, use_container_width=True)

    # Traffic Light System
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Risk Category", risk_category, delta=None)

    with col2:
        st.metric("Recommended Action", renewal_action, delta=None)

    with col3:
        st.metric("Premium Adjustment", f"+{premium_change:.1f}%", delta=None)

    # Color-coded alert box
    if total_score <= 30:
        st.success("""
        ✅ **LOW RISK (0-30)** - Renew at current terms

        This contract is performing well. Minor adjustments (0-5% increase) may be appropriate for inflation.
        """)
    elif total_score <= 60:
        st.info("""
        🟡 **MEDIUM RISK (31-60)** - Adjustments needed

        This contract requires moderate changes. Recommend 5-15% premium increase with enhanced monitoring.
        """)
    elif total_score <= 80:
        st.warning("""
        🟠 **HIGH RISK (61-80)** - Major changes required

        This contract needs significant restructuring. Recommend 15-25% premium increase plus care management programs.
        """)
    else:
        st.error("""
        🔴 **EXTREME RISK (81-100)** - Consider termination

        This contract is at extreme risk. Recommend non-renewal or >25% premium increase with comprehensive intervention.
        """)

    # Component Breakdown
    st.markdown("---")
    st.markdown("## 📊 Risk Component Breakdown")

    if component_scores:
        # Create DataFrame for bar chart
        components_df = pd.DataFrame({
            'Component': ['MLR Risk\n(30%)', 'HCC Persistence\n(25%)', 'Chronic Burden\n(20%)', 'Collection Risk\n(15%)', 'Claims Trend\n(10%)'],
            'Score': [
                component_scores.get('mlr_risk', 0),
                component_scores.get('hcc_persistence', 0),
                component_scores.get('chronic_burden', 0),
                component_scores.get('collection_risk', 0),
                component_scores.get('trend_risk', 0)
            ],
            'Weight': [30, 25, 20, 15, 10]
        })

        # Bar chart
        fig = go.Figure()

        fig.add_trace(go.Bar(
            x=components_df['Component'],
            y=components_df['Score'],
            text=components_df['Score'].apply(lambda x: f"{x:.0f}"),
            textposition='outside',
            marker_color=['#FF6B6B' if x > 60 else '#FFA500' if x > 30 else '#4ECDC4' for x in components_df['Score']]
        ))

        fig.update_layout(
            title="Risk Component Scores (0-100 scale)",
            yaxis_title="Score",
            xaxis_title="Component (Weight %)",
            height=400,
            yaxis=dict(range=[0, 100])
        )

        st.plotly_chart(fig, use_container_width=True)

        # Component details table
        st.markdown("### Component Details")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**MLR Risk (30% weight)**")
            mlr_score = component_scores.get('mlr_risk', 0)
            if mlr_score > 60:
                st.error(f"🔴 Score: {mlr_score} - MLR significantly above target")
            elif mlr_score > 30:
                st.warning(f"🟡 Score: {mlr_score} - MLR moderately elevated")
            else:
                st.success(f"✅ Score: {mlr_score} - MLR within acceptable range")

            st.markdown("**Chronic Disease Burden (20% weight)**")
            chronic_score = component_scores.get('chronic_burden', 0)
            if chronic_score > 60:
                st.error(f"🔴 Score: {chronic_score} - High chronic disease prevalence")
            elif chronic_score > 30:
                st.warning(f"🟡 Score: {chronic_score} - Moderate chronic conditions")
            else:
                st.success(f"✅ Score: {chronic_score} - Low chronic burden")

            st.markdown("**Claims Trend (10% weight)**")
            trend_score = component_scores.get('trend_risk', 0)
            if trend_score > 60:
                st.error(f"🔴 Score: {trend_score} - Rapidly increasing costs")
            elif trend_score > 30:
                st.warning(f"🟡 Score: {trend_score} - Moderate cost growth")
            else:
                st.success(f"✅ Score: {trend_score} - Stable cost trend")

        with col2:
            st.markdown("**HCC Persistence (25% weight)**")
            hcc_score = component_scores.get('hcc_persistence', 0)
            if hcc_score > 60:
                st.error(f"🔴 Score: {hcc_score} - Structural pattern (persistent conditions)")
            elif hcc_score > 30:
                st.warning(f"🟡 Score: {hcc_score} - Mixed episodic/chronic pattern")
            else:
                st.success(f"✅ Score: {hcc_score} - Episodic pattern (fixable)")

            st.markdown("**Collection Risk (15% weight)**")
            collection_score = component_scores.get('collection_risk', 0)
            if collection_score > 60:
                st.error(f"🔴 Score: {collection_score} - Poor payment history")
            elif collection_score > 30:
                st.warning(f"🟡 Score: {collection_score} - Moderate collection delays")
            else:
                st.success(f"✅ Score: {collection_score} - Good payment compliance")

    # Recommended Actions Table
    st.markdown("---")
    st.markdown("## 🎯 Specific Recommended Actions")

    actions = []

    # Add actions based on component scores
    if component_scores.get('mlr_risk', 0) > 60:
        actions.append({
            'Issue': 'High MLR',
            'Severity': '🔴 Critical',
            'Action': 'Immediate premium increase 15-25%',
            'Timeline': 'Next renewal'
        })

    if component_scores.get('hcc_persistence', 0) > 60:
        actions.append({
            'Issue': 'Structural Pattern',
            'Severity': '🔴 Critical',
            'Action': 'Implement chronic disease management program',
            'Timeline': '30 days'
        })

    if component_scores.get('chronic_burden', 0) > 60:
        actions.append({
            'Issue': 'High Chronic Disease',
            'Severity': '🟠 High',
            'Action': 'Partner with chronic care providers, wellness programs',
            'Timeline': '60 days'
        })

    if component_scores.get('collection_risk', 0) > 60:
        actions.append({
            'Issue': 'Poor Payment',
            'Severity': '🔴 Critical',
            'Action': 'Restructure payment terms or require prepayment',
            'Timeline': 'Immediate'
        })

    if component_scores.get('trend_risk', 0) > 60:
        actions.append({
            'Issue': 'Rising Costs',
            'Severity': '🟠 High',
            'Action': 'Provider renegotiation + utilization management',
            'Timeline': '90 days'
        })

    # Check for gaming patterns
    monthly_pmpm = analysis_data.get('monthly_pmpm', {})
    if monthly_pmpm.get('gaming_risk') == 'HIGH':
        actions.append({
            'Issue': 'Gaming Pattern Detected',
            'Severity': '🟠 High',
            'Action': 'Base renewal on first 10 months only, exclude final spike',
            'Timeline': 'Next renewal calculation'
        })

    # Check PA effectiveness
    pa_effectiveness = analysis_data.get('pa', {}).get('pa_effectiveness', {})
    if pa_effectiveness.get('effectiveness_score') == 'POOR':
        actions.append({
            'Issue': 'Ineffective PA System',
            'Severity': '🟡 Medium',
            'Action': 'Tighten PA approval criteria, improve gatekeeping',
            'Timeline': '60 days'
        })

    if actions:
        actions_df = pd.DataFrame(actions)
        st.dataframe(actions_df, use_container_width=True, hide_index=True)
    else:
        st.success("✅ No critical actions required - contract performing within acceptable parameters")

    # Summary recommendation
    st.markdown("---")
    st.markdown("## 📋 Executive Summary")

    st.markdown(f"""
    **Overall Assessment:** {risk_category} ({total_score}/100)

    **Renewal Recommendation:** {renewal_action}

    **Premium Adjustment:** +{premium_change:.1f}%

    **Key Drivers:**
    - MLR Risk contributing {component_scores.get('mlr_risk', 0) * 0.30:.1f} points (30% weight)
    - HCC Persistence contributing {component_scores.get('hcc_persistence', 0) * 0.25:.1f} points (25% weight)
    - Chronic Burden contributing {component_scores.get('chronic_burden', 0) * 0.20:.1f} points (20% weight)
    - Collection Risk contributing {component_scores.get('collection_risk', 0) * 0.15:.1f} points (15% weight)
    - Trend Risk contributing {component_scores.get('trend_risk', 0) * 0.10:.1f} points (10% weight)

    **Bottom Line:** {'PROCEED WITH RENEWAL' if total_score < 60 else 'MAJOR CHANGES REQUIRED' if total_score < 80 else 'CONSIDER TERMINATION'}
    """)


def render_plan_analysis(analysis_data: dict):
    """Tab 7: Plan-Level Enrollment vs Utilization Analysis"""

    st.markdown("# 📋 Plan Distribution Analysis")

    plan_analysis = analysis_data.get('plan_analysis', {})

    if not plan_analysis.get('success'):
        st.warning(f"⚠️ Plan analysis unavailable: {plan_analysis.get('error', 'No data')}")
        return

    # Summary metrics
    st.markdown("## 📊 Overall Plan Portfolio")

    total_plans = plan_analysis.get('total_plans', 0)
    total_enrolled = plan_analysis.get('total_enrolled', 0)
    total_claims_cost = plan_analysis.get('total_claims_cost', 0)
    total_pa_cost = plan_analysis.get('total_pa_cost', 0)
    total_cost = plan_analysis.get('total_cost', 0)
    terminated_cost = plan_analysis.get('terminated_cost', 0)
    terminated_enrollees = plan_analysis.get('terminated_enrollees', 0)

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Plans", total_plans)
    with col2:
        st.metric("Total Enrolled", f"{total_enrolled:,}")
    with col3:
        st.metric("Claims Cost", f"₦{total_claims_cost:,.0f}")
    with col4:
        st.metric("PA Cost", f"₦{total_pa_cost:,.0f}")

    # Second row with total cost and terminated cost
    col5, col6, col7, col8 = st.columns(4)
    with col5:
        st.metric("Total Cost", f"₦{total_cost:,.0f}")
    with col6:
        avg_cost_per_member = total_cost / total_enrolled if total_enrolled > 0 else 0
        st.metric("Avg Cost/Member", f"₦{avg_cost_per_member:,.0f}")
    with col7:
        if terminated_cost > 0:
            st.metric("⚠️ Terminated Cost", f"₦{terminated_cost:,.0f}")
    with col8:
        if terminated_enrollees > 0:
            st.metric("⚠️ Terminated Enrollees", f"{terminated_enrollees:,}")

    # Warning for terminated costs
    if terminated_cost > 0:
        terminated_pct = (terminated_cost / total_cost * 100) if total_cost > 0 else 0
        st.warning(f"""
        **⚠️ TERMINATED ENROLLEE COSTS DETECTED**

        ₦{terminated_cost:,.0f} ({terminated_pct:.1f}% of total costs) attributed to {terminated_enrollees:,} terminated enrollees.

        These are costs from members who are no longer active but had claims during the period.
        This cost is tracked separately and not attributed to any specific plan in the analysis below.
        """)

    # Plan details table
    st.markdown("---")
    st.markdown("## 📋 Plan-by-Plan Analysis")

    plan_details = plan_analysis.get('plan_details', [])

    if plan_details:
        # Create DataFrame
        plan_df = pd.DataFrame(plan_details)

        # Add variance category for filtering
        plan_df['variance_category'] = plan_df['status'].apply(
            lambda x: 'Over-Utilizing' if x == 'OVER_UTILIZING' else
                      'Under-Utilizing' if x == 'UNDER_UTILIZING' else
                      'Balanced'
        )

        # Display filter options
        filter_option = st.radio(
            "Filter by:",
            ["All Plans", "Over-Utilizing Plans", "Under-Utilizing Plans", "Balanced Plans"],
            horizontal=True
        )

        # Apply filter
        if filter_option == "Over-Utilizing Plans":
            filtered_df = plan_df[plan_df['variance_category'] == 'Over-Utilizing']
        elif filter_option == "Under-Utilizing Plans":
            filtered_df = plan_df[plan_df['variance_category'] == 'Under-Utilizing']
        elif filter_option == "Balanced Plans":
            filtered_df = plan_df[plan_df['variance_category'] == 'Balanced']
        else:
            filtered_df = plan_df

        # Create display dataframe
        display_df = filtered_df.copy()
        display_df.index = range(1, len(display_df) + 1)

        # Format columns
        if 'total_enrolled' in display_df.columns:
            display_df['total_enrolled'] = display_df['total_enrolled'].apply(lambda x: f"{int(x):,}")
        if 'claims_cost' in display_df.columns:
            display_df['claims_cost'] = display_df['claims_cost'].apply(lambda x: f"₦{float(x):,.0f}")
        if 'unclaimed_pa_cost' in display_df.columns:
            display_df['unclaimed_pa_cost'] = display_df['unclaimed_pa_cost'].apply(lambda x: f"₦{float(x):,.0f}")
        if 'total_cost' in display_df.columns:
            display_df['total_cost'] = display_df['total_cost'].apply(lambda x: f"₦{float(x):,.0f}")
        if 'cost_per_member' in display_df.columns:
            display_df['cost_per_member'] = display_df['cost_per_member'].apply(lambda x: f"₦{float(x):,.0f}")
        if 'enrollment_pct' in display_df.columns:
            display_df['enrollment_pct'] = display_df['enrollment_pct'].apply(lambda x: f"{float(x):.1f}%")
        if 'cost_pct' in display_df.columns:
            display_df['cost_pct'] = display_df['cost_pct'].apply(lambda x: f"{float(x):.1f}%")
        if 'utilization_variance' in display_df.columns:
            display_df['utilization_variance'] = display_df['utilization_variance'].apply(
                lambda x: f"+{float(x):.1f}%" if x > 0 else f"{float(x):.1f}%"
            )
        if 'unique_claimants' in display_df.columns:
            display_df['unique_claimants'] = display_df['unique_claimants'].apply(lambda x: f"{int(x):,}")
        if 'claimant_ratio' in display_df.columns:
            display_df['claimant_ratio'] = display_df['claimant_ratio'].apply(lambda x: f"{float(x):.1f}%")

        # Add status emoji
        display_df['Alert'] = display_df['status'].apply(
            lambda x: "🔴" if x == 'OVER_UTILIZING' else
                      "🟢" if x == 'UNDER_UTILIZING' else "✅"
        )

        # Rename columns for display
        column_mapping = {
            'planname': 'Plan Name',
            'total_enrolled': 'Enrolled',
            'enrollment_pct': 'Enrollment %',
            'claims_cost': 'Claims Cost',
            'unclaimed_pa_cost': 'PA Cost',
            'total_cost': 'Total Cost',
            'cost_pct': 'Cost %',
            'utilization_variance': 'Variance',
            'cost_per_member': 'Cost/Member',
            'unique_claimants': 'Claimants',
            'claimant_ratio': 'Claimant %',
            'Alert': 'Status'
        }
        display_df = display_df.rename(columns=column_mapping)

        # Select columns to display in order
        cols_to_show = [
            'Plan Name', 'Enrolled', 'Enrollment %',
            'Claims Cost', 'PA Cost', 'Total Cost', 'Cost %',
            'Variance', 'Status', 'Cost/Member', 'Claimants', 'Claimant %'
        ]
        cols_to_show = [col for col in cols_to_show if col in display_df.columns]
        st.dataframe(display_df[cols_to_show], use_container_width=True)

        # Visualizations
        st.markdown("---")
        st.markdown("## 📊 Visual Analysis")

        col1, col2 = st.columns(2)

        with col1:
            # Enrollment vs Cost comparison
            st.markdown("### Enrollment % vs Cost %")

            # Prepare data for chart
            viz_data = []
            for _, row in plan_df.iterrows():
                viz_data.append({
                    'Plan': row['planname'],
                    'Type': 'Enrollment %',
                    'Percentage': row['enrollment_pct']
                })
                viz_data.append({
                    'Plan': row['planname'],
                    'Type': 'Cost %',
                    'Percentage': row['cost_pct']
                })

            viz_df = pd.DataFrame(viz_data)

            fig = px.bar(
                viz_df,
                x='Plan',
                y='Percentage',
                color='Type',
                barmode='group',
                title='Enrollment vs Cost Distribution',
                color_discrete_map={
                    'Enrollment %': '#17a2b8',
                    'Cost %': '#dc3545'
                }
            )
            fig.update_layout(xaxis_tickangle=-45, height=400)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Cost per member by plan
            st.markdown("### Cost per Member by Plan")

            cost_fig = px.bar(
                plan_df.sort_values('cost_per_member', ascending=False),
                x='planname',
                y='cost_per_member',
                title='Cost per Member by Plan',
                color='cost_per_member',
                color_continuous_scale='Reds'
            )
            cost_fig.update_layout(
                xaxis_tickangle=-45,
                height=400,
                xaxis_title='Plan Name',
                yaxis_title='Cost per Member (₦)'
            )
            st.plotly_chart(cost_fig, use_container_width=True)

        # Add Claims vs PA Cost breakdown
        st.markdown("### Claims Cost vs PA Cost by Plan")

        cost_breakdown_data = []
        for _, row in plan_df.iterrows():
            cost_breakdown_data.append({
                'Plan': row['planname'],
                'Cost Type': 'Claims Cost',
                'Amount': row['claims_cost']
            })
            cost_breakdown_data.append({
                'Plan': row['planname'],
                'Cost Type': 'PA Cost',
                'Amount': row['unclaimed_pa_cost']
            })

        breakdown_df = pd.DataFrame(cost_breakdown_data)

        breakdown_fig = px.bar(
            breakdown_df,
            x='Plan',
            y='Amount',
            color='Cost Type',
            barmode='stack',
            title='Claims vs PA Cost Breakdown by Plan',
            color_discrete_map={
                'Claims Cost': '#28a745',
                'PA Cost': '#ffc107'
            }
        )
        breakdown_fig.update_layout(
            xaxis_tickangle=-45,
            height=400,
            xaxis_title='Plan Name',
            yaxis_title='Cost Amount (₦)'
        )
        st.plotly_chart(breakdown_fig, use_container_width=True)

        # Plan-specific recommendations
        st.markdown("---")
        st.markdown("## 🎯 Plan-Specific Recommendations")

        over_utilizing = [p for p in plan_details if p['status'] == 'OVER_UTILIZING']
        under_utilizing = [p for p in plan_details if p['status'] == 'UNDER_UTILIZING']

        if over_utilizing:
            st.error("### 🔴 Over-Utilizing Plans (Cost % > Enrollment %)")
            for plan in over_utilizing:
                with st.expander(f"📋 {plan['planname']} - Variance: +{plan['utilization_variance']:.1f}%"):
                    st.markdown(f"""
                    **Enrollment:** {plan['enrollment_pct']:.1f}% of members
                    **Total Cost:** ₦{plan['total_cost']:,.0f} ({plan['cost_pct']:.1f}% of portfolio)
                    - Claims Cost: ₦{plan['claims_cost']:,.0f}
                    - PA Cost: ₦{plan['unclaimed_pa_cost']:,.0f}
                    **Variance:** +{plan['utilization_variance']:.1f}% over-utilization
                    **Cost per Member:** ₦{plan['cost_per_member']:,.0f}
                    **Claimants:** {plan['unique_claimants']:,} ({plan['claimant_ratio']:.1f}% of enrolled)

                    **Analysis:**
                    This plan is generating disproportionately high costs relative to enrollment.
                    Members on this plan are utilizing services at a rate {plan['utilization_variance']:.1f}%
                    higher than their population share.

                    **Recommended Actions:**
                    {chr(10).join('- ' + rec for rec in plan.get('recommendations', []))}

                    **Renewal Strategy:**
                    - Consider 15-25% premium increase for this specific plan
                    - Implement enhanced pre-authorization for high-cost services
                    - Review plan benefits for potential cost-sharing adjustments
                    - Analyze member demographics to understand driver of high utilization
                    """)

        if under_utilizing:
            st.success("### 🟢 Under-Utilizing Plans (Cost % < Enrollment %)")
            for plan in under_utilizing:
                with st.expander(f"📋 {plan['planname']} - Variance: {plan['utilization_variance']:.1f}%"):
                    st.markdown(f"""
                    **Enrollment:** {plan['enrollment_pct']:.1f}% of members
                    **Total Cost:** ₦{plan['total_cost']:,.0f} ({plan['cost_pct']:.1f}% of portfolio)
                    - Claims Cost: ₦{plan['claims_cost']:,.0f}
                    - PA Cost: ₦{plan['unclaimed_pa_cost']:,.0f}
                    **Variance:** {plan['utilization_variance']:.1f}% under-utilization
                    **Cost per Member:** ₦{plan['cost_per_member']:,.0f}
                    **Claimants:** {plan['unique_claimants']:,} ({plan['claimant_ratio']:.1f}% of enrolled)

                    **Analysis:**
                    This plan is generating lower costs relative to enrollment. This could indicate:
                    - Healthier population segment
                    - Barriers to care access
                    - Better preventive care
                    - Possible gaming with enrollment but low utilization

                    **Recommended Actions:**
                    {chr(10).join('- ' + rec for rec in plan.get('recommendations', []))}

                    **Renewal Strategy:**
                    - Potential for modest premium reduction (0-5%) to incentivize enrollment
                    - Maintain current benefits structure
                    - Monitor for adverse selection if adjustments made
                    - Consider expanding marketing for this plan type
                    """)

        # Overall portfolio insights
        st.markdown("---")
        st.markdown("## 💡 Portfolio-Level Strategic Insights")

        if len(over_utilizing) > len(plan_details) * 0.5:
            st.warning(f"""
            **⚠️ PORTFOLIO IMBALANCE DETECTED**

            {len(over_utilizing)} out of {len(plan_details)} plans are over-utilizing
            ({len(over_utilizing)/len(plan_details)*100:.1f}% of portfolio).

            **Strategic Implications:**
            - Overall portfolio may be mispriced
            - Consider across-the-board premium adjustment
            - Review plan design and member cost-sharing structure
            - Analyze whether specific conditions or services are driving the imbalance
            """)
        elif len(under_utilizing) > len(plan_details) * 0.5:
            st.info(f"""
            **✅ FAVORABLE PORTFOLIO POSITION**

            {len(under_utilizing)} out of {len(plan_details)} plans are under-utilizing
            ({len(under_utilizing)/len(plan_details)*100:.1f}% of portfolio).

            **Strategic Opportunities:**
            - Strong pricing position for renewal negotiations
            - Potential to expand enrollment in profitable plans
            - Consider slight premium reductions to capture market share
            - Maintain vigilance for adverse selection if market conditions change
            """)
        else:
            st.success("""
            **✅ BALANCED PORTFOLIO**

            Your plan portfolio shows reasonable balance between enrollment and claims distribution.

            **Maintain Current Strategy:**
            - Continue monitoring individual plan performance
            - Apply targeted adjustments to outlier plans
            - Overall portfolio pricing appears appropriate
            """)


def render_benefit_limits_tab(benefit_analysis: dict, company_name: str):
    """Tab 9: Benefit Limit Analysis"""

    st.markdown("# 🚨 Benefit Limit Analysis")

    if not benefit_analysis or not benefit_analysis.get('success'):
        st.error(f"❌ Failed to analyze benefit limits: {benefit_analysis.get('error', 'Unknown error')}")
        return

    # Summary metrics at the top
    st.markdown("## 📊 Summary Metrics")
    
    st.info("📅 **Date Filter:** Using `encounterdatefrom` for claims (service date) | `requestdate` for PA")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "💰 Total Monetary Loss",
            f"₦{benefit_analysis.get('total_monetary_loss', 0):,.2f}",
            help="Total amount paid beyond defined benefit limits (Active + Terminated)"
        )
        st.caption(f"Active: ₦{benefit_analysis.get('active_monetary_loss', 0):,.2f} | Terminated: ₦{benefit_analysis.get('terminated_monetary_loss', 0):,.2f}")

    with col2:
        st.metric(
            "👥 Members Over Limit",
            f"{benefit_analysis.get('total_members_over_limit', 0):,}",
            help="Number of members who exceeded monetary limits"
        )
        st.caption(f"Active: {benefit_analysis.get('active_members_over_limit', 0):,} | Terminated: {benefit_analysis.get('terminated_members_over_limit', 0):,}")

    with col3:
        st.metric(
            "🎯 Benefits Violated",
            f"{benefit_analysis.get('total_benefits_violated', 0):,}",
            help="Number of benefit types with limit violations"
        )

    with col4:
        st.metric(
            "⚠️ Unlimited Benefits Spending",
            f"₦{benefit_analysis.get('no_limit_spending', 0):,.2f}",
            help="Total spending on benefits with no limits defined"
        )

    st.markdown("---")

    # Tab navigation for detailed views
    tab1, tab2, tab3, tab4 = st.tabs([
        "✅ Active Members",
        "🔴 Terminated Members",
        "📈 Benefit Breakdown",
        "⚠️ Unlimited Benefits"
    ])

    active_violations = benefit_analysis.get('active_violations', [])
    terminated_violations = benefit_analysis.get('terminated_violations', [])
    violations = benefit_analysis.get('violations', [])

    with tab1:
        st.markdown("### ✅ Active Members Who Exceeded Benefit Limits")

        if not active_violations:
            st.success("✅ No active members exceeded their benefit limits during this period.")
        else:
            # Filter for actual monetary violations
            monetary_violations = [v for v in active_violations if v.get('monetary_overage', 0) > 0]

            if monetary_violations:
                st.warning(f"Found {len(monetary_violations)} benefit limit violations affecting {benefit_analysis.get('active_members_over_limit', 0)} active members")

                # Convert to DataFrame for display
                df = pd.DataFrame(monetary_violations)

                # Format display columns
                display_df = pd.DataFrame({
                    'Member ID': df['enrollee_id'],
                    'Plan': df['planname'].fillna('Unknown'),
                    'Benefit Code': df['benefitcode'].fillna('Unknown'),
                    'Limit (₦)': df['maxlimit'].apply(lambda x: f"₦{x:,.0f}" if pd.notna(x) else 'N/A'),
                    'Total Spent (₦)': df['total_spent'].apply(lambda x: f"₦{x:,.2f}"),
                    'Overage (₦)': df['monetary_overage'].apply(lambda x: f"₦{x:,.2f}"),
                    'Visit Count': df['visit_count'],
                    'Count Limit': df['countperannum'].fillna('N/A')
                })

                st.dataframe(
                    display_df,
                    use_container_width=True,
                    height=400
                )

                # Download CSV
                csv = df.to_csv(index=False)
                st.download_button(
                    "📥 Download Active Members Violations (CSV)",
                    csv,
                    f"benefit_violations_active_{company_name}_{datetime.now().strftime('%Y%m%d')}.csv",
                    "text/csv",
                    key='download_active_violations'
                )

                # Recommendations
                st.markdown("---")
                st.markdown("### 💡 Recommended Actions for Active Members")

                active_loss = benefit_analysis.get('active_monetary_loss', 0)

                st.info(f"""
                **Immediate Actions:**
                - **Review Benefit Design**: {len(monetary_violations)} violations suggest limits may be too low or not enforced
                - **Member Education**: Notify affected active members about their limit status
                - **Claims Review**: Investigate whether overages are medically necessary or indicate fraud/abuse
                - **Financial Impact**: ₦{active_loss:,.2f} in unexpected costs from active members - factor into renewal pricing

                **Strategic Considerations:**
                - If violations are frequent: Consider raising limits or implementing better pre-authorization
                - If violations are concentrated in specific benefits: Target those for benefit redesign
                - If specific members repeatedly violate: May indicate chronic conditions needing care management
                """)
            else:
                st.success("✅ No monetary violations found for active members.")

    with tab2:
        st.markdown("### 🔴 Terminated Members Who Exceeded Benefit Limits")
        st.warning("⚠️ These are members who had claims/PA during the period but are no longer active. Their violations are tracked separately.")

        if not terminated_violations:
            st.success("✅ No terminated members exceeded their benefit limits during this period.")
        else:
            # Filter for actual monetary violations
            monetary_violations = [v for v in terminated_violations if v.get('monetary_overage', 0) > 0]

            if monetary_violations:
                st.error(f"Found {len(monetary_violations)} benefit limit violations affecting {benefit_analysis.get('terminated_members_over_limit', 0)} terminated members")

                # Convert to DataFrame for display
                df = pd.DataFrame(monetary_violations)

                # Format display columns
                display_df = pd.DataFrame({
                    'Member ID': df['enrollee_id'],
                    'Plan': df['planname'].fillna('Unknown'),
                    'Benefit Code': df['benefitcode'].fillna('Unknown'),
                    'Limit (₦)': df['maxlimit'].apply(lambda x: f"₦{x:,.0f}" if pd.notna(x) else 'N/A'),
                    'Total Spent (₦)': df['total_spent'].apply(lambda x: f"₦{x:,.2f}"),
                    'Overage (₦)': df['monetary_overage'].apply(lambda x: f"₦{x:,.2f}"),
                    'Visit Count': df['visit_count'],
                    'Count Limit': df['countperannum'].fillna('N/A')
                })

                st.dataframe(
                    display_df,
                    use_container_width=True,
                    height=400
                )

                # Download CSV
                csv = df.to_csv(index=False)
                st.download_button(
                    "📥 Download Terminated Members Violations (CSV)",
                    csv,
                    f"benefit_violations_terminated_{company_name}_{datetime.now().strftime('%Y%m%d')}.csv",
                    "text/csv",
                    key='download_terminated_violations'
                )

                # Recommendations
                st.markdown("---")
                st.markdown("### 💡 Notes on Terminated Members")

                terminated_loss = benefit_analysis.get('terminated_monetary_loss', 0)

                st.warning(f"""
                **Important Notes:**
                - These members are **no longer active** but had claims during the contract period
                - **Financial Impact**: ₦{terminated_loss:,.2f} in overages from terminated members
                - These costs are **historical** and cannot be recovered or managed going forward
                - Consider this when setting renewal pricing - these members won't be in the new contract
                - Review termination patterns: Are high-cost members being terminated to avoid limits?
                """)
            else:
                st.success("✅ No monetary violations found for terminated members.")

    with tab3:
        st.markdown("### Benefit-Level Breakdown")

        if violations:
            # Group by benefit code
            df = pd.DataFrame(violations)

            # Aggregate by benefit
            benefit_summary = df.groupby('benefitcode').agg({
                'enrollee_id': 'nunique',
                'monetary_overage': 'sum',
                'total_spent': 'sum',
                'maxlimit': 'first'
            }).reset_index()

            benefit_summary.columns = ['Benefit Code', 'Members', 'Total Overage', 'Total Spent', 'Limit']
            benefit_summary = benefit_summary.sort_values('Total Overage', ascending=False)

            # Display top violating benefits
            st.markdown("#### Top Benefits by Total Overage")

            # Create bar chart
            fig = px.bar(
                benefit_summary.head(10),
                x='Benefit Code',
                y='Total Overage',
                title='Top 10 Benefits by Total Monetary Overage',
                labels={'Total Overage': 'Overage Amount (₦)'},
                color='Total Overage',
                color_continuous_scale='Reds'
            )

            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

            # Display table
            display_summary = pd.DataFrame({
                'Benefit Code': benefit_summary['Benefit Code'],
                'Members Affected': benefit_summary['Members'],
                'Total Overage (₦)': benefit_summary['Total Overage'].apply(lambda x: f"₦{x:,.2f}"),
                'Total Spent (₦)': benefit_summary['Total Spent'].apply(lambda x: f"₦{x:,.2f}"),
                'Defined Limit (₦)': benefit_summary['Limit'].apply(lambda x: f"₦{x:,.0f}" if pd.notna(x) else 'N/A')
            })

            st.dataframe(display_summary, use_container_width=True)

        else:
            st.info("No benefit violations to display.")

    with tab4:
        st.markdown("### Benefits with No Limits Defined")

        # Filter for unlimited benefits
        unlimited = [v for v in violations if v.get('no_limit_flag', 0) == 1]

        if unlimited:
            st.warning(f"""
            ⚠️ **RISK ALERT**: {benefit_analysis.get('members_no_limit', 0)} members used benefits
            with NO defined limits, totaling ₦{benefit_analysis.get('no_limit_spending', 0):,.2f}

            This represents unlimited financial exposure!
            """)

            # Convert to DataFrame
            df = pd.DataFrame(unlimited)

            # Group by benefit
            unlimited_summary = df.groupby('benefitcode').agg({
                'enrollee_id': 'nunique',
                'total_spent': 'sum',
                'visit_count': 'sum'
            }).reset_index()

            unlimited_summary.columns = ['Benefit Code', 'Members', 'Total Spent', 'Total Visits']
            unlimited_summary = unlimited_summary.sort_values('Total Spent', ascending=False)

            # Display
            display_unlimited = pd.DataFrame({
                'Benefit Code': unlimited_summary['Benefit Code'],
                'Members Using': unlimited_summary['Members'],
                'Total Spent (₦)': unlimited_summary['Total Spent'].apply(lambda x: f"₦{x:,.2f}"),
                'Total Visits': unlimited_summary['Total Visits']
            })

            st.dataframe(display_unlimited, use_container_width=True)

            # Urgent recommendations
            st.markdown("---")
            st.error(f"""
            ### 🚨 URGENT: Unlimited Benefits Detected

            **Immediate Actions Required:**
            1. **Define Limits NOW**: Set monetary and/or count limits for these {len(unlimited_summary)} benefit types
            2. **Historical Analysis**: Review past utilization to set appropriate limits
            3. **Industry Benchmarks**: Compare against standard HMO benefit structures
            4. **Financial Modeling**: Project potential costs if current usage continues uncapped

            **Recommended Limits to Consider:**
            - Annual maximums based on 95th percentile of current usage
            - Per-visit or per-procedure caps
            - Frequency limits (e.g., max visits per year)
            - Prior authorization requirements for high-cost procedures

            **Financial Risk**: Current unlimited exposure = ₦{benefit_analysis.get('no_limit_spending', 0):,.2f}
            """)

        else:
            st.success("✅ All benefits have defined limits. No unlimited exposure detected.")

    st.markdown("---")
    st.markdown("### 📋 Summary Report")

    # Generate downloadable summary
    summary_text = f"""
BENEFIT LIMIT ANALYSIS REPORT
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Company: {company_name}
Date Filter: encounterdatefrom (claims) | requestdate (PA)

SUMMARY METRICS:
- Total Monetary Loss from Over-Limit Claims: ₦{benefit_analysis.get('total_monetary_loss', 0):,.2f}
  * Active Members: ₦{benefit_analysis.get('active_monetary_loss', 0):,.2f}
  * Terminated Members: ₦{benefit_analysis.get('terminated_monetary_loss', 0):,.2f}
- Members Who Exceeded Limits: {benefit_analysis.get('total_members_over_limit', 0):,}
  * Active Members: {benefit_analysis.get('active_members_over_limit', 0):,}
  * Terminated Members: {benefit_analysis.get('terminated_members_over_limit', 0):,}
- Benefit Types Violated: {benefit_analysis.get('total_benefits_violated', 0):,}
- Members with Unlimited Benefits: {benefit_analysis.get('members_no_limit', 0):,}
- Unlimited Benefits Spending: ₦{benefit_analysis.get('no_limit_spending', 0):,.2f}

RECOMMENDATIONS:
1. Review and adjust benefit limits based on utilization patterns
2. Implement stronger pre-authorization controls for high-cost benefits
3. Define limits for any unlimited benefits immediately
4. Factor overage costs into renewal pricing calculations (focus on active members)
5. Investigate repeated violators for potential care management intervention
6. Review terminated member patterns - are high-cost members being terminated to avoid limits?

---
Detailed violations data available in downloadable CSV format.
Active and terminated members are tracked separately.
"""

    st.download_button(
        "📥 Download Summary Report",
        summary_text,
        f"benefit_limits_summary_{company_name}_{datetime.now().strftime('%Y%m%d')}.txt",
        "text/plain",
        key='download_summary'
    )