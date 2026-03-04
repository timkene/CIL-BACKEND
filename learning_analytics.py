#!/usr/bin/env python3
"""
LEARNING ANALYTICS DASHBOARD
============================

Streamlit dashboard for tracking learning system performance

Features:
- Cost savings over time
- Learning growth charts
- Top performing rules
- ROI calculator

Author: Casey's AI Assistant  
Date: February 2026
Version: 1.0
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from vetting_learning_engine import LearningVettingEngine
import duckdb

st.set_page_config(
    page_title="Learning Analytics",
    page_icon="📊",
    layout="wide"
)

# Initialize engine
if 'engine' not in st.session_state:
    st.session_state.engine = LearningVettingEngine()

st.title("📊 Learning System Analytics Dashboard")
st.markdown("*Track AI cost savings and learning performance*")
st.markdown("---")

# Get stats
stats = st.session_state.engine.get_learning_stats()

# Top metrics
col1, col2, col3, col4 = st.columns(4)

if 'summary' in stats:
    with col1:
        st.metric(
            "💰 Total Savings",
            f"${stats['summary']['cost_savings_usd']}",
            help="Total AI costs saved by reusing learned rules"
        )
    
    with col2:
        st.metric(
            "🔄 AI Calls Saved",
            f"{stats['summary']['total_reuses']}",
            help="Number of times learned rules were reused"
        )
    
    total_rules = sum(s['total_rules'] for s in stats.values() if isinstance(s, dict) and 'total_rules' in s)
    with col3:
        st.metric(
            "📚 Total Rules",
            f"{total_rules}",
            help="Total number of learned rules across all tables"
        )
    
    avg_reuses = stats['summary']['total_reuses'] / max(total_rules, 1)
    with col4:
        st.metric(
            "📈 Avg Reuses/Rule",
            f"{avg_reuses:.1f}",
            help="Average number of times each rule has been reused"
        )

# Learning by table
st.markdown("---")
st.subheader("📋 Learning Rules by Table")

table_data = []
for table_name, table_stats in stats.items():
    if table_name != 'summary':
        table_data.append({
            'Table': table_name,
            'Total Rules': table_stats['total_rules'],
            'Total Reuses': table_stats['total_reuses'],
            'Avg Reuses': table_stats['avg_reuses_per_rule'],
            'Valid': table_stats['valid_count'],
            'Invalid': table_stats['invalid_count'],
            'Max Usage': table_stats['max_usage']
        })

if table_data:
    df_tables = pd.DataFrame(table_data)
    st.dataframe(df_tables, use_container_width=True)
    
    # Visualization
    col1, col2 = st.columns(2)
    
    with col1:
        # Pie chart: Rules by table
        fig = px.pie(
            df_tables,
            values='Total Rules',
            names='Table',
            title='Distribution of Rules by Table',
            hole=0.3
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Bar chart: Reuses by table
        fig = px.bar(
            df_tables,
            x='Table',
            y='Total Reuses',
            title='Total Reuses by Table',
            color='Total Reuses',
            color_continuous_scale='Viridis'
        )
        st.plotly_chart(fig, use_container_width=True)

# Top performing rules
st.markdown("---")
st.subheader("🏆 Top 10 Most-Used Rules")

try:
    conn = duckdb.connect('ai_driven_data.duckdb', read_only=True)
    
    top_rules = conn.execute("""
        SELECT 
            procedure_code,
            diagnosis_code,
            is_valid_match,
            usage_count,
            match_reason,
            usage_count * 0.003 as dollars_saved
        FROM "PROCEDURE_DIAGNOSIS"."ai_human_procedure_diagnosis"
        ORDER BY usage_count DESC
        LIMIT 10
    """).fetchdf()
    
    conn.close()
    
    if not top_rules.empty:
        # Format for display
        top_rules['is_valid_match'] = top_rules['is_valid_match'].apply(lambda x: '✅ Valid' if x else '❌ Invalid')
        top_rules['dollars_saved'] = top_rules['dollars_saved'].apply(lambda x: f"${x:.3f}")
        
        top_rules.columns = ['Procedure', 'Diagnosis', 'Valid?', 'Usage Count', 'Reason', 'Savings']
        
        st.dataframe(top_rules, use_container_width=True)
    else:
        st.info("No rules learned yet. Start vetting claims to build learning data!")

except Exception as e:
    st.warning(f"Could not load top rules: {e}")

# ROI Calculator
st.markdown("---")
st.subheader("💡 ROI Projection Calculator")

col1, col2 = st.columns(2)

with col1:
    monthly_requests = st.number_input(
        "Expected Monthly PA Requests",
        min_value=100,
        max_value=100000,
        value=10000,
        step=1000,
        help="How many PA requests do you expect per month?"
    )

with col2:
    months_projection = st.slider(
        "Projection Period (months)",
        min_value=1,
        max_value=12,
        value=6,
        help="How far ahead to project savings"
    )

# Calculate projections
ai_cost_per_call = 0.003
learning_rate_curve = [1.0, 0.85, 0.70, 0.55, 0.40, 0.30, 0.25, 0.23, 0.21, 0.20, 0.20, 0.20]

projections = []
for month in range(1, months_projection + 1):
    ai_rate = learning_rate_curve[min(month - 1, 11)]
    ai_calls = monthly_requests * ai_rate
    ai_cost = ai_calls * ai_cost_per_call
    learned_hits = monthly_requests * (1 - ai_rate)
    savings = learned_hits * ai_cost_per_call
    
    projections.append({
        'Month': month,
        'AI Calls': int(ai_calls),
        'Learned Hits': int(learned_hits),
        'AI Cost': ai_cost,
        'Savings': savings
    })

df_proj = pd.DataFrame(projections)

# Display projections
col1, col2 = st.columns(2)

with col1:
    # Cost over time chart
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df_proj['Month'],
        y=df_proj['AI Cost'],
        mode='lines+markers',
        name='AI Cost',
        line=dict(color='red')
    ))
    
    fig.add_trace(go.Scatter(
        x=df_proj['Month'],
        y=df_proj['Savings'],
        mode='lines+markers',
        name='Savings',
        line=dict(color='green')
    ))
    
    fig.update_layout(
        title='Cost vs Savings Over Time',
        xaxis_title='Month',
        yaxis_title='USD ($)',
        hovermode='x unified'
    )
    
    st.plotly_chart(fig, use_container_width=True)

with col2:
    # AI calls reduction chart
    fig = px.area(
        df_proj,
        x='Month',
        y=['AI Calls', 'Learned Hits'],
        title='AI Calls vs Learned Hits',
        labels={'value': 'Requests', 'variable': 'Type'}
    )
    
    st.plotly_chart(fig, use_container_width=True)

# Summary metrics
st.markdown("### 📈 Projection Summary")

total_ai_cost = df_proj['AI Cost'].sum()
total_savings = df_proj['Savings'].sum()
total_requests = monthly_requests * months_projection

col1, col2, col3 = st.columns(3)

with col1:
    st.metric(
        f"Total Cost ({months_projection} months)",
        f"${total_ai_cost:.2f}",
        help="Total AI costs over projection period"
    )

with col2:
    st.metric(
        f"Total Savings ({months_projection} months)",
        f"${total_savings:.2f}",
        help="Total savings from learned rules"
    )

with col3:
    reduction_pct = (total_savings / (total_ai_cost + total_savings)) * 100
    st.metric(
        "Cost Reduction",
        f"{reduction_pct:.1f}%",
        help="Percentage reduction in AI costs"
    )

# Refresh button
st.markdown("---")
if st.button("🔄 Refresh Dashboard", type="primary", use_container_width=True):
    st.rerun()