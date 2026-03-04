"""
HEALTHINSIGHT Configuration
This file defines the HEALTHINSIGHT folder structure and context.
"""
import os

# Get the HEALTHINSIGHT folder path
HEALTHINSIGHT_FOLDER = os.path.dirname(os.path.abspath(__file__))
# Parent directory (DLT root)
DLT_ROOT = os.path.dirname(HEALTHINSIGHT_FOLDER)

# Database paths
DUCKDB_PATH = os.path.join(DLT_ROOT, 'ai_driven_data.duckdb')
CHAT_DB_PATH = os.path.join(HEALTHINSIGHT_FOLDER, 'healthinsight_chat.duckdb')

# HEALTHINSIGHT knows about these analysis tools:
ANALYSIS_TOOLS = [
    'ai_health_analyst.py',  # Main AI health insurance analyst dashboard
    'MLR.py',  # MLR analysis and email alerts
    'band_streamlit.py',  # Band analysis
    'ai_data_explorer.py',  # Data exploration
    'CHECK_BENEFIT.py',  # Benefit code verification
    'fetch_price.py',  # Procedure price analysis
    'create_aviation_analysis_complete.py',  # Aviation company analysis
    'create_performance_report.py',  # Performance reporting
]

# HEALTHINSIGHT context - what it knows
HEALTHINSIGHT_KNOWLEDGE = """
HEALTHINSIGHT is an AI data analyst for health insurance data. It has access to:

1. DuckDB Database (ai_driven_data.duckdb):
   - All claims, PA (pre-authorization), members, groups, providers
   - Financial data (debit notes, cash received, expenses, salaries)
   - Procedure codes, benefit codes, tariffs, plans

2. Analysis Capabilities:
   - Enrollee utilization queries (diagnoses, procedures, PA)
   - Company/group financial analysis
   - Procedure price analysis
   - Benefit code verification
   - MLR (Medical Loss Ratio) calculations

3. Natural Language Queries:
   - "What diagnosis has [enrollee_id] been diagnosed with in the last X months?"
   - "Show utilization for [enrollee_id]"
   - "Calculate total claims + unclaimed PA for [company]"
   - SQL queries with "SQL:" prefix

4. All scripts in this folder are focused on AI-powered health insurance data analysis.
"""

