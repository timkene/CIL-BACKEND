# HEALTHINSIGHT Folder Structure

This folder (`healthinsight/`) contains **ALL** AI data analysis tools and services. HEALTHINSIGHT knows about this folder and focuses exclusively on it.

## Organization

```
healthinsight/
├── klaire_service.py                 # Main chat service (FastAPI, port 8787)
├── ai_health_analyst.py              # Main AI dashboard (Streamlit)
├── MLR.py                            # MLR analysis & email alerts (Streamlit)
├── band_streamlit.py                 # Band analysis (Streamlit)
├── ai_data_explorer.py               # Data exploration tools
│
├── Analysis Scripts/
│   ├── CHECK_BENEFIT.py              # Benefit code verification
│   ├── fetch_price.py                # Procedure price analysis
│   ├── create_aviation_analysis_complete.py
│   ├── create_performance_report.py
│   └── classify_procedures_smart.py
│
├── Setup & Config/
│   ├── setup_ai_analyst.py
│   ├── setup_ai_database.py
│   ├── .healthinsight_config.py      # Configuration
│   ├── start_services.sh             # Start all services
│   └── stop_services.sh              # Stop all services
│
├── Documentation/
│   ├── README.md                     # This folder overview
│   ├── README_AI_ANALYST.md
│   └── README_streamlit.md
│
└── Data/
    └── healthinsight_chat.duckdb     # Chat history database
```

## Key Points

1. **KLAIRE Service** (`klaire_service.py`):
   - Knows it's in the `healthinsight/` folder
   - Accesses `ai_driven_data.duckdb` from parent directory
   - Stores chat history in this folder
   - Focused exclusively on AI data analysis

2. **Database Access**:
   - All scripts access `../ai_driven_data.duckdb` (parent directory)
   - Chat history: `healthinsight_chat.duckdb` (this folder)

3. **Running Services**:
   ```bash
   cd healthinsight
   ./start_services.sh              # Start chat service only
   ./start_services.sh --with-streamlit  # Start chat + Streamlit apps
   ./stop_services.sh               # Stop all services
   ```

4. **HEALTHINSIGHT Knowledge**:
   - All tables in DuckDB
   - Enrollee queries (diagnoses, procedures, utilization)
   - Company/group financial analysis
   - All analysis scripts in this folder

## What's NOT in this folder

- `dlt_sources.py` - Data loading (stays in root)
- `auto_update_database.py` - Database updates (stays in root)
- `ai_driven_data.duckdb` - Main database (stays in root)
- Other non-AI scripts

