# HEALTHINSIGHT - AI Data Analysis Suite

This folder contains all AI-powered data analysis tools and services for the health insurance platform.

## Core Services

### KLAIRE Chat Service (`klaire_service.py`)
- FastAPI service providing natural language queries to DuckDB
- Runs on port 8787
- Handles enrollee diagnosis queries, utilization analysis, and data exploration
- Accessible via Chrome extension or Streamlit app sidebars

### Streamlit Applications
- **`ai_health_analyst.py`**: Main AI health insurance analyst dashboard
- **`MLR.py`**: MLR (Medical Loss Ratio) analysis and email alerts
- **`band_streamlit.py`**: Band analysis application
- **`ai_data_explorer.py`**: Data exploration tools

## Analysis Scripts
- `CHECK_BENEFIT.py`: Benefit code verification and analysis
- `fetch_price.py`: Procedure price analysis
- `create_aviation_analysis_complete.py`: Aviation company analysis
- `create_performance_report.py`: Performance reporting
- `classify_procedures_smart.py`: Smart procedure classification

## Setup Files
- `setup_ai_analyst.py`: Initial setup for AI analyst
- `setup_ai_database.py`: Database setup scripts
- `requirements_ai.txt`: Python dependencies for AI tools

## Usage

### Starting the HEALTHINSIGHT Chat Service
```bash
cd healthinsight
source ../venv/bin/activate
uvicorn klaire_service:app --host 0.0.0.0 --port 8787
```

### Running Streamlit Apps
```bash
cd healthinsight
source ../venv/bin/activate
streamlit run ai_health_analyst.py --server.port 8501
streamlit run MLR.py --server.port 8502
```

## Database
All tools connect to `ai_driven_data.duckdb` in the parent directory.

## Notes
- The HEALTHINSIGHT service automatically handles enrollee ID variations (e.g., `CL/XXX/123/2024-A` vs `CL/XXX/123/2024-A-E`)
- Chat history is persisted in `healthinsight_chat.duckdb`
- All analysis scripts use DuckDB as the primary data source unless explicitly told to use MediCloud

