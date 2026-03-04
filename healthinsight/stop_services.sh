#!/bin/bash
# Stop HEALTHINSIGHT services

cd "$(dirname "$0")"

# Kill by PID files
if [ -f klaire_service.pid ]; then
    kill $(cat klaire_service.pid) 2>/dev/null
    rm klaire_service.pid
    echo "✅ Stopped KLAIRE service"
fi

if [ -f ai_analyst.pid ]; then
    kill $(cat ai_analyst.pid) 2>/dev/null
    rm ai_analyst.pid
    echo "✅ Stopped AI Health Analyst"
fi

if [ -f mlr_streamlit.pid ]; then
    kill $(cat mlr_streamlit.pid) 2>/dev/null
    rm mlr_streamlit.pid
    echo "✅ Stopped MLR Dashboard"
fi

# Kill by ports (fallback)
lsof -ti:8787 | xargs kill -9 2>/dev/null
lsof -ti:8501 | xargs kill -9 2>/dev/null
lsof -ti:8502 | xargs kill -9 2>/dev/null

echo "All services stopped."

