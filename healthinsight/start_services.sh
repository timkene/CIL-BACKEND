#!/bin/bash
# Start HEALTHINSIGHT services from the healthinsight folder

cd "$(dirname "$0")"
DLT_ROOT="$(dirname "$(pwd)")"

# Activate virtual environment
source "$DLT_ROOT/venv/bin/activate"

# Kill existing processes on ports
lsof -ti:8787 | xargs kill -9 2>/dev/null
lsof -ti:8501 | xargs kill -9 2>/dev/null
lsof -ti:8502 | xargs kill -9 2>/dev/null

sleep 1

# Start KLAIRE chat service
echo "Starting KLAIRE chat service on port 8787..."
nohup uvicorn klaire_service:app --host 0.0.0.0 --port 8787 > klaire_service.log 2>&1 &
echo $! > klaire_service.pid
echo "✅ KLAIRE service started (PID: $(cat klaire_service.pid))"

# Optional: Start Streamlit apps
if [ "$1" == "--with-streamlit" ]; then
    echo "Starting Streamlit apps..."
    nohup streamlit run ai_health_analyst.py --server.port 8501 > ai_analyst.log 2>&1 &
    echo $! > ai_analyst.pid
    echo "✅ AI Health Analyst started on port 8501"
    
    nohup streamlit run MLR.py --server.port 8502 > mlr_streamlit.log 2>&1 &
    echo $! > mlr_streamlit.pid
    echo "✅ MLR Dashboard started on port 8502"
fi

echo ""
echo "Services running:"
echo "  - HEALTHINSIGHT Chat: http://localhost:8787"
[ "$1" == "--with-streamlit" ] && echo "  - AI Health Analyst: http://localhost:8501"
[ "$1" == "--with-streamlit" ] && echo "  - MLR Dashboard: http://localhost:8502"
echo ""
echo "To stop services: ./stop_services.sh"

