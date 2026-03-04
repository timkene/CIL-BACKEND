#!/bin/bash
# Kill processes on common dev ports so you can run Streamlit/API/frontend cleanly.
set -e
PORTS="8501 8000 3000 5000 8080"
for port in $PORTS; do
  pid=$(lsof -ti :$port 2>/dev/null || true)
  if [ -n "$pid" ]; then
    echo "Killing PID $pid on port $port"
    kill -9 $pid 2>/dev/null || true
  fi
done
# Also kill by name (in case port changed)
pkill -f "streamlit run" 2>/dev/null || true
pkill -f "uvicorn main:app" 2>/dev/null || true
pkill -f "python.*main.py" 2>/dev/null || true
echo "Done. Ports $PORTS and common dev processes cleared."
