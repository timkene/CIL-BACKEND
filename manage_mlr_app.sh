#!/bin/bash

# MLR Streamlit App Manager
# Automatically starts/refreshes MLR.py at scheduled times (10 AM, 1 PM, 3 PM) on weekdays

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

VENV_PYTHON="$SCRIPT_DIR/venv/bin/python"
STREAMLIT_CMD="$SCRIPT_DIR/venv/bin/streamlit"
MLR_SCRIPT="$SCRIPT_DIR/MLR.py"
PORT=8501
PID_FILE="$SCRIPT_DIR/.mlr_app.pid"

# Function to check if it's a weekday (Monday-Friday)
is_weekday() {
    day=$(date +%u)  # 1=Monday, 7=Sunday
    [ "$day" -ge 1 ] && [ "$day" -le 5 ]
}

# Function to get current hour
get_hour() {
    date +%H | sed 's/^0//'  # Remove leading zero if present
}

# Function to stop existing Streamlit process
stop_app() {
    if [ -f "$PID_FILE" ]; then
        old_pid=$(cat "$PID_FILE")
        if ps -p "$old_pid" > /dev/null 2>&1; then
            echo "Stopping existing MLR app (PID: $old_pid)..."
            kill "$old_pid" 2>/dev/null
            sleep 2
            # Force kill if still running
            if ps -p "$old_pid" > /dev/null 2>&1; then
                kill -9 "$old_pid" 2>/dev/null
            fi
        fi
        rm -f "$PID_FILE"
    fi
    
    # Also kill any other Streamlit processes on this port
    pkill -f "streamlit run MLR.py" 2>/dev/null || true
}

# Function to start the Streamlit app
start_app() {
    echo "Starting MLR Streamlit app..."
    export STREAMLIT_SERVER_FILE_WATCHER_TYPE=none
    export WATCHDOG_FORCE_POLLING=true
    
    nohup "$STREAMLIT_CMD" run "$MLR_SCRIPT" \
        --server.port "$PORT" \
        --server.address 127.0.0.1 \
        --server.headless true \
        > "$SCRIPT_DIR/.mlr_app.log" 2>&1 &
    
    streamlit_pid=$!
    echo "$streamlit_pid" > "$PID_FILE"
    echo "MLR app started (PID: $streamlit_pid) at http://127.0.0.1:$PORT"
}

# Main logic
if ! is_weekday; then
    echo "$(date): Today is not a weekday. Exiting."
    exit 0
fi

current_hour=$(get_hour)

# Check if we should run (10 AM, 1 PM, or 3 PM)
# Also allow starting if it's after 10 AM and before 3 PM (system just booted)
if [ "$current_hour" -eq 10 ] || [ "$current_hour" -eq 13 ] || [ "$current_hour" -eq 15 ]; then
    stop_app
    sleep 1
    start_app
    
    # If it's 3 PM, schedule a stop after 1 minute
    if [ "$current_hour" -eq 15 ]; then
        echo "$(date): Scheduling app closure at 3:01 PM..."
        (
            sleep 60
            stop_app
            echo "$(date): App closed at 3:01 PM"
        ) &
    fi
elif [ "$current_hour" -gt 10 ] && [ "$current_hour" -lt 15 ]; then
    # System booted mid-day - start the app if it's not already running
    if [ ! -f "$PID_FILE" ] || ! ps -p "$(cat "$PID_FILE" 2>/dev/null)" > /dev/null 2>&1; then
        echo "$(date): System booted during work hours. Starting MLR app..."
        start_app
    else
        echo "$(date): MLR app is already running (PID: $(cat "$PID_FILE"))"
    fi
else
    echo "$(date): Current hour ($current_hour) is not a scheduled time. Exiting."
    exit 0
fi

