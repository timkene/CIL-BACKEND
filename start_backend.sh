#!/bin/bash

# Navigate to project directory
cd "$(dirname "$0")"

echo "🚀 Starting MLR Analysis Backend..."
echo "=================================="
echo ""

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "❌ Virtual environment not found!"
    echo "Please create one first: python3 -m venv venv"
    exit 1
fi

# Activate virtual environment
echo "✅ Activating virtual environment..."
source venv/bin/activate

# Check if FastAPI is installed
if ! python -c "import fastapi" 2>/dev/null; then
    echo "❌ FastAPI not installed!"
    echo "Installing required packages..."
    pip install fastapi uvicorn polars pandas python-multipart
fi

# Kill any existing process on port 8000
echo "🔄 Checking for existing backend..."
if lsof -ti:8000 > /dev/null 2>&1; then
    echo "⚠️  Killing existing process on port 8000..."
    lsof -ti:8000 | xargs kill -9 2>/dev/null
    sleep 1
fi

echo ""
echo "🎉 Starting backend on http://localhost:8000"
echo "📚 API Docs: http://localhost:8000/docs"
echo ""
echo "Press Ctrl+C to stop"
echo "=================================="
echo ""

# Start the backend
python main.py
