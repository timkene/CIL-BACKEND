#!/bin/bash

# Start script for Benefit Limit Analysis application
# This activates the virtual environment and runs Streamlit

cd "$(dirname "$0")"

echo "🚀 Starting Benefit Limit Analysis Application..."
echo ""

# Activate virtual environment
source venv/bin/activate

# Check if activation worked
if [ $? -eq 0 ]; then
    echo "✅ Virtual environment activated"
else
    echo "❌ Failed to activate virtual environment"
    echo "Run: python3 -m venv venv"
    exit 1
fi

# Run Streamlit
echo "🌐 Starting Streamlit server..."
echo ""
streamlit run contract_analyzer_complete_hybrid.py
