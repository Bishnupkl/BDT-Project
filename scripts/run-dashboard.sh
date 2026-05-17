#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Navigate to the dashboard directory
cd "$ROOT_DIR/dashboard"

# Create a virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating Python virtual environment in dashboard/venv..."
    python3 -m venv venv
fi

# Activate the virtual environment
source venv/bin/activate

# Install the required packages
echo "Installing requirements..."
pip install -r requirements.txt

# Run the Streamlit app
echo "Starting Streamlit dashboard..."
streamlit run streamlit_app.py
