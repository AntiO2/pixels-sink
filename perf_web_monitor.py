from flask import Flask, render_template, jsonify, request, abort
import pandas as pd
import os
from functools import lru_cache
from time import time

# Configuration
DATA_DIR = "/home/ubuntu/pixels-sink/result1k2"
# DATA_DIR = "/home/antio2/projects/pixels-sink/tmp"
PORT = 8083
CACHE_TTL = 5  # seconds

app = Flask(__name__, template_folder='develop')

# ---- 1. File Reading Cache (Reduces frequent I/O) ----
@lru_cache(maxsize=64)
def _read_csv_cached(path, mtime):
    """Read CSV with simple caching by modification time."""
    df = pd.read_csv(path, names=["time", "rows", "txns", "debezium", "serdRows", "serdTxs", "last"])
    # Take only the last 300 records for real-time display
    df = df.tail(300)

    # Drop rows where all numeric values are zero
    mask = (df.drop(columns=["time"]).sum(axis=1) != 0)
    df = df.loc[mask]

    return {col: df[col].tolist() for col in df.columns}

def read_csv_with_cache(path):
    """Wrapper that invalidates cache when file modification time changes."""
    try:
        mtime = os.path.getmtime(path)
        return _read_csv_cached(path, mtime)
    except Exception as e:
        print(f"[WARN] Failed to read {path}: {e}")
        return None

# ---- 2. Main Index Page ----
@app.route('/')
def index():
    return render_template('index.html')

# ---- 3. CSV File List ----
@app.route('/list')
def list_csv():
    """Returns a list of all CSV files in the data directory."""
    try:
        files = [
            f for f in os.listdir(DATA_DIR)
            if f.endswith(".csv") and os.path.isfile(os.path.join(DATA_DIR, f))
        ]
        return jsonify(sorted(files))
    except Exception as e:
        print(f"[ERROR] list_csv failed: {e}")
        return jsonify([])

# ---- 4. Data Retrieval for Specific File ----
@app.route('/data/<filename>')
def get_data_file(filename):
    print(f"Requesting data for: {filename}")
    path = os.path.join(DATA_DIR, filename)

    if not os.path.exists(path):
        abort(404, description=f"File {filename} not found")

    data = read_csv_with_cache(path)
    if not data:
        print("File is empty or could not be processed")
        return jsonify({})  # Return empty object if file processing fails
    return jsonify(data)

# ---- 5. Simple Health Check ----
@app.route('/health')
def health():
    return jsonify({"status": "ok", "time": time()})

# ---- 6. Server Startup ----
if __name__ == '__main__':
    # Ensure the data directory exists
    os.makedirs(DATA_DIR, exist_ok=True)
    # Run server on all interfaces at the configured port
    app.run(host='0.0.0.0', port=PORT, debug=False)
