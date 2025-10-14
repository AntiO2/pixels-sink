from flask import Flask, render_template, jsonify, request
import pandas as pd
import os
from functools import lru_cache
from time import time

# Configuration
DATA_DIR = "/home/ubuntu/pixels-sink/tmp"
# DATA_DIR = "/home/antio2/projects/pixels-sink/tmp"
PORT = 8083
CACHE_TTL = 5  # seconds

app = Flask(__name__, template_folder='develop')

# ---- 1. 文件读取缓存（减少频繁IO） ----
@lru_cache(maxsize=64)
def _read_csv_cached(path, mtime):
    """Read CSV with simple caching by modification time."""
    df = pd.read_csv(path, names=["time", "rows", "txns", "debezium", "serdRows", "serdTxs"])
    df = df.tail(300)
    # drop continuous zero lines
    mask = (df.drop(columns=["time"]).sum(axis=1) != 0)
    df = df.loc[mask]
    return {col: df[col].tolist() for col in df.columns}

def read_csv_with_cache(path):
    """Wrapper that invalidates cache when file changes."""
    try:
        mtime = os.path.getmtime(path)
        return _read_csv_cached(path, mtime)
    except Exception as e:
        print(f"[WARN] Failed to read {path}: {e}")
        return None

# ---- 2. 首页 ----
@app.route('/')
def index():
    return render_template('index.html')

# ---- 3. 文件列表 ----
@app.route('/list')
def list_csv():
    try:
        files = [
            f for f in os.listdir(DATA_DIR)
            if f.endswith(".csv") and os.path.isfile(os.path.join(DATA_DIR, f))
        ]
        return jsonify(sorted(files))
    except Exception as e:
        print(f"[ERROR] list_csv failed: {e}")
        return jsonify([])

@app.route('/data/<filename>')
def get_data_file(filename):
    print(filename)
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        abort(404, description=f"File {filename} not found")

    data = read_csv_with_cache(path)
    if not data:
        print("Empty")
        return jsonify({})  # 文件为空返回空对象
    return jsonify(data)

# ---- 5. 简单健康检查 ----
@app.route('/health')
def health():
    return jsonify({"status": "ok", "time": time()})

# ---- 6. 启动 ----
if __name__ == '__main__':
    os.makedirs(DATA_DIR, exist_ok=True)
    app.run(host='0.0.0.0', port=PORT, debug=False)
