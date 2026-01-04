import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import os
from datetime import datetime, date

##########################################
# Configuration: Labels and Base Directory
##########################################
csv_labels = {
    "1 Node": "nodes_1_rate_2.csv",
    "2 Nodes": "nodes_2_rate_2.csv",
    "4 Nodes": "nodes_4_rate_2.csv",
    "8 Nodes": "nodes_8_rate_2.csv",
    # "16 Nodes 8WB": "nodes_16_rate_2.csv",
    # "16 Nodes 16WB": "nodes_16_rate_3.csv",
    # "16 Nodes": "nodes_16_rate_4.csv",
    "16 Nodes": "nodes_16_rate_16c.csv",
}

LOG_BASE_DIR = "collected-logs"
# Added "interval_sec" to handle the precise delta time from Java logs
COL_NAMES_NEW = ["time", "rows", "txns", "debezium", "serdRows", "serdTxs", "interval_sec"]
NUMERIC_COLS = ["rows", "txns", "debezium", "serdRows", "serdTxs"]
PLOT_COL = "rows" # This represents 'rows' ops

MAX_SECONDS = 2100
SKIP_SECONDS = 0
BIN_SECONDS = 20

data = {}

for label, filename in csv_labels.items():
    print(f"Processing Experiment: {label}")
    all_node_data = []
    
    if not os.path.exists(LOG_BASE_DIR):
        print(f"Directory {LOG_BASE_DIR} not found!")
        continue

    nodes = [d for d in os.listdir(LOG_BASE_DIR) if os.path.isdir(os.path.join(LOG_BASE_DIR, d))]
    
    for node in nodes:
        path = os.path.join(LOG_BASE_DIR, node, filename)
        if os.path.exists(path):
            print(f"  Reading node {node} -> {filename}")
            
            with open(path, 'r') as f:
                first_line = f.readline()
                col_count = len(first_line.split(','))
            
            current_cols = COL_NAMES_NEW if col_count >= 7 else COL_NAMES_NEW[:6]
            df = pd.read_csv(path, header=None, names=current_cols, sep=',')
            
            # Standardize Time
            df["ts"] = pd.to_datetime(df["time"], format="%H:%M:%S", errors='coerce')
            df = df.dropna(subset=["ts"]).copy()
            df["ts"] = df["ts"].dt.time.apply(lambda x: datetime.combine(date.today(), x))
            
            for col in NUMERIC_COLS:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            if "interval_sec" not in df.columns:
                # Calculate the difference between consecutive timestamps (Successive Diffs)
                # We sort by time first to ensure the diff is chronological
                df = df.sort_values("ts")
                
                # Use shift(-1) to look at the 'next' row's timestamp
                df["interval_sec"] = (df["ts"].shift(-1) - df["ts"]).dt.total_seconds()
                
                # For the very last row, we use the average of all previous intervals as a fallback
                mean_interval = df["interval_sec"].mean()
                if pd.isna(mean_interval) or mean_interval <= 0:
                    mean_interval = 1.0 # Absolute fallback if only one row exists
                
                df["interval_sec"] = df["interval_sec"].fillna(mean_interval)
            else:
                # If the column exists, trust the Java-side measured interval
                df["interval_sec"] = pd.to_numeric(df["interval_sec"], errors='coerce').fillna(1.0)

            # Reconstruct the absolute quantity from Ops/s
            # Since input is Ops, (Ops * actual_seconds) = Total Events in that interval
            for col in NUMERIC_COLS:
                df[f"{col}_total_events"] = df[col] * df["interval_sec"]
            
            all_node_data.append(df)

    if not all_node_data:
        continue

    combined_raw = pd.concat(all_node_data).sort_values("ts")
    t0 = combined_raw["ts"].iloc[0]
    combined_raw["sec_from_start"] = (combined_raw["ts"] - t0).dt.total_seconds()

    # Filter time range
    filtered_df = combined_raw[(combined_raw["sec_from_start"] >= SKIP_SECONDS) & 
                               (combined_raw["sec_from_start"] <= SKIP_SECONDS + MAX_SECONDS + BIN_SECONDS)].copy()
    
    if filtered_df.empty:
        continue

    # 3. Aggregation via Resampling
    # We sum the 'total_events' reconstructed from all nodes in the bin
    df_bin = filtered_df.set_index("ts").resample(
        f"{BIN_SECONDS}s", 
        origin='start'
    ).sum(numeric_only=True).reset_index()

    # --- Unit Conversion ---
    for col in NUMERIC_COLS:
        # Cluster Ops = (Sum of all events from all nodes) / (Bin Duration)
        df_bin[col] = df_bin[f"{col}_total_events"] / BIN_SECONDS
    
    # 4. Final Alignment
    df_bin["bin_sec"] = (df_bin["ts"] - df_bin["ts"].iloc[0]).dt.total_seconds()
    df_bin = df_bin[df_bin["bin_sec"] <= MAX_SECONDS]

    data[label] = df_bin
    

##########################################
# Plotting
##########################################
if not data:
    print("No data available to plot.")
    exit()

# Plot 1: Time Series
plt.figure(figsize=(12, 6))
for label, df in data.items():
    plt.plot(df["bin_sec"], df[PLOT_COL], marker='o', markersize=4, label=label)

plt.xlim(0, MAX_SECONDS)
plt.xticks(np.arange(0, MAX_SECONDS + 1, 300))
plt.ylim(bottom=0) 

# --- 关键修改：取消科学计数法 ---
# style='plain' 强制使用常规数字格式
# axis='y' 指定只针对 y 轴
plt.ticklabel_format(style='plain', axis='y')

# 如果数值非常大，还可以配合使用 ScalarFormatter 确保不出现偏移量
# plt.gca().yaxis.set_major_formatter(ticker.ScalarFormatter(useOffset=False))

plt.xlabel("Time (sec)")
# Changed label to Ops/s
plt.ylabel(f"Total Cluster {PLOT_COL} (Ops/s, {BIN_SECONDS}s bin)")
plt.title(f"Cluster Aggregate Throughput: {PLOT_COL} (Ops/s)")
plt.legend()
plt.grid(True, linestyle='--', alpha=0.7)
plt.tight_layout()
plt.savefig(f"cluster_rate_{PLOT_COL}_time.png")

# Plot 2: CDF
plt.figure(figsize=(10, 5))
for label, df in data.items():
    vals = np.sort(df[PLOT_COL].dropna())
    if len(vals) > 0:
        y = np.linspace(0, 1, len(vals))
        plt.plot(vals, y, label=label)

plt.xlim(left=0) 
plt.xlabel(f"Aggregate Cluster {PLOT_COL} (Ops/s)")
plt.ylabel("CDF")
plt.title(f"Throughput Distribution CDF: {PLOT_COL}")
plt.legend()
plt.grid(True, linestyle='--', alpha=0.7)
plt.tight_layout()
plt.savefig(f"cluster_rate_{PLOT_COL}_cdf.png")

print(f"\nProcessing Complete. Metrics plotted as Total Cluster Ops/s.")

if not data:
    print("No data available to plot.")
    exit()

# Plot 1: Time Series
plt.figure(figsize=(12, 6))
for label, df in data.items():
    # 绘图依然使用原始数据，保证趋势准确
    plt.plot(df["bin_sec"], df[PLOT_COL], marker='o', markersize=4, label=label)

# --- X-axis Scaling ---
plt.xlim(0, MAX_SECONDS)
plt.xticks(np.arange(0, MAX_SECONDS + 1, 300))

# --- Y-axis Scaling: 1, 2, 3, 4 (Units of 100k) ---

# 1. 强制 Y 轴从 0 开始
plt.ylim(bottom=0)

# 2. 定义格式化函数：将数值 (如 200000) 转换为标签 "2"
def units_of_100k(x, pos):
    return f'{int(x / 100000)}' if x != 0 else '0'

# 3. 设置主要刻度：每隔 100,000 一个刻度
plt.gca().yaxis.set_major_locator(ticker.MultipleLocator(100000))
# 4. 应用格式化
plt.gca().yaxis.set_major_formatter(ticker.FuncFormatter(units_of_100k))

# 修改 Y 轴标签，注明单位
plt.ylabel(f"Total Cluster {PLOT_COL} (x 100k Ops/s)")
plt.xlabel("Time (sec)")
plt.title(f"Cluster Aggregate Throughput ({PLOT_COL})")
plt.legend()
plt.grid(True, linestyle='--', alpha=0.7)
plt.tight_layout()
plt.savefig(f"cluster_rate_{PLOT_COL}_time_units.png")