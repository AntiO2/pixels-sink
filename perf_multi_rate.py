import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import os
from datetime import datetime, date

##########################################
# Configuration: Labels and Base Directory
##########################################
# csv_labels = {
#     "1 Node": "nodes_1_rate_2.csv",
#     "2 Nodes": "nodes_2_rate_2.csv",
#     "4 Nodes": "nodes_4_rate_2.csv",
#     "8 Nodes": "nodes_8_rate_2.csv",
#     # "16 Nodes 8WB": "nodes_16_rate_2.csv",
#     # "16 Nodes 16WB": "nodes_16_rate_3.csv",
#     # "16 Nodes": "nodes_16_rate_4.csv",
#     "16 Nodes": "nodes_16_rate_16c.csv",

# }

csv_labels = {
    # "4 Node 6 Client": "test_rate_4nodes_6c.csv",
    "100k": "test_rate_1n16_1c_batch50.csv",
    "120k": "rate_1n16_120k_batch500.csv",
    "160k 2Client": "rate_1n16c2_160k_batch500.csv",
    "180k 3Client": "rate_1n16c3_180k_batch500.csv",
    "200k 2Client": "rate_1n16c2_200k_batch500.csv",
    "240k 3Client": "rate_1n16c3_240k_batch500.csv",
}

csv_labels = {
    "1Node" : "rate_1n16_120k_batch500.csv",
    "2Nodes": "rate_2n162c_250k.csv",
    "4Nodes": "rate_4n162c_500k.csv",
    "8Nodes": "rate_8n164c.csv",
    # "16Nodes": "rate_16n168c_250k.csv",
    # "16Nodes_2": "rate_16n168c.csv",
    # "16Nodes_2": "rate_16n168c_180k.csv",
    "16Nodes": "rate_16n168c_500k.csv"
}
LOG_BASE_DIR = "collected-logs"
# Added "interval_sec" to handle the precise delta time from Java logs
COL_NAMES_NEW = ["time", "rows", "txns", "debezium", "serdRows", "serdTxs", "interval_sec"]
NUMERIC_COLS = ["rows", "txns", "debezium", "serdRows", "serdTxs"]
PLOT_COL = "rows" # This represents 'rows' ops

MAX_SECONDS = 1800
SKIP_SECONDS = 30
BIN_SECONDS = 5

data = {}



def export_scalability_matrix(data_dict, output_file="scalability_results.csv"):
    """
    将处理后的 data 对象转换为按列排列的 CSV (1Node, 2Nodes, 4Nodes...)
    """
    # 1. 自动识别并排序节点维度 (1, 2, 4, 8, 16)
    # 通过正则表达式从 label 中提取数字
    def get_node_count(label):
        import re
        nums = re.findall(r'\d+', label)
        return int(nums[0]) if nums else 999

    sorted_labels = sorted(data_dict.keys(), key=get_node_count)
    
    # 2. 对齐各实验的样本行数 (取各组数据的最小行数，保证矩阵对齐)
    min_len = min([len(data_dict[label][PLOT_COL]) for label in sorted_labels])
    print(f"\n[Export] 对齐样本行数: {min_len}")

    # 3. 构造结果矩阵
    matrix_data = {}
    for label in sorted_labels:
        # 获取该实验下的吞吐量序列
        series = data_dict[label][PLOT_COL].dropna().values
        # 只截取 min_len 长度
        matrix_data[label] = series[:min_len].astype(int)

    # 4. 转换为 DataFrame 并导出
    df_result = pd.DataFrame(matrix_data)
    
    # 按照您要求的格式重命名列头 (1node, 2node...)
    rename_map = {lbl: f"{get_node_count(lbl)}node" for lbl in sorted_labels}
    df_result = df_result.rename(columns=rename_map)

    # 导出
    df_result.to_csv(output_file, index=False)
    
    print(f"--- 性能数据矩阵 (前5行) ---")
    print(df_result.head(5))
    print(f"---------------------------")
    print(f"CSV 文件已生成: {output_file}")

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

##########################################
# Plot 3: Scalability (Using Filtered Data)
##########################################
plt.figure(figsize=(10, 6))

# 1. 提取并解析节点数
node_results = []
for label, df in data.items():
    # 从 label (如 "16 Nodes") 中提取数字 16
    node_num = int(''.join(filter(str.isdigit, label)))
    # 使用你过滤并重采样后的数据列
    series = df[PLOT_COL].dropna()
    node_results.append((node_num, series.values))

# 2. 排序确保连线正确
node_results.sort(key=lambda x: x[0])
sorted_nodes = [x[0] for x in node_results]
sorted_values = [x[1] for x in node_results]
means = [np.mean(v) for v in sorted_values]

# 3. 绘图
box = plt.boxplot(sorted_values, positions=sorted_nodes, widths=np.array(sorted_nodes) * 0.2)

# 绘制均值趋势线
plt.plot(sorted_nodes, means, marker='o', markersize=8, linestyle='-', 
         linewidth=2, label='Mean Throughput (Filtered)', color='#1f77b4')

# 4. 坐标轴与美化 (使用 log2 比例展示扩展性)
plt.xscale('log', base=2)
plt.xticks(sorted_nodes, labels=[f"{n}N" for n in sorted_nodes])
# plt.yscale('log') # 吞吐量通常用 log 轴看线性增长斜率

# 格式化 Y 轴（取消科学计数法，或者使用你之前的 100k 逻辑）
plt.gca().yaxis.set_major_formatter(ticker.ScalarFormatter())
plt.ticklabel_format(style='plain', axis='y')

plt.xlabel("Cluster Size (Nodes, log2 scale)")
plt.ylabel(f"Filtered {PLOT_COL} (Ops/s, {BIN_SECONDS}s bin)")
plt.title(f"Throughput Scalability: {PLOT_COL} (Filtered Data)")
plt.grid(True, which="both", linestyle='--', alpha=0.5)
plt.legend()
plt.tight_layout()

plt.savefig(f"cluster_scalability_{PLOT_COL}_final.png")

export_scalability_matrix(data)