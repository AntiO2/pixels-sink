import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

##########################################
# Configuration: CSV Files and Labels
##########################################
# csv_files = {
#     "10k": "result100/fresh_1n4_10k_2.csv",
#     "20k": "result100/fresh_1n4_20k_2.csv",
#     "40k": "result100/fresh_1n4_40k.csv",
#     "80k": "result100/fresh_1n4_80k.csv",
#     "120k": "result100/fresh_1n4_120k_2.csv",
#     # "150k": "result100/nouse_fresh_1n4_150k.csv",
# }

csv_files = {
    "10k": "result100_2/fresh_10k.csv",
    "20k": "result100_2/fresh_20k.csv",
    "40k": "result100_2/fresh_40k.csv",
    "80k": "result100_2/fresh_80k.csv",
    "100k": "result100_2/fresh_100k.csv",
    "120k": "result100_2/fresh_120k.csv",
    "160k": "result100_2/fresh_160k.csv",
    "200k": "result100_2/fresh_200k.csv",
}

csv_files = {
    "10k": "result_ch/fresh_10K.csv",
    "20k": "result_ch/fresh_20K.csv",
    "40k": "result_ch/fresh_40K.csv",
    "80k": "result_ch/fresh_80K.csv",
    "85k": "result_ch/fresh_85K.csv"
}
# csv_files = {
#     "Query Transaction": "tmp/i7i_2k_dec_freshness.csv",
#     "Query Record": "tmp/i7i_2k_record_dec_freshness.csv",
#     "Internal Transaction Context": "tmp/i7i_2k_txn_dec_freshness.csv",
#     "Query Selected Table, Trans Mode": "tmp/i7i_2k_batchtest_dec_freshness_2.csv"
# }


csv_files = {
    "200": "result_lance/fresh_bucket1_batch1000.csv",
    "300": "result_lance/fresh_bucket4_batch1000.csv"
}

MAX_SECONDS = 30000           # Capture data for the first N seconds
SKIP_SECONDS = 10            # Skip the first N seconds (adjustable)
BIN_SECONDS = 10            # Average window (seconds)
MAX_FRESHNESS = 500000       # Filter out useless data during initial warmup

##########################################
# Data Loading and Processing
##########################################
data = {}
data_raw_filtered = {} # 新增：用于存储未被 resample 的原始过滤数据

for label, path in csv_files.items():
    df_full = pd.read_csv(path, header=None)
    
    df = pd.DataFrame()
    df["ts"] = pd.to_datetime(df_full.iloc[:, 0], unit="ms")
    df["freshness"] = df_full.iloc[:, 1] # 始终取第 2 列

    # 1. 基础过滤逻辑
    t0 = df["ts"].iloc[0]
    df["sec"] = (df["ts"] - t0).dt.total_seconds()
    
    mask = (df["sec"] >= SKIP_SECONDS) & \
           (df["sec"] <= MAX_SECONDS) & \
           (df["freshness"] <= MAX_FRESHNESS)
    df = df[mask].copy()

    # 对齐时间轴起点为 0
    if not df.empty:
        t_new0 = df["ts"].iloc[0]
        df["sec"] = (df["ts"] - t_new0).dt.total_seconds()

        # 存储原始过滤后的数据，用于 CDF 绘图
        data_raw_filtered[label] = df["freshness"].copy()

        # 2. Resample 仅用于 Plot 1 (Time Series)
        df_bin = df.resample(f"{BIN_SECONDS}s", on="ts").mean().reset_index()
        df_bin["bin_sec"] = (df_bin["ts"] - df_bin["ts"].iloc[0]).dt.total_seconds()
        data[label] = df_bin

##########################################
# Plot 1: (保持不变) Smoothed Time Series
##########################################
# ... (此处省略 Plot 1 的代码，逻辑与你之前的一致) ...

##########################################
# Plot 2: Inverted CDF (使用原始数据 data_raw_filtered)
##########################################
plt.figure(figsize=(10, 5))

for label, raw_vals in data_raw_filtered.items():
    # 使用未经过 resample 的原始数据点
    vals = np.sort(raw_vals.dropna())
    prob = np.linspace(0, 1, len(vals))

    plt.plot(prob, vals, label=label)

plt.xticks(np.arange(0, 1.1, 0.1))
plt.xlim(0, 1)
plt.xlabel("CDF (Probability)")
plt.ylabel("Freshness (ms, Raw Data)") # 更新 Label 强调是原始数据
plt.title(
    f"Inverted Freshness CDF\n(Raw Data Points, Skip {SKIP_SECONDS}s)"
)

plt.grid(True, which="both", ls="-", alpha=0.3)
plt.legend()
plt.tight_layout()
plt.savefig("freshness_cdf_raw_fixed_ticks.png") # 建议改名以区分
plt.close()

##########################################
# Data Export: Export Raw Filtered Data
##########################################
raw_series_list = []

for label, path in csv_files.items():
    # 1. 读取所有列
    df_raw = pd.read_csv(path, header=None)
    
    # 2. 核心兼容逻辑：始终取第 2 列（索引为 1）作为 freshness
    df_processed = pd.DataFrame()
    df_processed["ts"] = pd.to_datetime(df_raw.iloc[:, 0], unit="ms")
    
    # 关键修改：iloc[:, 1] 确保取的是中间那一列 (freshness)
    # 即使后面有第三列 (query time)，也会被忽略
    df_processed["freshness"] = df_raw.iloc[:, 1] 
    
    # 3. 计算相对时间轴
    t0 = df_processed["ts"].iloc[0]
    df_processed["sec"] = (df_processed["ts"] - t0).dt.total_seconds()
    
    # 4. 执行过滤逻辑
    mask = (df_processed["sec"] >= SKIP_SECONDS) & \
           (df_processed["sec"] <= MAX_SECONDS) & \
           (df_processed["freshness"] <= MAX_FRESHNESS)
    
    # 5. 提取并重命名，用于横向合并
    filtered_series = df_processed.loc[mask, "freshness"].reset_index(drop=True)
    filtered_series.name = label
    raw_series_list.append(filtered_series)

# 6. 横向合并并导出
if raw_series_list:
    df_export = pd.concat(raw_series_list, axis=1)
    export_filename = "freshness_raw_filtered.csv"
    df_export.to_csv(export_filename, index=False)
    print(f"Filtered raw data exported to: {export_filename}")
