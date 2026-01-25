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
    "10k": "result1k2/fresh_1n16_10k_3.csv",
    "40k": "result1k2/fresh_1n16_40k_3.csv",
    "80k": "result1k2/fresh_1n16_80k_3.csv",
    "100k": "result1k2/fresh_1n16_100k_3.csv",
    "120k": "result1k2/fresh_1n16_120k_3_batch200_flush_10.csv",
    "200k": "result_res/fresh_200k.csv",
    # "150k": "result1k2/fresh_1n16_150k_3.csv",
    # "150k": "result1k2/fresh_1n16_150k_2_batch200_flush_40.csv",
    # "60k": "resulti7i_100/60k_fresh.csv",
    # "100k": "resulti7i_100/100k_fresh.csv",
}
# csv_files = {
#     "Query Transaction": "tmp/i7i_2k_dec_freshness.csv",
#     "Query Record": "tmp/i7i_2k_record_dec_freshness.csv",
#     "Internal Transaction Context": "tmp/i7i_2k_txn_dec_freshness.csv",
#     "Query Selected Table, Trans Mode": "tmp/i7i_2k_batchtest_dec_freshness_2.csv"
# }

MAX_SECONDS = 3000           # Capture data for the first N seconds
SKIP_SECONDS = 10            # Skip the first N seconds (adjustable)
BIN_SECONDS = 3            # Average window (seconds)
MAX_FRESHNESS = 5000       # Filter out useless data during initial warmup
##########################################
# Data Loading and Processing
##########################################
data = {}
for label, path in csv_files.items():
    df = pd.read_csv(path, header=None, names=["ts", "freshness"])

    # Convert to datetime
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")

    # Relative seconds
    t0 = df["ts"].iloc[0]
    df["sec"] = (df["ts"] - t0).dt.total_seconds()

    # Skip initial SKIP_SECONDS
    df = df[df["sec"] >= SKIP_SECONDS]

    # Filter by max freshness threshold
    df = df[df["freshness"] <= MAX_FRESHNESS]

    # Recalculate time (align all curves to start at 0 seconds)
    t_new0 = df["ts"].iloc[0]
    df["sec"] = (df["ts"] - t_new0).dt.total_seconds()

    # Limit to MAX_SECONDS
    df = df[df["sec"] <= MAX_SECONDS]

    # Sample using an adjustable averaging window
    df_bin = df.resample(f"{BIN_SECONDS}s", on="ts").mean().reset_index()

    # Align horizontal axis (time series)
    df_bin["bin_sec"] = (df_bin["ts"] - df_bin["ts"].iloc[0]).dt.total_seconds()

    data[label] = df_bin


##########################################
# Plot 1: Smoothed/Beautified Time Series Oscillations
##########################################
# Set overall style; whitegrid looks clean and professional
sns.set_theme(style="whitegrid")

plt.figure(figsize=(12, 6)) # Slightly wider for better time trend visualization

for label, df in data.items():
    # Ensure data is sorted
    df_plot = df.sort_values("bin_sec")

    # Option A: Increase line width and anti-aliasing; use alpha for transparency to distinguish overlaps
    line, = plt.plot(
        df_plot["bin_sec"],
        df_plot["freshness"],
        label=label,
        linewidth=1.8,
        alpha=0.9,
        antialiased=True
    )


# Axis labeling and beautification
plt.xlabel("Time (sec)", fontsize=11, fontweight='bold')
plt.ylabel(f"Freshness (ms, {BIN_SECONDS}s average)", fontsize=11, fontweight='bold')

# Remove top and right spines for a cleaner look
sns.despine()

plt.title(
    f"Freshness Oscillations\n({BIN_SECONDS}s Binning, Skip {SKIP_SECONDS}s)",
    fontsize=13,
    pad=15
)

# Move legend outside or to the top right to avoid blocking curves
plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)

plt.grid(True, which="major", ls="-", alpha=0.4)
plt.tight_layout()
plt.savefig("freshness_over_time_smooth.png", dpi=300) # Save with high resolution
plt.close()


##########################################
# Plot 2: Inverted CDF (X-axis 0-1, Step 0.1)
##########################################
plt.figure(figsize=(10, 5))

for label, df in data.items():
    vals = np.sort(df["freshness"].dropna())
    prob = np.linspace(0, 1, len(vals))

    # X-axis is probability [0, 1], Y-axis is value
    plt.plot(prob, vals, label=label)

# Set X-axis ticks: from 0 to 1.1 (excluding 1.1) with step 0.1
plt.xticks(np.arange(0, 1.1, 0.1))
plt.xlim(0, 1) # Force display range between 0 and 1

# plt.yscale("log")
plt.xlabel("CDF (Probability)")
plt.ylabel(f"Freshness (ms, {BIN_SECONDS}s average)")
plt.title(
    f"Inverted Freshness CDF ({BIN_SECONDS}-Second Sampled, Skip {SKIP_SECONDS}s)"
)

plt.grid(True, which="both", ls="-", alpha=0.3)
plt.legend()
plt.tight_layout()
plt.savefig("freshness_cdf_fixed_ticks.png")
plt.close()

print("Plots generated: freshness_over_time_smooth.png, freshness_cdf_fixed_ticks.png")


##########################################
# Data Export: Export Raw Filtered Data
##########################################
# 提取每个 label 的原始过滤后数据
raw_columns = {}
for label, df in data.items():
    # 注意：这里的 data[label] 存储的是 df_bin
    # 为了获取原始数据，我们需要在过滤步骤时多存一个副本
    pass # 见下方完整集成代码

# 建议在循环处理数据时直接保存原始列
raw_series_list = []
for label, path in csv_files.items():
    df_raw = pd.read_csv(path, header=None, names=["ts", "freshness"])
    df_raw["ts"] = pd.to_datetime(df_raw["ts"], unit="ms")
    t0 = df_raw["ts"].iloc[0]
    df_raw["sec"] = (df_raw["ts"] - t0).dt.total_seconds()
    
    # 执行您的过滤逻辑
    mask = (df_raw["sec"] >= SKIP_SECONDS) & \
           (df_raw["sec"] <= MAX_SECONDS) & \
           (df_raw["freshness"] <= MAX_FRESHNESS)
    
    # 提取符合条件的原始 freshness 数据并重命名列名为 label
    filtered_series = df_raw.loc[mask, "freshness"].reset_index(drop=True)
    filtered_series.name = label
    raw_series_list.append(filtered_series)

# 横向合并数据 (由于行数可能不等，缺失值会填补为 NaN)
df_export = pd.concat(raw_series_list, axis=1)

# 保存为 CSV
export_filename = "freshness_raw_filtered.csv"
df_export.to_csv(export_filename, index=False)
print(f"Filtered raw data exported to: {export_filename}")