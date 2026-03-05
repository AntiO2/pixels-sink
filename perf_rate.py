import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os
from datetime import datetime, date

##########################################
# Configuration: CSV Files and Labels
##########################################
# csv_files = {
#     # "10k": "resulti7i/10k_rate_2.csv",
#     # "20k": "resulti7i/20k_rate_2.csv",
#     # "30k": "resulti7i/30k_rate_2.csv",
#     # "40k": "resulti7i/40k_rate_2.csv",
#     # "50k": "resulti7i/50k_rate.csv",
#     # "60k": "resulti7i/60k_rate_2.csv",
#     # "80k": "resulti7i/80k_rate_2.csv",
#     # "100k": "resulti7i_100/100k_rate.csv",
#     "100k": "result1k2/test_rate_1n16_1c_batch50.csv",
# }

csv_files = {
    "64": {
        "rate": "result_ablation/rate_64size.csv",
        # "fresh": "result_ablation/fresh_64size.csv",
    },
    "128": {
        "rate": "result_ablation/rate_128size.csv",
        # "fresh": "result_ablation/fresh_128size.csv",
    },
    "256": {
        "rate": "result_ablation/rate_256size.csv",
        # "fresh": "result_ablation/fresh_256size.csv",
    },
    "512": {
        "rate": "result_ablation/rate_512size.csv",
        # "fresh": "result_ablation/fresh_512size.csv",
    },
    "1024": {
        "rate":  "result_ablation/rate_1024_2size.csv",
        "fresh": "result_ablation/fresh_1024_2size.csv",
    },
    "2048": {
        "rate": "result_ablation/rate_2048size.csv",
        # "fresh": "result_ablation/fresh_2048size.csv",
    },
    "4096": {
        "rate": "result_ablation/rate_4096size.csv",
        # "fresh": "result_ablation/fresh_4096size.csv",
    },
    "8192": {
        "rate": "result_ablation/rate_8192size.csv",
        # "fresh": "result_ablation/fresh_8192size.csv",
    },
    "16384": {
        "rate":  "result_ablation/rate_16384size.csv",
        "fresh": "result_ablation/fresh_16384size.csv",
    },
}

csv_files = {
    "200": {
        "rate": "result_res/rate_200k.csv",
        "freshness": "result_res/fresh_200k.csv"
    }
}

csv_files = {
    "64": {
        "rate": "result1k2_2/ablation/rate_64size.csv",
        "fresh": "result1k2_2/ablation/fresh_64size.csv",
    },
    "128": {
        "rate": "result1k2_2/ablation/rate_128size.csv",
        "fresh": "result1k2_2/ablation/fresh_128size.csv",
    },
    "256": {
        "rate": "result1k2_2/ablation/rate_256size.csv",
        "fresh": "result1k2_2/ablation/fresh_256size.csv",
    },
    "512": {
        "rate": "result1k2_2/ablation/rate_512size.csv",
        "fresh": "result1k2_2/ablation/fresh_512size.csv",
    },
    "1024": {
        "rate": "result1k2_2/ablation/rate_1024size.csv",
        "fresh": "result1k2_2/ablation/fresh_1024size.csv",
    },
    "2048": {
        "rate": "result1k2_2/ablation/rate_2048size.csv",
        "fresh": "result1k2_2/ablation/fresh_2048.size.csv", # 注意：ls 结果此处有 .size
    },
    "4096": {
        "rate": "result1k2_2/ablation/rate_4096size.csv",
        "fresh": "result1k2_2/ablation/fresh_4096.size.csv", # 注意：ls 结果此处有 .size
    },
    "8192": {
        "rate": "result1k2_2/ablation/rate_8192size_2.csv",
        "fresh": "result1k2_2/ablation/fresh_8192.size.csv", # 注意：ls 结果此处有 .size
    },
    "16384": {
        "rate": "result1k2_2/ablation/rate_16384size_2.csv",
        "fresh": "result1k2_2/ablation/fresh_16384size.csv",
    },
}

COL_NAMES = ["time", "rows", "txns", "debezium", "serdRows", "serdTxs", "lastTime"]
PLOT_COL = "rows"

MAX_SECONDS = 1800
SKIP_SECONDS = 100
BIN_SECONDS = 2

##########################################
# Load & process RATE data
##########################################

rate_ts_data = {}
rate_box_data = {}

for label, files in csv_files.items():
    rate_path = files["rate"]

    df = pd.read_csv(rate_path, header=None, names=COL_NAMES)

    df["ts"] = pd.to_datetime(df["time"], format="%H:%M:%S", errors="coerce")
    df = df.dropna(subset=["ts"]).copy()
    df["ts"] = df["ts"].dt.time.apply(
        lambda x: datetime.combine(date.today(), x)
    )

    df = df.sort_values("ts")
    t0 = df["ts"].iloc[0]
    df["sec"] = (df["ts"] - t0).dt.total_seconds()

    df = df[df["sec"] >= SKIP_SECONDS].copy()
    if df.empty:
        continue

    t1 = df["ts"].iloc[0]
    df["sec"] = (df["ts"] - t1).dt.total_seconds()
    df = df[df["sec"] <= MAX_SECONDS]

    for col in ["rows", "txns", "debezium", "serdRows", "serdTxs"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.set_index("ts")
    df_bin = df.resample(f"{BIN_SECONDS}s").mean(numeric_only=True).reset_index()

    if not df_bin.empty:
        df_bin["bin_sec"] = (
            df_bin["ts"] - df_bin["ts"].iloc[0]
        ).dt.total_seconds()
        rate_ts_data[label] = df_bin
        rate_box_data[label] = df_bin[PLOT_COL].dropna()

##########################################
# Load FRESHNESS data
##########################################

fresh_val_data = {}

for label, files in csv_files.items():
    # 兼容 freshness 和 fresh 两种配置键名
    fresh_path = files.get("freshness") or files.get("fresh")
    if fresh_path is None or not os.path.exists(fresh_path):
        continue

    # 根据你的说明：第二列是 freshness (新鲜度)，第三列是 query_time
    df = pd.read_csv(
        fresh_path,
        header=None,
        names=["ts", "freshness_val", "query_time"]
    )

    # 转换第二列数据
    df["freshness_val"] = pd.to_numeric(df["freshness_val"], errors="coerce")
    df = df.dropna(subset=["freshness_val"])

    if not df.empty:
        # 存储第二列的新鲜度数据
        fresh_val_data[label] = df["freshness_val"]

##########################################
# Plot 1: Rate over time
##########################################

plt.figure(figsize=(10, 5))

for label, df in rate_ts_data.items():
    plt.plot(df["bin_sec"], df[PLOT_COL], label=label)

plt.xlabel("Time (sec)")
plt.ylabel(f"Throughput ({BIN_SECONDS}s average)")
plt.title("Throughput Over Time")
plt.legend(title="Rate")
plt.grid(True, linestyle="--", alpha=0.6)

plt.tight_layout()
plt.savefig("rate_over_time.png")
plt.close()

##########################################
# Plot 2: Throughput Boxplot (Standalone)
##########################################

sorted_labels = sorted(rate_box_data.keys(), key=lambda x: int(x))
x_indices = np.arange(len(sorted_labels))

fig, ax1 = plt.subplots(figsize=(10, 6))

rate_boxes = [rate_box_data[k] for k in sorted_labels]
rate_means = [rate_box_data[k].mean() for k in sorted_labels]

box_rate = ax1.boxplot(
    rate_boxes,
    positions=x_indices,
    widths=0.4,
    patch_artist=True,
    showmeans=True
)
for patch in box_rate["boxes"]:
    patch.set_facecolor("skyblue")
    patch.set_alpha(0.7)

ax1.plot(x_indices, rate_means, color="blue", marker="o", linestyle="-", linewidth=2, label="Throughput Mean")

ax1.set_xticks(x_indices)
ax1.set_xticklabels(sorted_labels)
ax1.set_xlabel("Block Size")
ax1.set_ylabel("Throughput (rows / s)")
ax1.set_title("Throughput Distribution vs Block Size")
ax1.grid(True, axis="y", linestyle="--", alpha=0.5)
ax1.legend(loc="upper left")

plt.tight_layout()
plt.savefig("boxplot_throughput.png")
plt.close()

##########################################
# Plot 3: Freshness Boxplot (Standalone)
##########################################

# 仅绘制存在新鲜度数据的标签
fresh_labels = [l for l in sorted_labels if l in fresh_val_data]

if fresh_labels:
    fig, ax2 = plt.subplots(figsize=(10, 6))
    
    f_x_indices = np.arange(len(fresh_labels))
    # 提取第二列的新鲜度数值
    fresh_boxes = [fresh_val_data[k] for k in fresh_labels]
    fresh_means = [fresh_val_data[k].mean() for k in fresh_labels]

    box_fresh = ax2.boxplot(
        fresh_boxes,
        positions=f_x_indices,
        widths=0.4,
        patch_artist=True,
        showmeans=True
    )
    for patch in box_fresh["boxes"]:
        patch.set_facecolor("salmon") # 使用不同颜色区分新鲜度
        patch.set_alpha(0.7)
    
    ax2.plot(f_x_indices, fresh_means, color="red", marker="D", linestyle="-", linewidth=2, label="Freshness Mean")

    ax2.set_xticks(f_x_indices)
    ax2.set_xticklabels(fresh_labels)
    ax2.set_xlabel("Block Size")
    ax2.set_ylabel("Freshness (Value)")
    ax2.set_title("Freshness (Column 2) Distribution vs Block Size")
    ax2.grid(True, axis="y", linestyle="--", alpha=0.5)
    ax2.legend(loc="upper left")

    plt.tight_layout()
    plt.savefig("boxplot_freshness_value.png")
    plt.show()
    plt.close()

##########################################
# Export: Preprocessed Rate Data (Transposed)
##########################################

# 1. 按照 int 顺序排序标签
sorted_export_labels = sorted(rate_box_data.keys(), key=lambda x: int(x))

# 2. 提取数据列
export_series = []
for label in sorted_export_labels:
    s = rate_box_data[label].reset_index(drop=True)
    s.name = label
    export_series.append(s)

# 3. 合并
df_rate_transposed = pd.concat(export_series, axis=1)

# 4. 保存
export_filename = "rate_preprocessed_transposed.csv"
df_rate_transposed.to_csv(export_filename, index=False)

print(f"\n✅ 任务完成：")
print(f"1. 吞吐量趋势图 -> rate_over_time.png")
print(f"2. 吞吐量箱线图 -> boxplot_throughput.png")
print(f"3. 新鲜度(第二列)箱线图 -> boxplot_freshness_value.png")