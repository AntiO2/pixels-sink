import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, date

##########################################
# 配置 CSV 文件 和 标签
##########################################
csv_files = {
    "150k_2node": "tmp/rate150k_6.csv",
}

# CSV 文件列名 (无表头)
COL_NAMES = ["time", "rows", "txns", "debezium", "serdRows", "serdTxs"]
PLOT_COL = "rows"

MAX_SECONDS = 1800          # 截取前多少秒的数据
SKIP_SECONDS = 10            # 跳过前多少秒的数据（可调）
BIN_SECONDS = 60            # 平均窗口（秒）

##########################################
# 加载并处理数据
##########################################
data = {}
for label, path in csv_files.items():
    # 假设文件是逗号分隔
    df = pd.read_csv(
        path,
        header=None,
        names=COL_NAMES,
        sep=',' # 假设是逗号分隔，如果不是请修改
    )

    # --- 时间戳处理 ---
    # 1. 解析 HH:MM:SS 时间字符串
    # 2. 由于没有日期，将其与当前日期合并，形成完整的 datetime 对象 (ts)
    df["ts"] = pd.to_datetime(df["time"], format="%H:%M:%S", errors='coerce')
    df["ts"] = df["ts"].dt.time.apply(lambda x: datetime.combine(date.today(), x))

    # 相对秒 (使用第一个时间戳作为 t0)
    t0 = df["ts"].iloc[0]
    df["sec"] = (df["ts"] - t0).dt.total_seconds()

    # 跳过前 SKIP_SECONDS 秒
    df = df[df["sec"] >= SKIP_SECONDS].copy() # 使用 .copy() 避免 SettingWithCopyWarning

    # 重新计算时间（所有曲线从 0 秒开始对齐）
    t_new0 = df["ts"].iloc[0]
    df["sec"] = (df["ts"] - t_new0).dt.total_seconds()

    # 只取前 MAX_SECONDS 秒
    df = df[df["sec"] <= MAX_SECONDS]

    # 可调平均窗口采样 (将时间序列设为索引进行重采样)
    df = df.set_index("ts")
    df_bin = df.resample(f"{BIN_SECONDS}s").mean().reset_index()

    # 对齐横轴（时间序列）
    df_bin["bin_sec"] = (df_bin["ts"] - df_bin["ts"].iloc[0]).dt.total_seconds()

    data[label] = df_bin


##########################################
# 图 1：按可调窗口采样的时间序列波动
##########################################
plt.figure(figsize=(10, 5))

for label, df in data.items():
    # 绘制选定的列
    plt.plot(df["bin_sec"], df[PLOT_COL], label=label)

plt.xlabel("Time (sec)")
plt.ylabel(f"{PLOT_COL} ({BIN_SECONDS}s average)")
# plt.yscale("log") # 绘制 "rows" 等计数时，通常不使用对数坐标
plt.title(
    f"{PLOT_COL} Over Time ({BIN_SECONDS}-Second Avg, "
    f"Skip {SKIP_SECONDS}s, First {MAX_SECONDS}s)"
)
plt.legend()
plt.grid(True, linestyle='--', alpha=0.7)
plt.tight_layout()
plt.savefig(f"{PLOT_COL}_over_time_variable_bin.png")
plt.close()


##########################################
# 图 2：CDF（同样使用平均窗口后的数据）
##########################################
plt.figure(figsize=(10, 5))

for label, df in data.items():
    # 对选定的列进行 CDF 计算
    vals = np.sort(df[PLOT_COL].dropna())
    y = np.linspace(0, 1, len(vals))
    plt.plot(vals, y, label=label)

# plt.xscale("log") # 绘制 "rows" 等计数时，通常不使用对数坐标
plt.xlabel(f"{PLOT_COL} ({BIN_SECONDS}s average)")
plt.ylabel("CDF")
plt.title(
    f"{PLOT_COL} CDF Distribution ({BIN_SECONDS}-Second Sampled, Skip {SKIP_SECONDS}s)"
)
plt.legend()
plt.grid(True, linestyle='--', alpha=0.7)
plt.tight_layout()
plt.savefig(f"{PLOT_COL}_cdf_variable_bin.png")
plt.close()

print(f"图已生成: {PLOT_COL}_over_time_variable_bin.png, {PLOT_COL}_cdf_variable_bin.png")