import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, date

##########################################
# 配置 CSV 文件 和 标签
##########################################
csv_files = {
    # "10k": "resulti7i/10k_rate_2.csv",
    # "20k": "resulti7i/20k_rate_2.csv",
    # "30k": "resulti7i/30k_rate_2.csv",
    # "40k": "resulti7i/40k_rate_2.csv",
    # "50k": "resulti7i/50k_rate.csv",
    # "60k": "resulti7i/60k_rate_2.csv",
    # "80k": "resulti7i/80k_rate_2.csv",
    "100k": "resulti7i_100/100k_rate.csv",
}

COL_NAMES = ["time", "rows", "txns", "debezium", "serdRows", "serdTxs"]
PLOT_COL = "rows"

MAX_SECONDS = 1800          
SKIP_SECONDS = 10            
BIN_SECONDS = 60            

##########################################
# 加载并处理数据
##########################################
data = {}
for label, path in csv_files.items():
    print(f"正在处理: {label} -> {path}")
    
    # 1. 加载数据
    df = pd.read_csv(path, header=None, names=COL_NAMES, sep=',')

    # 2. 【核心修改】处理时间戳并跳过格式不对的行
    # errors='coerce' 会将无法解析的格式转为 NaT
    df["ts"] = pd.to_datetime(df["time"], format="%H:%M:%S", errors='coerce')
    
    # 剔除无法解析时间的行 (NaT)
    initial_count = len(df)
    df = df.dropna(subset=["ts"]).copy()
    if len(df) < initial_count:
        print(f"  注意: 跳过了 {initial_count - len(df)} 行格式不正确的数据")

    # 合并日期
    df["ts"] = df["ts"].dt.time.apply(lambda x: datetime.combine(date.today(), x))

    # 3. 计算相对时间
    df = df.sort_values("ts") # 确保时间有序
    t0 = df["ts"].iloc[0]
    df["sec"] = (df["ts"] - t0).dt.total_seconds()

    # 4. 过滤时间范围
    df = df[df["sec"] >= SKIP_SECONDS].copy()
    if df.empty:
        print(f"  警告: {label} 在跳过 {SKIP_SECONDS}s 后没有剩余数据")
        continue

    t_new0 = df["ts"].iloc[0]
    df["sec"] = (df["ts"] - t_new0).dt.total_seconds()
    df = df[df["sec"] <= MAX_SECONDS]

    # 5. 【核心修改】重采样聚合
    # 设置索引前，先确保 PLOT_COL 等数值列是 numeric 类型
    # 这样可以防止其他列中混入字符串导致 mean() 失败
    for col in ["rows", "txns", "debezium", "serdRows", "serdTxs"]:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df.set_index("ts")
    
    # 只对数值列进行 mean 运算，忽略字符串列（如 time 列）
    df_bin = df.resample(f"{BIN_SECONDS}s").mean(numeric_only=True).reset_index()

    # 6. 对齐横轴
    if not df_bin.empty:
        df_bin["bin_sec"] = (df_bin["ts"] - df_bin["ts"].iloc[0]).dt.total_seconds()
        data[label] = df_bin

##########################################
# 图 1：时间序列波动
##########################################
plt.figure(figsize=(10, 5))
for label, df in data.items():
    plt.plot(df["bin_sec"], df[PLOT_COL], label=label)

plt.xlabel("Time (sec)")
plt.ylabel(f"{PLOT_COL} ({BIN_SECONDS}s average)")
plt.title(f"{PLOT_COL} Over Time ({BIN_SECONDS}s Avg)")
plt.legend()
plt.grid(True, linestyle='--', alpha=0.7)
plt.tight_layout()
plt.savefig(f"rate_{PLOT_COL}_over_time_variable_bin.png")
plt.close()

##########################################
# 图 2：CDF
##########################################
plt.figure(figsize=(10, 5))
for label, df in data.items():
    vals = np.sort(df[PLOT_COL].dropna())
    if len(vals) > 0:
        y = np.linspace(0, 1, len(vals))
        plt.plot(vals, y, label=label)

plt.xlabel(f"{PLOT_COL} ({BIN_SECONDS}s average)")
plt.ylabel("CDF")
plt.title(f"{PLOT_COL} CDF Distribution")
plt.legend()
plt.grid(True, linestyle='--', alpha=0.7)
plt.tight_layout()
plt.savefig(f"rate_{PLOT_COL}_cdf_variable_bin.png")
plt.close()

print(f"\n全部完成! 图已生成。")