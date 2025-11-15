import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

##########################################
# 配置 CSV 文件 和 标签
##########################################
csv_files = {
    "10k": "tmp/freshness10k.csv",
    "20k": "tmp/freshness20k.csv",
    "30k": "tmp/freshness30k.csv",
    "50k": "tmp/freshness50k.csv",
    # "unlimit": "tmp/freshnessut.csv"
}

MAX_SECONDS = 1800          # 只取前 1800 秒
BIN_SECONDS = 40            # 可调平均窗口（秒）

##########################################
# 加载并处理数据
##########################################
data = {}
for label, path in csv_files.items():
    df = pd.read_csv(path, header=None, names=["ts", "freshness"])

    # 转为 datetime
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")

    # 相对秒
    t0 = df["ts"].iloc[0]
    df["sec"] = (df["ts"] - t0).dt.total_seconds()

    # 只取前 MAX_SECONDS 秒
    df = df[df["sec"] <= MAX_SECONDS]

    # 可调平均窗口采样
    df_bin = df.resample(f"{BIN_SECONDS}s", on="ts").mean().reset_index()

    # 时间对齐（横轴）
    df_bin["bin_sec"] = (df_bin["ts"] - df_bin["ts"].iloc[0]).dt.total_seconds()

    data[label] = df_bin


##########################################
# 图 1：按可调窗口采样的时间序列波动
##########################################
plt.figure(figsize=(10, 5))

for label, df in data.items():
    plt.plot(df["bin_sec"], df["freshness"], label=label)

plt.xlabel("Time (sec)")
plt.ylabel(f"Freshness (ms, {BIN_SECONDS}s average)")
plt.yscale("log")
plt.title(f"Freshness Over Time ({BIN_SECONDS}-Second Avg, First {MAX_SECONDS}s)")
plt.legend()
plt.tight_layout()
plt.savefig("freshness_over_time_variable_bin.png")
plt.close()


##########################################
# 图 2：CDF（使用同样的平均窗口数据）
##########################################
plt.figure(figsize=(10, 5))

for label, df in data.items():
    vals = np.sort(df["freshness"].dropna())
    y = np.linspace(0, 1, len(vals))
    plt.plot(vals, y, label=label)

plt.xlabel(f"Freshness (ms, {BIN_SECONDS}s average)")
plt.xscale("log") 
plt.ylabel("CDF")
plt.title(f"Freshness CDF Distribution ({BIN_SECONDS}-Second Sampled)")
plt.legend()
plt.tight_layout()
plt.savefig("freshness_cdf_variable_bin.png")
plt.close()

print("图已生成: freshness_over_time_variable_bin.png, freshness_cdf_variable_bin.png")
