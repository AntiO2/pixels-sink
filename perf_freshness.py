import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

##########################################
# 配置 CSV 文件 和 标签
##########################################
csv_files = {
    "10k": "resulti7i/10k_freshness.csv",
    "20k": "resulti7i/20k_freshness.csv",
    "30k": "resulti7i/30k_freshness.csv",
    "40k": "resulti7i/40k_freshness.csv",
    # "40k": "tmp/freshness40k_5.csv",
    # "50k": "tmp/freshness50k_5.csv",
    # "60k": "tmp/freshness60k_5.csv"
}
# csv_files = {
#     "Query Transaction": "tmp/i7i_2k_dec_freshness.csv",
#     "Query Record": "tmp/i7i_2k_record_dec_freshness.csv",
#     "Internal Transaction Context": "tmp/i7i_2k_txn_dec_freshness.csv",
#     "Query Selected Table, Trans Mode": "tmp/i7i_2k_batchtest_dec_freshness_2.csv"
# }
MAX_SECONDS = 2000         # 截取前多少秒的数据
SKIP_SECONDS = 10            # 跳过前多少秒的数据（可调）
BIN_SECONDS = 10            # 平均窗口（秒）
MAX_FRESHNESS = 5000         # 过滤初始warmup时的无用数据
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

    # 跳过前 SKIP_SECONDS 秒
    df = df[df["sec"] >= SKIP_SECONDS]

    df = df[df["freshness"] <= MAX_FRESHNESS]

    # 重新计算时间（所有曲线从 0 秒开始对齐）
    t_new0 = df["ts"].iloc[0]
    df["sec"] = (df["ts"] - t_new0).dt.total_seconds()

    # 只取前 MAX_SECONDS 秒
    df = df[df["sec"] <= MAX_SECONDS]

    # 可调平均窗口采样
    df_bin = df.resample(f"{BIN_SECONDS}s", on="ts").mean().reset_index()

    # 对齐横轴（时间序列）
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
# plt.yscale("log")
plt.title(
    f"Freshness Over Time ({BIN_SECONDS}-Second Avg, "
    f"Skip {SKIP_SECONDS}s, First {MAX_SECONDS}s)"
)
plt.legend()
plt.tight_layout()
plt.savefig("freshness_over_time_variable_bin.png")
plt.close()


##########################################
# 图 2：CDF（同样使用平均窗口后的数据）
##########################################
plt.figure(figsize=(10, 5))

for label, df in data.items():
    vals = np.sort(df["freshness"].dropna())
    y = np.linspace(0, 1, len(vals))
    plt.plot(vals, y, label=label)

plt.xscale("log")
plt.xlabel(f"Freshness (ms, {BIN_SECONDS}s average)")
plt.ylabel("CDF")
plt.title(
    f"Freshness CDF Distribution ({BIN_SECONDS}-Second Sampled, Skip {SKIP_SECONDS}s)"
)
plt.legend()
plt.tight_layout()
plt.savefig("freshness_cdf_variable_bin.png")
plt.close()

print("图已生成: freshness_over_time_variable_bin.png, freshness_cdf_variable_bin.png")
