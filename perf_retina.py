import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import re
import os

##########################################
# 1. 配置参数
##########################################
SET_NAME="retina_realtime-pixels-retina_200k"

LOG_FILE = "collected-retina-logs/" + SET_NAME + ".out"
OUTPUT_BASE = "collected-retina-logs/final/" + SET_NAME
RESAMPLE_INTERVAL = '10s' 
GI_FACTOR = 1024**3

def parse_and_plot(log_path, output_name, interval='10s'):
    # --- 2. 正则表达式配置 ---
    rdb_re = re.compile(
        r"(?P<time>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},(?P<ms>\d{3})).*?\[RocksDB Metrics\].*?"
        r"Total=.*? \((?P<rdb_total>\d+) Bytes\).*?"
        r"JVM_Heap\[Used=.*? \((?P<jvm_used>\d+) Bytes\)"
    )
    ret_re = re.compile(
        r"(?P<time>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},(?P<ms>\d{3})).*?\[Retina Metrics\].*?"
        r"CPU_Usage\[Process: (?P<cpu_proc>[\d.]+)%.*?"
        r"Retina_Mem\[Allocated: .*? \((?P<ret_allocated>\d+) Bytes\), "
        r"Tracked: (?P<ret_tracked>\d+) Bytes, "
        r"Objects: (?P<ret_objects>\d+), "
        r"Pure_Tracked: (?P<ret_pure>\d+) Bytes\]"
    )

    rows = []
    print(f"[*] 正在读取并解析日志: {log_path}")

    if not os.path.exists(log_path):
        print(f"[!] 错误: 找不到文件 {log_path}")
        return

    # --- 3. 解析逻辑 ---
    with open(log_path, 'r', encoding='utf-8') as f:
        for line in f:
            rm = rdb_re.search(line)
            if rm:
                d = rm.groupdict()
                rows.append({
                    "time": d["time"].replace(',', '.'), 
                    "rdb_total": int(d["rdb_total"]), 
                    "jvm_used": int(d["jvm_used"])
                })
                continue
            tm = ret_re.search(line)
            if tm:
                d = tm.groupdict()
                rows.append({
                    "time": d["time"].replace(',', '.'), 
                    "cpu_proc": float(d["cpu_proc"]),
                    "ret_allocated": int(d["ret_allocated"]),
                    "ret_tracked": int(d["ret_tracked"]),
                    "ret_objects": int(d["ret_objects"]),
                    "ret_pure": int(d["ret_pure"])
                })

    if not rows:
        print("[!] 错误: 未能在日志中匹配到任何数据。")
        return

    # --- 4. 数据预处理 ---
    df = pd.DataFrame(rows)
    df['time'] = pd.to_datetime(df['time'])
    df = df.set_index('time').sort_index()

    required_columns = ['rdb_total', 'jvm_used', 'ret_allocated', 'ret_tracked', 'ret_objects', 'ret_pure', 'cpu_proc']
    for col in required_columns:
        if col not in df.columns:
            df[col] = 0.0

    # 前向填充防止 Objects 被稀释
    df = df.ffill().fillna(0.0)

    # 重采样
    df_res = df.resample(interval, label='right', closed='right').mean()

    # 去掉最后一个桶
    if len(df_res) > 1:
        df_res = df_res.iloc[:-1]

    # 插入 T=0 原点
    start_time = df_res.index[0] - pd.Timedelta(interval)
    df_zero = pd.DataFrame(0.0, index=[start_time], columns=df_res.columns)
    
    # 特殊处理 CPU：T=0 时刻的 CPU 不应该为 0，应与第一个桶保持一致
    df_zero['cpu_proc'] = df_res['cpu_proc'].iloc[0]
    
    df_res = pd.concat([df_zero, df_res])

    # 线性插值
    df_res = df_res.interpolate(method='linear').fillna(0.0)
    df_res['rel_sec'] = (df_res.index - df_res.index[0]).total_seconds()

# --- 5. 计算指标 ---
    df_res['L1_Pure_Retina'] = df_res['ret_pure'] / GI_FACTOR
    df_res['L2_Overhead'] = (df_res['ret_tracked'] - df_res['ret_pure']).clip(lower=0) / GI_FACTOR
    df_res['L3_Other_OffHeap'] = (df_res['ret_allocated'] - df_res['ret_tracked'] - df_res['rdb_total']).clip(lower=0) / GI_FACTOR
    df_res['L4_RocksDB'] = df_res['rdb_total'] / GI_FACTOR
    df_res['L5_JVM'] = df_res['jvm_used'] / GI_FACTOR

    # 【关键修改】：确保索引有名字，并显式指定 index_label
    df_res.index.name = 'time'
    df_res.to_csv(f"{output_name}.csv", index_label='time')

    # --- 6. 绘图 ---
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 12), sharex=True)
    
    # A. 上图：内存堆叠图 + CPU 曲线
    colors = ['#2ca02c', '#98df8a', '#bcbd22', '#ff7f0e', '#1f77b4']
    labels = ['Retina (Pure Data)', 'Monitor Overhead', 'Other Off-Heap (S3/etc.)', 'RocksDB Native', 'JVM Heap']

    # 堆叠图
    ax1.stackplot(df_res['rel_sec'], 
                  df_res['L1_Pure_Retina'], 
                  df_res['L2_Overhead'], 
                  df_res['L3_Other_OffHeap'],
                  df_res['L4_RocksDB'],
                  df_res['L5_JVM'],
                  labels=labels, colors=colors, alpha=0.85)

    # 叠加 CPU (右侧轴)
    ax1_cpu = ax1.twinx()
    ax1_cpu.plot(df_res['rel_sec'], df_res['cpu_proc'], color='#d62728', linestyle='--', linewidth=1.5, label='CPU Process %')
    ax1_cpu.set_ylabel("CPU Usage (%)", color='#d62728', fontsize=12)
    ax1_cpu.tick_params(axis='y', labelcolor='#d62728')
    ax1_cpu.set_ylim(0, max(df_res['cpu_proc'].max() * 1.2, 100))

    ax1.set_title(f"Retina Full-Stack Analysis (Resampled: {interval} Mean)", fontsize=14, fontweight='bold')
    ax1.set_ylabel("Memory Usage (GiB)", fontsize=12)
    # 合并图例
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax1_cpu.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', frameon=True, ncol=2)
    
    ax1.grid(True, linestyle=':', alpha=0.6)
    ax1.yaxis.set_major_formatter(ticker.FormatStrFormatter('%.1f GiB'))
    ax1.set_ylim(0, None)
    ax1.set_xlim(0, None)

    # B. 下图：Object 数量
    ax2.plot(df_res['rel_sec'], df_res['ret_objects'], color='#9467bd', linewidth=2, label='Retina Objects')
    ax2.fill_between(df_res['rel_sec'], df_res['ret_objects'], color='#9467bd', alpha=0.15)
    ax2.set_xlabel("Time from Start (seconds)", fontsize=12)
    ax2.set_ylabel("Active Objects Count", fontsize=12)
    ax2.legend(loc='upper left')
    ax2.grid(True, linestyle=':', alpha=0.6)
    ax2.yaxis.set_major_formatter(ticker.EngFormatter())
    ax2.set_ylim(0, None)

    plt.tight_layout()
    plt.savefig(f"{output_name}.png", dpi=300)
    print(f"[+] 脚本运行成功！图1已包含内存堆叠与CPU趋势。")
    plt.show()

if __name__ == "__main__":
    parse_and_plot(LOG_FILE, OUTPUT_BASE, RESAMPLE_INTERVAL)
