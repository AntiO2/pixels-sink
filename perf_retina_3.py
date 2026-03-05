import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import glob
import os
import re
import seaborn as sns
from scipy.signal import find_peaks

def get_throughput_value(path):
    filename = os.path.basename(path)
    # 匹配文件名末尾的数字，如 _85k.csv 或 _120.csv
    match = re.search(r'_(\d+)([kK])?\.csv$', filename)
    if match:
        val = int(match.group(1))
        unit = match.group(2)
        return val * 1000 if unit else val
    return 0

def analyze_retina_stable_base(folder_path):
    all_files = glob.glob(os.path.join(folder_path, "*.csv"))
    if not all_files:
        print(f"No CSV files found in {folder_path}")
        return

    # 按吞吐量数值排序
    all_files.sort(key=get_throughput_value)
    
    sns.set_theme(style="whitegrid")
    fig, ax = plt.subplots(figsize=(18, 10))
    plt.subplots_adjust(right=0.72) 
    
    stats_summary = []

    for idx, file in enumerate(all_files):
        tp_tag = os.path.basename(file).split('_')[-1].replace('.csv', '')
        
        # 读取数据，假设列名为 rel_sec, L1_Pure_Retina, L4_RocksDB
        try:
            df = pd.read_csv(file)
        except Exception as e:
            print(f"Error reading {file}: {e}")
            continue
            
        # --- A. 时间对齐 (t=0) ---
        # 寻找 RocksDB 开始工作的第一个点
        start_condition = df['L4_RocksDB'] > 0
        if not start_condition.any(): 
            print(f"Skip {file}: L4_RocksDB always 0")
            continue
            
        t_zero_abs = df[start_condition]['rel_sec'].iloc[0]
        v_zero_val = df[start_condition]['L1_Pure_Retina'].iloc[0] * (1024**3)
        
        df['norm_sec'] = df['rel_sec'] - t_zero_abs
        
        # 截取绘图区间 (0 - 1800s)
        mask_plot = (df['norm_sec'] >= 0) & (df['norm_sec'] <= 1800)
        work_df = df[mask_plot].copy()
        if work_df.empty: continue
        
        x = work_df['norm_sec'].values
        y_bytes = work_df['L1_Pure_Retina'].values * (1024**3)
        
        # --- B. 峰谷检测 ---
        # 设定 prominence 以过滤微小噪声
        peaks, _ = find_peaks(y_bytes, distance=5, prominence=1024*1024) 
        valleys, _ = find_peaks(-y_bytes, distance=5, prominence=1024*1024)
        
        # --- C. Base 趋势线拟合 (处理无谷底情况) ---
        stable_valleys_mask = x[valleys] > 600
        vx_stable = x[valleys][stable_valleys_mask].tolist()
        vy_stable = y_bytes[valleys][stable_valleys_mask].tolist()
        
        # 构造拟合点：起点(0, v_zero) + 600s后的所有谷底
        vx_fit = [0] + vx_stable
        vy_fit = [v_zero_val] + vy_stable
        
        # 鲁棒性逻辑：如果谷底太少无法拟合
        if len(vx_fit) < 2:
            stable_mask = x > 600
            if stable_mask.any():
                # 兜底方案：直接对 600s 后的所有原始点做线性回归
                a_base, b_base = np.polyfit(x[stable_mask], y_bytes[stable_mask], 1)
            else:
                a_base, b_base = 0, v_zero_val
        else:
            # 标准方案：拟合清理后的基准水位
            a_base, b_base = np.polyfit(vx_fit, vy_fit, 1)

        # --- D. Rise 速率计算 (处理无锯齿情况) ---
        rise_slopes = []
        for v_idx in valleys[stable_valleys_mask]:
            sub_peaks = peaks[peaks > v_idx]
            if len(sub_peaks) > 0:
                p_idx = sub_peaks[0]
                dt = x[p_idx] - x[v_idx]
                dy = y_bytes[p_idx] - y_bytes[v_idx]
                if dt > 0: 
                    rise_slopes.append(dy / dt)
        
        if rise_slopes:
            avg_rise_rate = np.mean(rise_slopes)
        else:
            # 兜底方案：如果没有锯齿，Rise Rate 即为 Base 线的斜率
            avg_rise_rate = a_base if a_base > 0 else 0
        
        # --- E. 绘图与美化 ---
        line_label = f"Throughput {tp_tag}"
        line, = ax.plot(x, y_bytes, alpha=0.6, label=line_label, linewidth=1.5)
        color = line.get_color()
        
        # 绘制 Base 趋势虚线
        x_trend = np.array([0, 1800])
        ax.plot(x_trend, a_base * x_trend + b_base, color=color, linestyle='--', alpha=0.4)
        
        stats_summary.append({'label': tp_tag, 'rise': avg_rise_rate, 'base': a_base})

    # --- F. 侧边栏信息 ---
    sidebar_text = "Metric Definitions (Byte/s):\n"
    sidebar_text += "Rise: Avg Sawtooth Slope (t > 600s)\n"
    sidebar_text += "Base: Bottom Trend (t=0 & Valleys)\n"
    sidebar_text += "-"*44 + "\n"
    sidebar_text += f"{'Config':<12} | {'Rise (B/s)':<14} | {'Base (B/s)':<10}\n"
    sidebar_text += "-"*44 + "\n"
    for s in stats_summary:
        sidebar_text += f"{s['label']:<12} | {s['rise']:>14,.0f} | {s['base']:>10,.0f}\n"

    # 将统计数据放置在图表右侧
    ax.text(1.02, 0.95, sidebar_text, transform=ax.transAxes, fontsize=10,
            verticalalignment='top', family='monospace', 
            bbox=dict(boxstyle='round', facecolor='white', alpha=0.9, edgecolor='gray'))

    # 格式化 Y 轴为带逗号的数字
    ax.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, p: format(int(x), ',')))
    
    ax.set_title("Retina Memory Dynamics: Stable State Analysis (Post-600s)", fontsize=16, fontweight='bold')
    ax.set_xlabel("Seconds since RocksDB Start (t=0)", fontsize=12)
    ax.set_ylabel("Memory Usage (Bytes)", fontsize=12)
    
    # 绘制 600s 稳定阈值线
    ax.axvline(x=600, color='red', linestyle=':', alpha=0.3, label='Stable Threshold (600s)')
    
    ax.grid(True, linestyle=':', alpha=0.5)
    ax.legend(loc='upper left', fontsize='small')
    
    plt.savefig("retina_stable_throughput.png", dpi=300, bbox_inches='tight')
    print("Analysis complete. Plot saved as 'retina_stable_throughput.png'.")
    plt.show()

# 执行分析
analyze_retina_stable_base("/home/ubuntu/disk2/pixels-sink/collected-retina-logs/final/")