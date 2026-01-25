import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import glob
import os
import re
from scipy.signal import find_peaks

def get_throughput_value(path):
    filename = os.path.basename(path)
    match = re.search(r'_(\d+)([kK])?\.csv$', filename)
    if match:
        val = int(match.group(1))
        unit = match.group(2)
        return val * 1000 if unit else val
    return 0

def analyze_retina_stable_base(folder_path):
    all_files = glob.glob(os.path.join(folder_path, "*.csv"))
    all_files.sort(key=get_throughput_value)
    
    fig, ax = plt.subplots(figsize=(18, 10))
    plt.subplots_adjust(right=0.72) 
    
    stats_summary = []

    for idx, file in enumerate(all_files):
        tp_tag = os.path.basename(file).split('_')[-1].replace('.csv', '')
        df = pd.read_csv(file)
        
        # --- A. 时间对齐 (t=0) ---
        start_condition = df['L4_RocksDB'] > 0
        if not start_condition.any(): continue
        t_zero_abs = df[start_condition]['rel_sec'].iloc[0]
        v_zero_val = df[start_condition]['L1_Pure_Retina'].iloc[0] * (1024**3)
        
        df['norm_sec'] = df['rel_sec'] - t_zero_abs
        
        # 绘图区间
        mask_plot = (df['norm_sec'] >= 0) & (df['norm_sec'] <= 1800)
        work_df = df[mask_plot].copy()
        x = work_df['norm_sec'].values
        y_bytes = work_df['L1_Pure_Retina'].values * (1024**3)
        
        # --- B. 峰谷检测 (用于筛选稳定期) ---
        peaks, _ = find_peaks(y_bytes, distance=5, prominence=1024*1024) 
        valleys, _ = find_peaks(-y_bytes, distance=5, prominence=1024*1024)
        
        # --- C. Base 拟合：t=0 + t>600s 的谷底 ---
        # 筛选 600s 后的谷底点
        stable_valleys_mask = x[valleys] > 600
        vx_stable = x[valleys][stable_valleys_mask].tolist()
        vy_stable = y_bytes[valleys][stable_valleys_mask].tolist()
        
        # 强制插入起点 t=0
        vx_fit = [0] + vx_stable
        vy_fit = [v_zero_val] + vy_stable
        
        if len(vx_fit) >= 2:
            a_base, b_base = np.polyfit(vx_fit, vy_fit, 1)
        else:
            a_base, b_base = 0, v_zero_val

        # --- D. Rise 计算：仅针对 600s 后的稳定锯齿 ---
        rise_slopes = []
        for v_idx in valleys[stable_valleys_mask]:
            sub_peaks = peaks[peaks > v_idx]
            if len(sub_peaks) > 0:
                p_idx = sub_peaks[0]
                dt = x[p_idx] - x[v_idx]
                dy = y_bytes[p_idx] - y_bytes[v_idx]
                if dt > 0: rise_slopes.append(dy / dt)
        avg_rise_rate = np.mean(rise_slopes) if rise_slopes else 0
        
        # --- E. 绘图 ---
        line_label = f"Throughput {tp_tag}"
        line, = ax.plot(x, y_bytes, alpha=0.6, label=line_label, linewidth=1.2)
        color = line.get_color()
        
        # 趋势线
        x_trend = np.array([0, 1800])
        ax.plot(x_trend, a_base * x_trend + b_base, color=color, linestyle='--', alpha=0.4)
        
        stats_summary.append({'label': tp_tag, 'rise': avg_rise_rate, 'base': a_base})

    # --- F. 侧边栏 ---
    sidebar_text = "Metric Definitions (Byte/s):\n"
    sidebar_text += "Rise: Avg Growth (t > 600s)\n"
    sidebar_text += "Base: Fit (t=0 & t > 600s Valleys)\n"
    sidebar_text += "-"*42 + "\n"
    sidebar_text += f"{'Config':<12} | {'Rise (B/s)':<12} | {'Base (B/s)':<10}\n"
    sidebar_text += "-"*42 + "\n"
    for s in stats_summary:
        sidebar_text += f"{s['label']:<12} | {s['rise']:>12,.0f} | {s['base']:>10,.0f}\n"

    ax.text(1.02, 0.95, sidebar_text, transform=ax.transAxes, fontsize=10,
            verticalalignment='top', family='monospace', 
            bbox=dict(boxstyle='round', facecolor='white', alpha=0.9, edgecolor='gray'))

    ax.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, p: format(int(x), ',')))
    ax.set_title("Retina Memory Dynamics: Stable State Analysis (Post-600s)", fontsize=16, fontweight='bold')
    ax.set_xlabel("Seconds since RocksDB Start (t=0)", fontsize=12)
    ax.set_ylabel("Memory Usage (Bytes)", fontsize=12)
    ax.axvline(x=600, color='red', linestyle=':', alpha=0.3, label='Stable Threshold (600s)')
    ax.grid(True, linestyle=':', alpha=0.5)
    ax.legend(loc='upper left', fontsize='small')
    
    plt.savefig("retina_stable_throughput.png", dpi=300, bbox_inches='tight')
    plt.show()

analyze_retina_stable_base("/home/ubuntu/disk2/pixels-sink/collected-retina-logs/final/")