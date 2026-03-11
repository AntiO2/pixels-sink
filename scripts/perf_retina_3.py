# Copyright 2026 PixelsDB.
#
# This file is part of Pixels.
#
# Pixels is free software: you can redistribute it and/or modify
# it under the terms of the Affero GNU General Public License as
# published by the Free Software Foundation, either version 3 of
# the License, or (at your option) any later version.
#
# Pixels is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# Affero GNU General Public License for more details.
#
# You should have received a copy of the Affero GNU General Public
# License along with Pixels.  If not, see
# <https://www.gnu.org/licenses/>.

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
    # Match trailing numbers in filenames, e.g., _85k.csv or _120.csv
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

    # Sort by throughput value
    all_files.sort(key=get_throughput_value)
    
    sns.set_theme(style="whitegrid")
    fig, ax = plt.subplots(figsize=(18, 10))
    plt.subplots_adjust(right=0.72) 
    
    stats_summary = []

    for idx, file in enumerate(all_files):
        tp_tag = os.path.basename(file).split('_')[-1].replace('.csv', '')
        
        # Read data; assume columns rel_sec, L1_Pure_Retina, L4_RocksDB
        try:
            df = pd.read_csv(file)
        except Exception as e:
            print(f"Error reading {file}: {e}")
            continue
            
        # --- A. Time alignment (t=0) ---
        # Find first point where RocksDB starts
        start_condition = df['L4_RocksDB'] > 0
        if not start_condition.any(): 
            print(f"Skip {file}: L4_RocksDB always 0")
            continue
            
        t_zero_abs = df[start_condition]['rel_sec'].iloc[0]
        v_zero_val = df[start_condition]['L1_Pure_Retina'].iloc[0] * (1024**3)
        
        df['norm_sec'] = df['rel_sec'] - t_zero_abs
        
        # Crop plot window (0 - 1800s)
        mask_plot = (df['norm_sec'] >= 0) & (df['norm_sec'] <= 1800)
        work_df = df[mask_plot].copy()
        if work_df.empty: continue
        
        x = work_df['norm_sec'].values
        y_bytes = work_df['L1_Pure_Retina'].values * (1024**3)
        
        # --- B. Peak/valley detection ---
        # Set prominence to filter small noise
        peaks, _ = find_peaks(y_bytes, distance=5, prominence=1024*1024) 
        valleys, _ = find_peaks(-y_bytes, distance=5, prominence=1024*1024)
        
        # --- C. Base trend fit (handle no-valley case) ---
        stable_valleys_mask = x[valleys] > 600
        vx_stable = x[valleys][stable_valleys_mask].tolist()
        vy_stable = y_bytes[valleys][stable_valleys_mask].tolist()
        
        # Build fit points: start(0,v_zero) + valleys after 600s
        vx_fit = [0] + vx_stable
        vy_fit = [v_zero_val] + vy_stable
        
        # Robustness: too few valleys to fit
        if len(vx_fit) < 2:
            stable_mask = x > 600
            if stable_mask.any():
                # Fallback: linear regression on all points after 600s
                a_base, b_base = np.polyfit(x[stable_mask], y_bytes[stable_mask], 1)
            else:
                a_base, b_base = 0, v_zero_val
        else:
            # Standard: fit cleaned baseline
            a_base, b_base = np.polyfit(vx_fit, vy_fit, 1)

        # --- D. Rise rate calc (no-sawtooth case) ---
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
            # Fallback: if no sawtooth, rise rate = base slope
            avg_rise_rate = a_base if a_base > 0 else 0
        
        # --- E. Plot and styling ---
        line_label = f"Throughput {tp_tag}"
        line, = ax.plot(x, y_bytes, alpha=0.6, label=line_label, linewidth=1.5)
        color = line.get_color()
        
        # Draw base trend dashed line
        x_trend = np.array([0, 1800])
        ax.plot(x_trend, a_base * x_trend + b_base, color=color, linestyle='--', alpha=0.4)
        
        stats_summary.append({'label': tp_tag, 'rise': avg_rise_rate, 'base': a_base})

    # --- F. Sidebar info ---
    sidebar_text = "Metric Definitions (Byte/s):\n"
    sidebar_text += "Rise: Avg Sawtooth Slope (t > 600s)\n"
    sidebar_text += "Base: Bottom Trend (t=0 & Valleys)\n"
    sidebar_text += "-"*44 + "\n"
    sidebar_text += f"{'Config':<12} | {'Rise (B/s)':<14} | {'Base (B/s)':<10}\n"
    sidebar_text += "-"*44 + "\n"
    for s in stats_summary:
        sidebar_text += f"{s['label']:<12} | {s['rise']:>14,.0f} | {s['base']:>10,.0f}\n"

    # Place stats on the right side
    ax.text(1.02, 0.95, sidebar_text, transform=ax.transAxes, fontsize=10,
            verticalalignment='top', family='monospace', 
            bbox=dict(boxstyle='round', facecolor='white', alpha=0.9, edgecolor='gray'))

    # Format Y axis with commas
    ax.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, p: format(int(x), ',')))
    
    ax.set_title("Retina Memory Dynamics: Stable State Analysis (Post-600s)", fontsize=16, fontweight='bold')
    ax.set_xlabel("Seconds since RocksDB Start (t=0)", fontsize=12)
    ax.set_ylabel("Memory Usage (Bytes)", fontsize=12)
    
    # Draw 600s stability threshold line
    ax.axvline(x=600, color='red', linestyle=':', alpha=0.3, label='Stable Threshold (600s)')
    
    ax.grid(True, linestyle=':', alpha=0.5)
    ax.legend(loc='upper left', fontsize='small')
    
    plt.savefig("retina_stable_throughput.png", dpi=300, bbox_inches='tight')
    print("Analysis complete. Plot saved as 'retina_stable_throughput.png'.")
    plt.show()

# Run analysis
analyze_retina_stable_base("/home/ubuntu/disk2/pixels-sink/collected-retina-logs/final/")
