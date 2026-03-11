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
from scipy.signal import find_peaks

def analyze_retina_performance_fixed_labels(folder_path):
    files = sorted(glob.glob(os.path.join(folder_path, "*.csv")))
    plt.figure(figsize=(16, 10))
    
    stats_table = []

    for idx, file in enumerate(files):
        cap = file.split('_')[-1].replace('.csv', '')
        df = pd.read_csv(file)
        
        # 1. Window filter
        mask = (df['rel_sec'] >= 300) & (df['rel_sec'] <= 660)
        work_df = df[mask].copy()
        if work_df.empty: continue
        
        x = work_df['rel_sec'].values
        y_bytes = work_df['L1_Pure_Retina'].values * (1024**3)
        
        # 2. Peak/valley detection
        peaks, _ = find_peaks(y_bytes, distance=5, prominence=1024*1024) 
        valleys, _ = find_peaks(-y_bytes, distance=5, prominence=1024*1024)
        
        if len(valleys) > 1 and len(peaks) > 1:
            # A. Valley fit
            a_val, b_val = np.polyfit(x[valleys], y_bytes[valleys], 1)
            
            # B. Rising slope calculation
            rise_slopes = []
            for v_idx in valleys:
                sub_peaks = peaks[peaks > v_idx]
                if len(sub_peaks) > 0:
                    p_idx = sub_peaks[0]
                    dt = x[p_idx] - x[v_idx]
                    dy = y_bytes[p_idx] - y_bytes[v_idx]
                    if dt > 0: rise_slopes.append(dy / dt)
            avg_rise_rate = np.mean(rise_slopes) if rise_slopes else 0
            
            # --- Plot section ---
            line, = plt.plot(x, y_bytes, alpha=0.6, label=f'Cap {cap}', linewidth=1.5)
            color = line.get_color()
            
            # Mark selected valley for annotation (middle one)
            target_v_idx = len(valleys) // 2
            v_x = x[valleys[target_v_idx]]
            v_y = y_bytes[valleys[target_v_idx]]
            
            # Draw valley trend line
            plt.plot(x, a_val * x + b_val, color=color, linestyle='--', alpha=0.5)
            
            # 3. Improved annotation with arrow
            # Use idx * 0.15 GiB offset to avoid overlap
            offset_y = (idx + 1) * (0.1 * 1024**3) 
            
            label_text = (f"Cap {cap}\n"
                         f"Rise: {avg_rise_rate:,.0f} B/s\n"
                         f"Base: {a_val:,.0f} B/s")
            
            plt.annotate(
                label_text,
                xy=(v_x, v_y), # Arrow points to valley coordinates
                xytext=(0, 40 + idx*30), # Text offset from point (pixels)
                textcoords='offset points',
                ha='center',
                fontsize=9,
                color='white',
                fontweight='bold',
                bbox=dict(boxstyle='round,pad=0.4', fc=color, alpha=0.8, ec='black'),
                arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0.1', color=color)
            )
            
            stats_table.append([cap, avg_rise_rate, a_val])

    # Format axes
    plt.gca().get_yaxis().set_major_formatter(
        plt.FuncFormatter(lambda x, p: format(int(x), ','))
    )
    
    plt.title("Retina Performance: Precise Byte/s Mapping (300s-660s)", fontsize=16)
    plt.xlabel("Time (s)")
    plt.ylabel("Memory Usage (Bytes)")
    plt.grid(True, linestyle=':', alpha=0.4)
    plt.legend(loc='upper left', bbox_to_anchor=(1, 1))
    plt.tight_layout()
    plt.savefig("retina_fixed_labels.png", dpi=300)
    plt.show()

analyze_retina_performance_fixed_labels("/home/ubuntu/disk2/pixels-sink/collected-retina-logs/capacity/")
