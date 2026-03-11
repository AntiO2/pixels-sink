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
import matplotlib.ticker as ticker
import re
import os
import numpy as np

##########################################
# 1. Config parameters
##########################################
SET_NAME = "retina_realtime-pixels-retina_8192tile_3"
LOG_FILE = "collected-retina-logs/" + SET_NAME + ".out"
OUTPUT_BASE = "collected-retina-logs/tile/" + SET_NAME
RESAMPLE_INTERVAL = '10s' 
GI_FACTOR = 1024**3
MAX_SECONDS = 2400  # Max window in seconds

def parse_and_plot(log_path, output_name, interval='10s'):
    # --- 2. Regex configuration ---
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
    print(f"[*] Parsing log: {log_path}")

    if not os.path.exists(log_path):
        print(f"[!] Error: file not found {log_path}")
        return

    # --- 3. Parsing logic ---
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
        print("[!] Error: no matching data found in log.")
        return

    # --- 4. Data preprocessing ---
    df = pd.DataFrame(rows)
    df['time'] = pd.to_datetime(df['time'])
    df = df.set_index('time').sort_index()

    required_columns = ['rdb_total', 'jvm_used', 'ret_allocated', 'ret_tracked', 'ret_objects', 'ret_pure', 'cpu_proc']
    for col in required_columns:
        if col not in df.columns:
            df[col] = 0.0

    df = df.ffill().fillna(0.0)

    # Resample
    df_res = df.resample(interval, label='right', closed='right').mean()

    # Drop last possibly incomplete bucket
    if len(df_res) > 1:
        df_res = df_res.iloc[:-1]

    # Insert T=0 origin point
    start_time = df_res.index[0] - pd.Timedelta(interval)
    df_zero = pd.DataFrame(0.0, index=[start_time], columns=df_res.columns)
    df_zero['cpu_proc'] = df_res['cpu_proc'].iloc[0]
    df_res = pd.concat([df_zero, df_res])

    # Linear interpolation
    df_res = df_res.interpolate(method='linear').fillna(0.0)
    df_res['rel_sec'] = (df_res.index - df_res.index[0]).total_seconds()

    # [Key change]: truncate time here
    df_res = df_res[df_res['rel_sec'] <= MAX_SECONDS]

    # --- 5. Compute metrics ---
    df_res['L1_Pure_Retina'] = df_res['ret_pure'] / GI_FACTOR
    df_res['L2_Overhead'] = (df_res['ret_tracked'] - df_res['ret_pure']).clip(lower=0) / GI_FACTOR
    df_res['L3_Other_OffHeap'] = (df_res['ret_allocated'] - df_res['ret_tracked'] - df_res['rdb_total']).clip(lower=0) / GI_FACTOR
    df_res['L4_RocksDB'] = df_res['rdb_total'] / GI_FACTOR
    df_res['L5_JVM'] = df_res['jvm_used'] / GI_FACTOR

    df_res.index.name = 'time'
    df_res.to_csv(f"{output_name}.csv", index_label='time')

    # --- 6. Plot ---
    plt.style.use('seaborn-v0_8-whitegrid') # Use a cleaner style
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 12), sharex=True)
    
    # A. Top plot: memory stack + CPU curve
    colors = ['#2ca02c', '#98df8a', '#bcbd22', '#ff7f0e', '#1f77b4']
    labels = ['Retina (Pure Data)', 'Monitor Overhead', 'Other Off-Heap (S3/etc.)', 'RocksDB Native', 'JVM Heap']

    ax1.stackplot(df_res['rel_sec'], 
                  df_res['L1_Pure_Retina'], 
                  df_res['L2_Overhead'], 
                  df_res['L3_Other_OffHeap'],
                  df_res['L4_RocksDB'],
                  df_res['L5_JVM'],
                  labels=labels, colors=colors, alpha=0.8)

    # Overlay CPU (right axis)
    ax1_cpu = ax1.twinx()
    # Smooth CPU curve for nicer look
    cpu_smooth = df_res['cpu_proc'].ewm(span=3).mean() 
    ax1_cpu.plot(df_res['rel_sec'], cpu_smooth, color='#d62728', linestyle='--', linewidth=1.5, label='CPU Process % (Smooth)')
    ax1_cpu.set_ylabel("CPU Usage (%)", color='#d62728', fontsize=12, fontweight='bold')
    ax1_cpu.tick_params(axis='y', labelcolor='#d62728')
    ax1_cpu.set_ylim(0, max(df_res['cpu_proc'].max() * 1.2, 100))

    ax1.set_title(f"Retina Analysis (First {MAX_SECONDS}s, Interval: {interval})", fontsize=14, fontweight='bold')
    ax1.set_ylabel("Memory Usage (GiB)", fontsize=12, fontweight='bold')
    
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax1_cpu.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', frameon=True, ncol=2)
    
    ax1.grid(True, linestyle=':', alpha=0.6)
    ax1.yaxis.set_major_formatter(ticker.FormatStrFormatter('%.1f GiB'))
    ax1.set_ylim(0, None)
    ax1.set_xlim(0, MAX_SECONDS)

    # B. Bottom plot: Object count
    ax2.plot(df_res['rel_sec'], df_res['ret_objects'], color='#9467bd', linewidth=2, label='Retina Objects')
    ax2.fill_between(df_res['rel_sec'], df_res['ret_objects'], color='#9467bd', alpha=0.1)
    ax2.set_xlabel("Time from Start (seconds)", fontsize=12, fontweight='bold')
    ax2.set_ylabel("Active Objects Count", fontsize=12, fontweight='bold')
    ax2.legend(loc='upper left')
    ax2.grid(True, linestyle=':', alpha=0.6)
    ax2.yaxis.set_major_formatter(ticker.EngFormatter())
    ax2.set_ylim(0, None)

    plt.tight_layout()
    plt.savefig(f"{output_name}.png", dpi=300)
    print(f"[+] Script completed. Max window: {MAX_SECONDS}s。")
    plt.show()

if __name__ == "__main__":
    parse_and_plot(LOG_FILE, OUTPUT_BASE, RESAMPLE_INTERVAL)
