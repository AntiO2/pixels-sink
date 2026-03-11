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
import seaborn as sns
import os

##########################################
# 1. Config: file paths and parameters
##########################################
data_dir = "result_ablation/gc"

# Strict display order
file_groups = {
    "Static": "static.csv",
    "10s": "fresh_10s_2.csv",
    "30s": "fresh_30s_size.csv",
    "60s": "fresh_60s_size.csv",
    "120s": "fresh_120s_size.csv",
    "240s": "fresh_240s.csv",
}


data_dir = "result1k2_feb"
file_groups = {
    "512": "fresh_512tile.csv",
    "4096": "fresh_4096tile.csv"
}

# Filter parameters
SKIP_SECONDS = 0      
MAX_SECONDS = 1200   
MAX_FRESHNESS = 50000 

##########################################
# 2. Data load and processing
##########################################
plot_data = {}

for label, filename in file_groups.items():
    path = os.path.join(data_dir, filename)
    if not os.path.exists(path):
        print(f"❌ File not found: {path}")
        continue

    # Read CSV (3 columns: ts, dummy, freshness)
    try:
        df = pd.read_csv(path, header=None, names=["ts", "dummy", "freshness"])
        print(f"📖 Read {label}: {len(df)} rows of raw data")
        
        if df.empty:
            print(f"⚠️ {label} file is empty")
            continue

        # Time axis transform (align to 0 seconds)
        df["ts_dt"] = pd.to_datetime(df["ts"], unit="ms")
        t0 = df["ts_dt"].iloc[0]
        df["sec"] = (df["ts_dt"] - t0).dt.total_seconds()

        # Data filtering
        mask = (df["sec"] >= SKIP_SECONDS) & \
               (df["sec"] <= MAX_SECONDS) & \
               (df["freshness"] <= MAX_FRESHNESS)
        
        clean_series = df.loc[mask, "freshness"].dropna()
        
        if clean_series.empty:
            print(f"⚠️ {label} No data left after filtering! (check SKIP_SECONDS)")
        else:
            plot_data[label] = clean_series
            print(f"✅ {label} usable data points: {len(clean_series)}")
            
    except Exception as e:
        print(f"❌ Error processing {label}: {e}")

##########################################
# 3. Box plot
##########################################
if not plot_data:
    print("‼️ No data to plot; check CSV content and paths.")
else:
    sns.set_theme(style="whitegrid")
    plt.figure(figsize=(10, 6))

    labels = list(plot_data.keys())
    values = [plot_data[label] for label in labels]

    # Draw box plot
    box_plot = plt.boxplot(
        values, 
        labels=labels, 
        patch_artist=True,
        showmeans=True, 
        meanprops={"marker":"D", "markerfacecolor":"white", "markeredgecolor":"black", "markersize":"5"},
        flierprops={'marker': 'o', 'markersize': 2, 'markerfacecolor': 'gray', 'alpha': 0.1}
    )

    # Color scheme
    colors = sns.color_palette("husl", len(labels))
    for patch, color in zip(box_plot['boxes'], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)

    plt.title("Freshness Ablation Study: Query Time Comparison", fontsize=14, pad=20)
    plt.ylabel("Query Time (ms)", fontsize=12, fontweight='bold')
    plt.xlabel("Configuration", fontsize=12, fontweight='bold')
    
    # Auto-set Y range from data
    all_vals = pd.concat(list(plot_data.values()))
    plt.ylim(0, all_vals.quantile(0.99) * 1.2) # Ignore extreme outliers to improve view

    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    
    save_path = "freshness_boxplot_final.png"
    plt.savefig(save_path, dpi=300)
    print(f"\n🚀 Plot saved to: {save_path}")
    
    # Print final stats comparison
    print("\n--- Final Statistics ---")
    for label in labels:
        s = plot_data[label]
        print(f"{label:10} | Mean: {s.mean():8.2f}ms | Median: {s.median():8.2f}ms | P95: {s.quantile(0.95):8.2f}ms")

    plt.show()

##########################################
# 4. Transpose export: each column is a config
##########################################
if plot_data:
    # Put all Series into a list
    export_columns = []
    for label in file_groups.keys():  # Extract in defined order
        if label in plot_data:
            # Extract data and rename Series as CSV headers
            s = plot_data[label].reset_index(drop=True)
            s.name = label
            export_columns.append(s)

    # Merge horizontally (axis=1), one config per column
    df_export = pd.concat(export_columns, axis=1)

    # Export CSV without index
    export_filename = "gc_interval_query.csv"
    df_export.to_csv(export_filename, index=False)
    
    print(f"\n📊 Exported successfully: {export_filename}")
    print(f"Table shape: {df_export.shape} (rows x cols)")
    print(df_export.head()) # Print preview rows
