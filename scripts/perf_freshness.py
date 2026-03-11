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

##########################################
# Configuration: CSV Files and Labels
##########################################
# csv_files = {
#     "10k": "result100/fresh_1n4_10k_2.csv",
#     "20k": "result100/fresh_1n4_20k_2.csv",
#     "40k": "result100/fresh_1n4_40k.csv",
#     "80k": "result100/fresh_1n4_80k.csv",
#     "120k": "result100/fresh_1n4_120k_2.csv",
#     # "150k": "result100/nouse_fresh_1n4_150k.csv",
# }

csv_files = {
    "10k": "result100_2/fresh_10k.csv",
    "20k": "result100_2/fresh_20k.csv",
    "40k": "result100_2/fresh_40k.csv",
    "80k": "result100_2/fresh_80k.csv",
    "100k": "result100_2/fresh_100k.csv",
    "120k": "result100_2/fresh_120k.csv",
    "160k": "result100_2/fresh_160k.csv",
    "200k": "result100_2/fresh_200k.csv",
}

csv_files = {
    "10k": "result_ch/fresh_10K.csv",
    "20k": "result_ch/fresh_20K.csv",
    "40k": "result_ch/fresh_40K.csv",
    "80k": "result_ch/fresh_80K.csv",
    "85k": "result_ch/fresh_85K.csv"
}
# csv_files = {
#     "Query Transaction": "tmp/i7i_2k_dec_freshness.csv",
#     "Query Record": "tmp/i7i_2k_record_dec_freshness.csv",
#     "Internal Transaction Context": "tmp/i7i_2k_txn_dec_freshness.csv",
#     "Query Selected Table, Trans Mode": "tmp/i7i_2k_batchtest_dec_freshness_2.csv"
# }


csv_files = {
    "200": "result_lance/fresh_bucket1_batch1000.csv",
    "300": "result_lance/fresh_bucket4_batch1000.csv"
}

MAX_SECONDS = 30000           # Capture data for the first N seconds
SKIP_SECONDS = 10            # Skip the first N seconds (adjustable)
BIN_SECONDS = 10            # Average window (seconds)
MAX_FRESHNESS = 500000       # Filter out useless data during initial warmup

##########################################
# Data Loading and Processing
##########################################
data = {}
data_raw_filtered = {} # New: store filtered raw data before resample

for label, path in csv_files.items():
    df_full = pd.read_csv(path, header=None)
    
    df = pd.DataFrame()
    df["ts"] = pd.to_datetime(df_full.iloc[:, 0], unit="ms")
    df["freshness"] = df_full.iloc[:, 1] # Always take the 2nd column

    # 1. Basic filter logic
    t0 = df["ts"].iloc[0]
    df["sec"] = (df["ts"] - t0).dt.total_seconds()
    
    mask = (df["sec"] >= SKIP_SECONDS) & \
           (df["sec"] <= MAX_SECONDS) & \
           (df["freshness"] <= MAX_FRESHNESS)
    df = df[mask].copy()

    # Align time axis start to 0
    if not df.empty:
        t_new0 = df["ts"].iloc[0]
        df["sec"] = (df["ts"] - t_new0).dt.total_seconds()

        # Store filtered raw data for CDF plot
        data_raw_filtered[label] = df["freshness"].copy()

        # 2. Resample only for Plot 1 (Time Series)
        df_bin = df.resample(f"{BIN_SECONDS}s", on="ts").mean().reset_index()
        df_bin["bin_sec"] = (df_bin["ts"] - df_bin["ts"].iloc[0]).dt.total_seconds()
        data[label] = df_bin

##########################################
# Plot 1: (unchanged) Smoothed Time Series
##########################################
# ... (Plot 1 code omitted; same logic as before) ...

##########################################
# Plot 2: Inverted CDF (use raw data_raw_filtered)
##########################################
plt.figure(figsize=(10, 5))

for label, raw_vals in data_raw_filtered.items():
    # Use raw points without resample
    vals = np.sort(raw_vals.dropna())
    prob = np.linspace(0, 1, len(vals))

    plt.plot(prob, vals, label=label)

plt.xticks(np.arange(0, 1.1, 0.1))
plt.xlim(0, 1)
plt.xlabel("CDF (Probability)")
plt.ylabel("Freshness (ms, Raw Data)") # Update label to emphasize raw data
plt.title(
    f"Inverted Freshness CDF\n(Raw Data Points, Skip {SKIP_SECONDS}s)"
)

plt.grid(True, which="both", ls="-", alpha=0.3)
plt.legend()
plt.tight_layout()
plt.savefig("freshness_cdf_raw_fixed_ticks.png") # Suggested rename for clarity
plt.close()

##########################################
# Data Export: Export Raw Filtered Data
##########################################
raw_series_list = []

for label, path in csv_files.items():
    # 1. Read all columns
    df_raw = pd.read_csv(path, header=None)
    
    # 2. Compatibility: always use column 2 (index 1) as freshness
    df_processed = pd.DataFrame()
    df_processed["ts"] = pd.to_datetime(df_raw.iloc[:, 0], unit="ms")
    
    # Key change: iloc[:, 1] ensures the middle column (freshness)
    # Even if a 3rd column (query time) exists, it is ignored
    df_processed["freshness"] = df_raw.iloc[:, 1] 
    
    # 3. Compute relative time axis
    t0 = df_processed["ts"].iloc[0]
    df_processed["sec"] = (df_processed["ts"] - t0).dt.total_seconds()
    
    # 4. Apply filter logic
    mask = (df_processed["sec"] >= SKIP_SECONDS) & \
           (df_processed["sec"] <= MAX_SECONDS) & \
           (df_processed["freshness"] <= MAX_FRESHNESS)
    
    # 5. Extract and rename for horizontal merge
    filtered_series = df_processed.loc[mask, "freshness"].reset_index(drop=True)
    filtered_series.name = label
    raw_series_list.append(filtered_series)

# 6. Merge horizontally and export
if raw_series_list:
    df_export = pd.concat(raw_series_list, axis=1)
    export_filename = "freshness_raw_filtered.csv"
    df_export.to_csv(export_filename, index=False)
    print(f"Filtered raw data exported to: {export_filename}")
