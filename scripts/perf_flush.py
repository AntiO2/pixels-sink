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
import os

##########################################
# Config parameters
##########################################
MAX_SECONDS = 600  # Max window in seconds

def preprocess_flush_data():
    # Define strategy file mapping
    strategies = {
        "High": "result_ablation/flush/fresh_low.csv",
        "Mid":  "result_ablation/flush/fresh_mid.csv",
        "Low":  "result_ablation/flush/fresh_large.csv"
    }
    
    combined_df = pd.DataFrame()
    
    for label, path in strategies.items():
        if not os.path.exists(path):
            print(f"Warning: {path} not found.")
            continue
            
        # Read data
        df = pd.read_csv(path, header=None, names=["ts", "freshness", "query_time"])
        
        # 1. Compute relative time (seconds)
        # Assume ts is milliseconds; if seconds, no /1000
        # Use first row of each file as T=0
        start_ts = df["ts"].iloc[0]
        df["rel_sec"] = (df["ts"] - start_ts) / 1000.0
        
        # 2. Filter by MAX_SECONDS
        df_filtered = df[df["rel_sec"] <= MAX_SECONDS].copy()
        
        # 3. Extract needed columns and merge
        # reset_index aligns rows across strategies (start at row 0)
        temp_df = pd.DataFrame({
            f"{label}_fresh": df_filtered["freshness"].reset_index(drop=True),
            f"{label}_query": df_filtered["query_time"].reset_index(drop=True)
        })
        
        # Concatenate horizontally with axis=1
        combined_df = pd.concat([combined_df, temp_df], axis=1)

    # Create output directory
    os.makedirs("tmp", exist_ok=True)
    
    output_path = "tmp/flush_ablation_combined.csv"
    combined_df.to_csv(output_path, index=False)
    print(f"✅ Combined data (First {MAX_SECONDS}s) saved to {output_path}")

if __name__ == "__main__":
    preprocess_flush_data()
