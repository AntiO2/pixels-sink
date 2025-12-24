import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, date

##########################################
# Configuration: CSV Files and Labels
##########################################
csv_files = {
    # "10k": "resulti7i/10k_rate_2.csv",
    # "20k": "resulti7i/20k_rate_2.csv",
    # "30k": "resulti7i/30k_rate_2.csv",
    # "40k": "resulti7i/40k_rate_2.csv",
    # "50k": "resulti7i/50k_rate.csv",
    # "60k": "resulti7i/60k_rate_2.csv",
    # "80k": "resulti7i/80k_rate_2.csv",
    "100k": "resulti7i_100/100k_rate.csv",
}

COL_NAMES = ["time", "rows", "txns", "debezium", "serdRows", "serdTxs"]
PLOT_COL = "rows"

MAX_SECONDS = 1800
SKIP_SECONDS = 10
BIN_SECONDS = 60

##########################################
# Data Loading and Processing
##########################################
data = {}
for label, path in csv_files.items():
    print(f"Processing: {label} -> {path}")

    # 1. Load data
    df = pd.read_csv(path, header=None, names=COL_NAMES, sep=',')

    # 2. Handle timestamps and skip rows with incorrect formats
    # errors='coerce' turns unparseable formats into NaT
    df["ts"] = pd.to_datetime(df["time"], format="%H:%M:%S", errors='coerce')

    # Remove rows where time could not be parsed (NaT)
    initial_count = len(df)
    df = df.dropna(subset=["ts"]).copy()
    if len(df) < initial_count:
        print(f"  Note: Skipped {initial_count - len(df)} rows with incorrect data format")

    # Combine with current date
    df["ts"] = df["ts"].dt.time.apply(lambda x: datetime.combine(date.today(), x))

    # 3. Calculate relative time
    df = df.sort_values("ts") # Ensure time is ordered
    t0 = df["ts"].iloc[0]
    df["sec"] = (df["ts"] - t0).dt.total_seconds()

    # 4. Filter time range
    df = df[df["sec"] >= SKIP_SECONDS].copy()
    if df.empty:
        print(f"  Warning: {label} has no data remaining after skipping {SKIP_SECONDS}s")
        continue

    t_new0 = df["ts"].iloc[0]
    df["sec"] = (df["ts"] - t_new0).dt.total_seconds()
    df = df[df["sec"] <= MAX_SECONDS]

    # 5. Resampling and Aggregation
    # Ensure numeric columns are numeric types before setting index
    # This prevents mean() failures if strings are mixed in numeric columns
    for col in ["rows", "txns", "debezium", "serdRows", "serdTxs"]:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df.set_index("ts")

    # Perform mean calculation on numeric columns only, ignoring strings (like 'time' column)
    df_bin = df.resample(f"{BIN_SECONDS}s").mean(numeric_only=True).reset_index()

    # 6. Align horizontal axis
    if not df_bin.empty:
        df_bin["bin_sec"] = (df_bin["ts"] - df_bin["ts"].iloc[0]).dt.total_seconds()
        data[label] = df_bin

##########################################
# Plot 1: Time Series Fluctuations
##########################################
plt.figure(figsize=(10, 5))
for label, df in data.items():
    plt.plot(df["bin_sec"], df[PLOT_COL], label=label)

plt.xlabel("Time (sec)")
plt.ylabel(f"{PLOT_COL} ({BIN_SECONDS}s average)")
plt.title(f"{PLOT_COL} Over Time ({BIN_SECONDS}s Avg)")
plt.legend()
plt.grid(True, linestyle='--', alpha=0.7)
plt.tight_layout()
plt.savefig(f"rate_{PLOT_COL}_over_time_variable_bin.png")
plt.close()

##########################################
# Plot 2: CDF (Cumulative Distribution Function)
##########################################
plt.figure(figsize=(10, 5))
for label, df in data.items():
    vals = np.sort(df[PLOT_COL].dropna())
    if len(vals) > 0:
        y = np.linspace(0, 1, len(vals))
        plt.plot(vals, y, label=label)

plt.xlabel(f"{PLOT_COL} ({BIN_SECONDS}s average)")
plt.ylabel("CDF")
plt.title(f"{PLOT_COL} CDF Distribution")
plt.legend()
plt.grid(True, linestyle='--', alpha=0.7)
plt.tight_layout()
plt.savefig(f"rate_{PLOT_COL}_cdf_variable_bin.png")
plt.close()

print(f"\nAll tasks completed! Plots have been generated.")