import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

##########################################
# Configuration: CSV Files and Labels
##########################################
csv_files = {
    # "10k_2": "resulti7i/10k_freshness.csv",
    # "10k": "resulti7i/10k_freshness_2.csv",
    # "20k": "resulti7i/20k_freshness.csv",
    # "20k": "resulti7i/20k_freshness_2.csv",
    # "30k": "resulti7i/30k_freshness_2.csv",
    # "40k": "resulti7i/40k_freshness_2.csv",
    # "50k": "resulti7i/50k_freshness.csv",
    # "60k": "resulti7i/60k_freshness_2.csv",
    # "80k": "resulti7i/80k_freshness_2.csv",
    # "10k": "resulti7i_100/10k_fresh.csv",
    # "20k": "resulti7i_100/20k_fresh.csv",
    # # "30k": "resulti7i_100/30k_fresh.csv",
    # "40k": "resulti7i_100/40k_fresh.csv",
    # "60k": "resulti7i_100/60k_fresh.csv",
    "100k": "resulti7i_100/100k_fresh.csv",
}
# csv_files = {
#     "Query Transaction": "tmp/i7i_2k_dec_freshness.csv",
#     "Query Record": "tmp/i7i_2k_record_dec_freshness.csv",
#     "Internal Transaction Context": "tmp/i7i_2k_txn_dec_freshness.csv",
#     "Query Selected Table, Trans Mode": "tmp/i7i_2k_batchtest_dec_freshness_2.csv"
# }

MAX_SECONDS = 1800           # Capture data for the first N seconds
SKIP_SECONDS = 10            # Skip the first N seconds (adjustable)
BIN_SECONDS = 180            # Average window (seconds)
MAX_FRESHNESS = 500000       # Filter out useless data during initial warmup
##########################################
# Data Loading and Processing
##########################################
data = {}
for label, path in csv_files.items():
    df = pd.read_csv(path, header=None, names=["ts", "freshness"])

    # Convert to datetime
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")

    # Relative seconds
    t0 = df["ts"].iloc[0]
    df["sec"] = (df["ts"] - t0).dt.total_seconds()

    # Skip initial SKIP_SECONDS
    df = df[df["sec"] >= SKIP_SECONDS]

    # Filter by max freshness threshold
    df = df[df["freshness"] <= MAX_FRESHNESS]

    # Recalculate time (align all curves to start at 0 seconds)
    t_new0 = df["ts"].iloc[0]
    df["sec"] = (df["ts"] - t_new0).dt.total_seconds()

    # Limit to MAX_SECONDS
    df = df[df["sec"] <= MAX_SECONDS]

    # Sample using an adjustable averaging window
    df_bin = df.resample(f"{BIN_SECONDS}s", on="ts").mean().reset_index()

    # Align horizontal axis (time series)
    df_bin["bin_sec"] = (df_bin["ts"] - df_bin["ts"].iloc[0]).dt.total_seconds()

    data[label] = df_bin


##########################################
# Plot 1: Smoothed/Beautified Time Series Oscillations
##########################################
# Set overall style; whitegrid looks clean and professional
sns.set_theme(style="whitegrid")

plt.figure(figsize=(12, 6)) # Slightly wider for better time trend visualization

for label, df in data.items():
    # Ensure data is sorted
    df_plot = df.sort_values("bin_sec")

    # Option A: Increase line width and anti-aliasing; use alpha for transparency to distinguish overlaps
    line, = plt.plot(
        df_plot["bin_sec"],
        df_plot["freshness"],
        label=label,
        linewidth=1.8,
        alpha=0.9,
        antialiased=True
    )


# Axis labeling and beautification
plt.xlabel("Time (sec)", fontsize=11, fontweight='bold')
plt.ylabel(f"Freshness (ms, {BIN_SECONDS}s average)", fontsize=11, fontweight='bold')

# Remove top and right spines for a cleaner look
sns.despine()

plt.title(
    f"Freshness Oscillations\n({BIN_SECONDS}s Binning, Skip {SKIP_SECONDS}s)",
    fontsize=13,
    pad=15
)

# Move legend outside or to the top right to avoid blocking curves
plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)

plt.grid(True, which="major", ls="-", alpha=0.4)
plt.tight_layout()
plt.savefig("freshness_over_time_smooth.png", dpi=300) # Save with high resolution
plt.close()


##########################################
# Plot 2: Inverted CDF (X-axis 0-1, Step 0.1)
##########################################
plt.figure(figsize=(10, 5))

for label, df in data.items():
    vals = np.sort(df["freshness"].dropna())
    prob = np.linspace(0, 1, len(vals))

    # X-axis is probability [0, 1], Y-axis is value
    plt.plot(prob, vals, label=label)

# Set X-axis ticks: from 0 to 1.1 (excluding 1.1) with step 0.1
plt.xticks(np.arange(0, 1.1, 0.1))
plt.xlim(0, 1) # Force display range between 0 and 1

# plt.yscale("log")
plt.xlabel("CDF (Probability)")
plt.ylabel(f"Freshness (ms, {BIN_SECONDS}s average)")
plt.title(
    f"Inverted Freshness CDF ({BIN_SECONDS}-Second Sampled, Skip {SKIP_SECONDS}s)"
)

plt.grid(True, which="both", ls="-", alpha=0.3)
plt.legend()
plt.tight_layout()
plt.savefig("freshness_cdf_fixed_ticks.png")
plt.close()

print("Plots generated: freshness_over_time_smooth.png, freshness_cdf_fixed_ticks.png")