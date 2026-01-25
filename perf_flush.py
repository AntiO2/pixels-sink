import pandas as pd
import os

def preprocess_flush_data():
    # 定义策略文件映射
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
            
        df = pd.read_csv(path, header=None, names=["ts", "freshness", "query_time"])
        
        temp_df = pd.DataFrame({
            f"{label}_fresh": df["freshness"],
            f"{label}_query": df["query_time"]
        })
        combined_df = pd.concat([combined_df, temp_df.reset_index(drop=True)], axis=1)

    output_path = "tmp/flush_ablation_combined.csv"
    combined_df.to_csv(output_path, index=False)
    print(f"✅ Combined data saved to {output_path}")

if __name__ == "__main__":
    preprocess_flush_data()