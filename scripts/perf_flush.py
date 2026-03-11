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
# 配置参数
##########################################
MAX_SECONDS = 600  # 设定截取的最大时间（秒）

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
            
        # 读取数据
        df = pd.read_csv(path, header=None, names=["ts", "freshness", "query_time"])
        
        # 1. 计算相对时间（秒）
        # 假设 ts 是毫秒单位，如果是秒则不需要 / 1000
        # 我们以每个文件的第一行作为 T=0
        start_ts = df["ts"].iloc[0]
        df["rel_sec"] = (df["ts"] - start_ts) / 1000.0
        
        # 2. 根据 MAX_SECONDS 过滤
        df_filtered = df[df["rel_sec"] <= MAX_SECONDS].copy()
        
        # 3. 提取需要的列并合并
        # reset_index 确保不同策略的数据行能对齐（从第0行开始）
        temp_df = pd.DataFrame({
            f"{label}_fresh": df_filtered["freshness"].reset_index(drop=True),
            f"{label}_query": df_filtered["query_time"].reset_index(drop=True)
        })
        
        # 使用 axis=1 横向拼接
        combined_df = pd.concat([combined_df, temp_df], axis=1)

    # 创建输出目录
    os.makedirs("tmp", exist_ok=True)
    
    output_path = "tmp/flush_ablation_combined.csv"
    combined_df.to_csv(output_path, index=False)
    print(f"✅ Combined data (First {MAX_SECONDS}s) saved to {output_path}")

if __name__ == "__main__":
    preprocess_flush_data()
