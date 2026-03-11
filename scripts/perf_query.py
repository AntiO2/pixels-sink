import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import os

##########################################
# 1. 配置：文件路径与参数
##########################################
data_dir = "result_ablation/gc"

# 严格按照显示顺序排列
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

# 过滤参数
SKIP_SECONDS = 0      
MAX_SECONDS = 1200   
MAX_FRESHNESS = 50000 

##########################################
# 2. 数据加载与处理
##########################################
plot_data = {}

for label, filename in file_groups.items():
    path = os.path.join(data_dir, filename)
    if not os.path.exists(path):
        print(f"❌ 文件未找到: {path}")
        continue

    # 读取 CSV (三列格式: ts, dummy, freshness)
    try:
        df = pd.read_csv(path, header=None, names=["ts", "dummy", "freshness"])
        print(f"📖 读取 {label}: {len(df)} 行原始数据")
        
        if df.empty:
            print(f"⚠️ {label} 文件为空")
            continue

        # 时间轴转换（对齐到 0 秒）
        df["ts_dt"] = pd.to_datetime(df["ts"], unit="ms")
        t0 = df["ts_dt"].iloc[0]
        df["sec"] = (df["ts_dt"] - t0).dt.total_seconds()

        # 数据过滤
        mask = (df["sec"] >= SKIP_SECONDS) & \
               (df["sec"] <= MAX_SECONDS) & \
               (df["freshness"] <= MAX_FRESHNESS)
        
        clean_series = df.loc[mask, "freshness"].dropna()
        
        if clean_series.empty:
            print(f"⚠️ {label} 经过过滤后没有剩余数据！(请检查 SKIP_SECONDS)")
        else:
            plot_data[label] = clean_series
            print(f"✅ {label} 可用数据点: {len(clean_series)}")
            
    except Exception as e:
        print(f"❌ 处理 {label} 时出错: {e}")

##########################################
# 3. 绘制箱线图
##########################################
if not plot_data:
    print("‼️ 没有可用的数据绘图，请检查 CSV 内容和路径。")
else:
    sns.set_theme(style="whitegrid")
    plt.figure(figsize=(10, 6))

    labels = list(plot_data.keys())
    values = [plot_data[label] for label in labels]

    # 绘制箱线图
    box_plot = plt.boxplot(
        values, 
        labels=labels, 
        patch_artist=True,
        showmeans=True, 
        meanprops={"marker":"D", "markerfacecolor":"white", "markeredgecolor":"black", "markersize":"5"},
        flierprops={'marker': 'o', 'markersize': 2, 'markerfacecolor': 'gray', 'alpha': 0.1}
    )

    # 配色方案
    colors = sns.color_palette("husl", len(labels))
    for patch, color in zip(box_plot['boxes'], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)

    plt.title("Freshness Ablation Study: Query Time Comparison", fontsize=14, pad=20)
    plt.ylabel("Query Time (ms)", fontsize=12, fontweight='bold')
    plt.xlabel("Configuration", fontsize=12, fontweight='bold')
    
    # 根据数据自动设置 Y 轴范围
    all_vals = pd.concat(list(plot_data.values()))
    plt.ylim(0, all_vals.quantile(0.99) * 1.2) # 忽略极个别超大离群值以优化视图

    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    
    save_path = "freshness_boxplot_final.png"
    plt.savefig(save_path, dpi=300)
    print(f"\n🚀 绘图成功！文件已保存至: {save_path}")
    
    # 打印最终统计对比
    print("\n--- Final Statistics ---")
    for label in labels:
        s = plot_data[label]
        print(f"{label:10} | Mean: {s.mean():8.2f}ms | Median: {s.median():8.2f}ms | P95: {s.quantile(0.95):8.2f}ms")

    plt.show()

##########################################
# 4. 数据转置导出: 每一列代表一个配置
##########################################
if plot_data:
    # 将所有 Series 放入一个列表
    export_columns = []
    for label in file_groups.keys():  # 按照定义的顺序提取
        if label in plot_data:
            # 提取数据并重命名 Series，这将成为 CSV 的表头
            s = plot_data[label].reset_index(drop=True)
            s.name = label
            export_columns.append(s)

    # 横向合并 (axis=1)，形成每列一个配置的结构
    df_export = pd.concat(export_columns, axis=1)

    # 导出 CSV，不保存索引
    export_filename = "gc_interval_query.csv"
    df_export.to_csv(export_filename, index=False)
    
    print(f"\n📊 导出成功: {export_filename}")
    print(f"表格形状: {df_export.shape} (行 x 列)")
    print(df_export.head()) # 打印前几行预览