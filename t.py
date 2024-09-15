import matplotlib.pyplot as plt

# 时间序列（每10秒一个数据点）
time_intervals = [i * 10 for i in range(1, 31)]

# rocksdb版本的qps数据
rocksdb_qps = [
    441.45, 464.23, 466.55, 658.20, 573.39, 521.32, 702.22, 573.09, 
    614.93, 753.40, 812.69, 477.48, 262.50, 346.80, 505.80, 610.39, 
    604.31, 596.50, 683.49, 725.80, 685.31, 827.90, 760.01, 638.87, 
    633.33, 733.18, 786.19, 745.11, 471.88, 545.92
]

# memory版本的qps数据
memory_qps = [
    481.80, 460.22, 482.70, 546.90, 536.69, 216.50, 291.95, 601.19, 
    595.20, 621.44, 452.12, 592.53, 467.00, 575.68, 457.08, 585.53, 
    537.78, 380.92, 466.49, 574.44, 497.33, 795.87, 592.55, 504.02, 
    714.65, 646.43, 633.53, 618.54, 606.42, 503.61
]

# 创建图形
plt.figure(figsize=(10, 6))

# rocksdb版本的QPS变化曲线
plt.plot(time_intervals, rocksdb_qps, label='RocksDB', marker='o')

# memory版本的QPS变化曲线
plt.plot(time_intervals, memory_qps, label='Memory', marker='s')

# 添加标题和标签
plt.title('Throughput (QPS) over Time')
plt.xlabel('Time (s)')
plt.ylabel('QPS')

# 添加图例
plt.legend()

# 显示图形
plt.grid(True)
plt.savefig('qps.png')
