# Spark Operator Time Analyzer

这个工具分析Spark事件日志，提取每个operator的时间统计信息并生成可视化报告。

## 功能

- 解析Spark事件日志中的`SparkListenerTaskEnd`和`SparkListenerSQLExecutionStart`事件
- 提取每个operator的时间信息（支持"op time"、"gpuTime"等时间指标）
- 统计每个operator的总时间和占比
- 生成饼图可视化和详细报告

## 安装依赖

```bash
pip3 install -r requirements.txt
```

## 使用方法

### 方法1：使用Python脚本直接分析

```bash
python3 spark_operator_analyzer.py /path/to/your/spark-events/event-log-file --output output.png --verbose
```

### 方法2：使用便捷脚本

```bash
chmod +x analyze_spark_logs.sh
./analyze_spark_logs.sh /path/to/your/spark-events/event-log-file
```

如果不提供参数，脚本会使用默认的事件日志路径：`/home/hongbin/spark-events/local-1758175103938`

## 输出文件

- **饼图**: `results/operator_time_distribution.png` - 显示各operator时间占比的饼图
- **CSV报告**: `results/operator_time_report.csv` - 详细的时间统计数据
- **控制台输出**: 详细的分析报告

## 示例输出

```
================================================================================
SPARK OPERATOR TIME ANALYSIS REPORT
================================================================================

Total Executor Run Time: 3947.755 seconds
Total Operator Time: 11544173011.659 seconds

Number of Operators: 9
Number of Metric Records: 46439
Number of Operator Mappings: 300

--------------------------------------------------------------------------------
OPERATOR TIME BREAKDOWN
--------------------------------------------------------------------------------
Operator                       Time (ms)    Time (s)   Percentage  
--------------------------------------------------------------------------------
GpuProject                     4072452679660.262 4072452679.660 35.28       %
GpuBroadcastHashJoin           2901840747976.288 2901840747.976 25.14       %
GpuColumnarExchange            1900420145981.522 1900420145.982 16.46       %
GpuCoalesceBatches             1001287311385.000 1001287311.385 8.67        %
GpuRange                       607197910772.465 607197910.772 5.26        %
```

## 工作原理

1. **提取时间指标**: 
   - 从`SparkListenerTaskEnd`事件的Accumulables中提取op time（纳秒单位）
   - 从`Task Metrics`中提取Executor Run Time（毫秒单位）
2. **建立映射关系**: 从`SparkListenerSQLExecutionStart`事件的sparkPlanInfo中提取accumulatorId到operator的映射
3. **单位统一**: 内部统一转换为一致的时间单位进行计算
4. **聚合统计**: 将相同operator的时间进行累加
5. **智能百分比计算**: 
   - 当operator总时间 > executor时间时：基于operator总时间计算百分比
   - 当operator总时间 < executor时间时：基于executor时间计算百分比，并添加"Others"类别
6. **生成报告**: 创建饼图和详细报告

## 支持的时间指标类型

- `op time`: 标准的operator时间
- `op time (shuffle read)`: Shuffle读取时间
- `op time (shuffle write partition & serial)`: Shuffle写入分区和序列化时间
- `shuffle write time`: Shuffle写入时间

注意：程序只统计以上op time相关的指标，不包括其他时间相关的指标如gpuTime、executor time等。

## "Others"类别说明

当operator时间总和小于executor时间时，程序会自动添加"Others"类别来表示未被跟踪的时间，包括：
- JVM开销、GC暂停
- 网络/IO等待时间
- Spark框架开销
- 其他未映射的操作时间

## 注意事项

- **时间单位**：
  - Op time在事件日志中以**纳秒(ns)**为单位存储
  - Executor Run Time在事件日志中以**毫秒(ms)**为单位存储
  - 程序内部自动处理单位转换
- 在GPU工作负载中，operator时间通常会超过executor时间（由于并行执行）
- 程序会智能选择合适的基准来计算百分比
- 未映射的accumulator ID会在日志中显示警告

## 文件说明

- `spark_operator_analyzer.py`: 主分析程序
- `analyze_spark_logs.sh`: 便捷运行脚本
- `demo_others_category.py`: 演示"Others"功能的示例脚本
- `requirements.txt`: Python依赖包列表
