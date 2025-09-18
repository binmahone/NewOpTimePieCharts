# Spark Operator Time Analyzer

A tool to analyze Spark event logs and visualize operator time distribution with dual pie charts.

## Features

- **Dual Perspective Analysis**: Shows both original op time and sister metrics (excl. SemWait) views
- **GPU SemWait Tracking**: Tracks GPU semaphore wait times separately  
- **Eye-Friendly Visualization**: Uses carefully selected colors for better readability
- **Comprehensive Reports**: Generates both visual charts and detailed text reports
- **Large File Support**: Handles multi-GB event logs efficiently

## Quick Start

### Local Analysis
```bash
# Install dependencies
pip3 install -r requirements.txt

# Analyze event log
python3 spark_operator_analyzer.py /path/to/event-log --output analysis.png --verbose
```

### Remote Analysis on u38
```bash
# Deploy and run on u38 machine (automated)
./deploy_and_run_u38.sh /data/spark/history/your-event-log

# Or with default event log
./deploy_and_run_u38.sh
```

## Supported Time Metrics

**Original Metrics** (Left Chart):
- `op time` - Standard operator time
- `op time (shuffle read)` - Shuffle read time  
- `op time (shuffle write partition & serial)` - Shuffle write time
- `shuffle write time` - Shuffle write time

**Sister Metrics** (Right Chart):
- Same metrics with `(excl. SemWait)` suffix - Time excluding semaphore waits
- `semWait` - Total GPU semaphore wait time from all `gpuSemaphoreWait` records

## Output Files

Each analysis generates:
- **Dual Pie Chart** (`analysis.png`) - Side-by-side comparison of both perspectives
- **Text Report** (`analysis_report.txt`) - Detailed tables and performance insights

## Understanding the Results

### Left Chart: Original Op Time
Shows complete operator times including all wait times. Best for understanding overall time distribution.

### Right Chart: Sister Metrics + SemWait  
Shows pure computation time separated from GPU wait times. Best for identifying:
- **Computation bottlenecks** (operator bars)
- **GPU resource contention** (semWait slice)
- **Untracked time** (Others slice)

### Key Insights
- **High semWait %**: GPU resource contention, consider reducing parallelism
- **Large Others %**: Many operations not tracked by sister metrics
- **Top operators**: Primary computation bottlenecks to optimize

## File Structure

- `spark_operator_analyzer.py` - Main analysis program
- `deploy_and_run_u38.sh` - Automated u38 deployment script
- `analyze_spark_logs.sh` - Local convenience script  
- `requirements.txt` - Python dependencies
- `README.md` - This documentation

## Requirements

- Python 3.7+
- matplotlib, seaborn, pandas
- SSH access to u38 (for remote analysis)

## Time Units

- **Op time metrics**: Stored as nanoseconds in event log
- **Executor Run Time**: Stored as milliseconds in event log  
- **gpuSemaphoreWait**: Parsed from "HH:MM:SS.mmm" format
- All conversions handled automatically

---

For questions or issues, check the generated text reports for detailed performance insights.