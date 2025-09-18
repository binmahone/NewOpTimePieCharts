#!/bin/bash
# Simple script to analyze Spark event logs for operator time distribution

# Default paths
EVENT_LOG_PATH="/home/hongbin/spark-events/local-1758175103938"
OUTPUT_DIR="/home/hongbin/develop/250918_pie_chart_on_newoptime/results"

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Set event log path from command line if provided
if [ "$1" != "" ]; then
    EVENT_LOG_PATH="$1"
fi

echo "Analyzing Spark event log: $EVENT_LOG_PATH"
echo "Output directory: $OUTPUT_DIR"

# Run the analysis
python3 spark_operator_analyzer.py "$EVENT_LOG_PATH" \
    --output "$OUTPUT_DIR/operator_time_distribution.png" \
    --verbose

echo ""
echo "Analysis complete! Check the following files:"
echo "- Pie chart: $OUTPUT_DIR/operator_time_distribution.png"

# Also create a simple CSV report
echo "Creating CSV report..."
python3 -c "
import sys
sys.path.append('.')
from spark_operator_analyzer import SparkEventLogAnalyzer
import csv

analyzer = SparkEventLogAnalyzer('$EVENT_LOG_PATH')
operator_stats, total_time = analyzer.run_analysis()

# Write CSV report
csv_file = '$OUTPUT_DIR/operator_time_report.csv'
with open(csv_file, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['Operator', 'Time_Nanoseconds', 'Time_Milliseconds', 'Time_Seconds', 'Percentage'])
    
    sorted_operators = sorted(operator_stats.items(), key=lambda x: x[1]['percentage'], reverse=True)
    for operator, stats in sorted_operators:
        writer.writerow([
            operator,
            stats['time_ns'],
            f\"{stats['time_ms']:.3f}\",
            f\"{stats['time_seconds']:.3f}\",
            f\"{stats['percentage']:.2f}\"
        ])

print(f'CSV report saved to: {csv_file}')
"

echo "- CSV report: $OUTPUT_DIR/operator_time_report.csv"
