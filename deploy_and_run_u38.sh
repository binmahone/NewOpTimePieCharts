#!/bin/bash
# Deploy and run Spark Operator Analyzer on u38 machine
# Usage: ./deploy_and_run_u38.sh [event_log_path]

set -e  # Exit on any error

# Configuration
U38_HOST="u38"
U38_BASE_DIR="/data/mahone/spark_analyzer"
DEFAULT_EVENT_LOG="/data/spark/history/app-20250918060750-0000"

# Use provided event log path or default
EVENT_LOG_PATH="${1:-$DEFAULT_EVENT_LOG}"

echo "=============================================================="
echo "Spark Operator Analyzer - U38 Deployment and Analysis"
echo "=============================================================="
echo "Target machine: $U38_HOST"
echo "Target directory: $U38_BASE_DIR"
echo "Event log: $EVENT_LOG_PATH"
echo ""

# Step 1: Create directory on u38
echo "[1/6] Creating directory on u38..."
ssh $U38_HOST "mkdir -p $U38_BASE_DIR"

# Step 2: Copy program files to u38
echo "[2/6] Copying program files to u38..."
scp spark_operator_analyzer.py requirements.txt analyze_spark_logs.sh README.md $U38_HOST:$U38_BASE_DIR/

# Step 3: Setup Python virtual environment on u38
echo "[3/6] Setting up Python virtual environment on u38..."
ssh $U38_HOST "cd $U38_BASE_DIR && python3 -m venv venv"

# Step 4: Install dependencies
echo "[4/6] Installing Python dependencies on u38..."
ssh $U38_HOST "cd $U38_BASE_DIR && source venv/bin/activate && pip install -r requirements.txt"

# Step 5: Verify event log exists
echo "[5/6] Verifying event log exists..."
if ssh $U38_HOST "[ -f '$EVENT_LOG_PATH' ]"; then
    EVENT_LOG_SIZE=$(ssh $U38_HOST "ls -lh '$EVENT_LOG_PATH' | awk '{print \$5}'")
    echo "Event log found: $EVENT_LOG_PATH ($EVENT_LOG_SIZE)"
else
    echo "Error: Event log not found at $EVENT_LOG_PATH"
    echo "Available event logs:"
    ssh $U38_HOST "ls -lh /data/spark/history/ | head -10"
    exit 1
fi

# Step 6: Run analysis
echo "[6/6] Running Spark operator analysis..."

# Create results directory on u38
ssh $U38_HOST "mkdir -p $U38_BASE_DIR/results"

# Use fixed filenames
OUTPUT_FILE="results/spark_analysis.png"

echo "Starting analysis... This may take several minutes for large files."
echo "Output will be saved as: $OUTPUT_FILE"

# Run the analysis with timing
START_TIME=$(date +%s)
ssh $U38_HOST "cd $U38_BASE_DIR && source venv/bin/activate && python3 spark_operator_analyzer.py '$EVENT_LOG_PATH' --output '$OUTPUT_FILE' --verbose"
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo ""
echo "Analysis completed in ${DURATION} seconds!"

# Step 7: Copy results back to local machine
echo "Copying results back to local machine..."

# Create local results directory if it doesn't exist
mkdir -p results

# Copy files with fixed names
scp $U38_HOST:$U38_BASE_DIR/$OUTPUT_FILE results/
scp $U38_HOST:$U38_BASE_DIR/results/spark_analysis_report.txt results/

echo ""
echo "=============================================================="
echo "Analysis Complete!"
echo "=============================================================="
echo "Files copied to local machine:"
echo "- Pie chart: results/spark_analysis.png"
echo "- Text report: results/spark_analysis_report.txt"
echo ""
echo "Analysis summary:"
ssh $U38_HOST "cd $U38_BASE_DIR && tail -20 results/spark_analysis_report.txt | head -10"
echo ""
echo "To view detailed results, check the files above."
echo "=============================================================="
