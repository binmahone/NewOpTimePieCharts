#!/usr/bin/env python3
"""
Spark Event Log Analyzer - Operator Time Distribution
Analyzes Spark event logs to extract operator time statistics and create pie chart visualization.
"""

import json
import argparse
import logging
from collections import defaultdict, namedtuple
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# Data structures
MetricRecord = namedtuple('MetricRecord', ['accumulator_id', 'op_time_delta', 'stage_id'])
OperatorMapping = namedtuple('OperatorMapping', ['accumulator_id', 'operator_name', 'metric_name'])

class SparkEventLogAnalyzer:
    """Analyzes Spark event logs to extract operator timing information."""
    
    def __init__(self, event_log_path):
        self.event_log_path = Path(event_log_path)
        self.metric_records = []
        self.operator_mappings = []
        self.total_executor_run_time = 0
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, 
                          format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
    
    def parse_spark_plan_recursive(self, spark_plan_info, mappings):
        """
        Recursively parse sparkPlanInfo to extract operator mappings.
        
        Args:
            spark_plan_info: The sparkPlanInfo JSON object
            mappings: List to collect OperatorMapping objects
        """
        node_name = spark_plan_info.get('nodeName', '')
        metrics = spark_plan_info.get('metrics', [])
        
        # Extract metrics for this node
        for metric in metrics:
            metric_name = metric.get('name', '')
            accumulator_id = metric.get('accumulatorId')
            
            # Include all time-related metrics
            if self._is_time_metric(metric_name) and accumulator_id is not None:
                mappings.append(OperatorMapping(
                    accumulator_id=accumulator_id,
                    operator_name=node_name,
                    metric_name=metric_name
                ))
        
        # Recursively process children
        children = spark_plan_info.get('children', [])
        for child in children:
            self.parse_spark_plan_recursive(child, mappings)
    
    def parse_event_log(self):
        """Parse the entire event log to extract metrics and operator mappings."""
        self.logger.info(f"Parsing event log: {self.event_log_path}")
        
        try:
            with open(self.event_log_path, 'r', encoding='utf-8') as file:
                for line_num, line in enumerate(file, 1):
                    try:
                        self._process_line(line.strip(), line_num)
                    except Exception as e:
                        self.logger.warning(f"Error processing line {line_num}: {e}")
                        continue
        
        except Exception as e:
            self.logger.error(f"Failed to read event log file: {e}")
            raise
        
        self.logger.info(f"Parsed {len(self.metric_records)} metric records")
        self.logger.info(f"Found {len(self.operator_mappings)} operator mappings")
    
    def _process_line(self, line, line_num):
        """Process a single line from the event log."""
        if not line:
            return
        
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            return
        
        event_type = event.get('Event', '')
        
        if event_type == 'SparkListenerTaskEnd':
            self._process_task_end_event(event)
        elif event_type in ['org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart',
                          'org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate']:
            self._process_sql_execution_event(event)
    
    def _process_task_end_event(self, event):
        """Process SparkListenerTaskEnd event to extract op time metrics."""
        stage_id = event.get('Stage ID')
        task_info = event.get('Task Info', {})
        task_metrics = event.get('Task Metrics', {})
        
        # Extract executor run time (in milliseconds, convert to microseconds for consistency)
        executor_run_time = task_metrics.get('Executor Run Time')
        if executor_run_time:
            # Convert milliseconds to microseconds
            self.total_executor_run_time += float(executor_run_time) * 1_000
        
        # Extract accumulator metrics
        accumulables = task_info.get('Accumulables', [])
        for acc in accumulables:
            name = acc.get('Name', '')
            update_value = acc.get('Update')
            accumulator_id = acc.get('ID')
            
            # Look for time-related metrics (op time, gpuTime, etc.)
            if self._is_time_metric(name) and update_value and accumulator_id:
                try:
                    op_time_ns = self._parse_time_value(update_value)
                    if op_time_ns > 0:  # Only add positive time values
                        self.metric_records.append(MetricRecord(
                            accumulator_id=accumulator_id,
                            op_time_delta=op_time_ns,
                            stage_id=stage_id
                        ))
                except (ValueError, TypeError):
                    continue
    
    def _is_time_metric(self, metric_name):
        """Check if a metric name represents an op time measurement."""
        # Accept "op time" and related shuffle op time metrics
        op_time_patterns = [
            'op time',
            'op time (shuffle read)',
            'op time (shuffle write partition & serial)',
            'shuffle write time'
        ]
        metric_lower = metric_name.lower()
        return any(pattern.lower() == metric_lower for pattern in op_time_patterns)
    
    def _parse_time_value(self, time_value):
        """Parse time value from nanoseconds (no conversion needed)."""
        try:
            # op time Update values are already in nanoseconds
            nanoseconds = int(float(str(time_value).strip()))
            return nanoseconds
        except (ValueError, TypeError):
            return 0
    
    def _process_sql_execution_event(self, event):
        """Process SQL execution events to extract operator mappings."""
        spark_plan_info = event.get('sparkPlanInfo')
        if spark_plan_info:
            self.parse_spark_plan_recursive(spark_plan_info, self.operator_mappings)
    
    def calculate_operator_statistics(self):
        """Calculate time statistics per operator."""
        # Create mapping from accumulator ID to operator name
        acc_to_operator = {}
        for mapping in self.operator_mappings:
            acc_to_operator[mapping.accumulator_id] = mapping.operator_name
        
        # Aggregate time by operator
        operator_times = defaultdict(int)
        unmapped_accumulators = set()
        
        for record in self.metric_records:
            operator_name = acc_to_operator.get(record.accumulator_id)
            if operator_name:
                operator_times[operator_name] += record.op_time_delta
            else:
                unmapped_accumulators.add(record.accumulator_id)
        
        if unmapped_accumulators:
            self.logger.warning(f"Found {len(unmapped_accumulators)} unmapped accumulator IDs: "
                              f"{sorted(unmapped_accumulators)}")
            # Also show what operators we DO have mappings for
            mapped_operators = set(acc_to_operator.values())
            self.logger.info(f"Found mappings for operators: {sorted(mapped_operators)}")
        
        # Calculate total op time (sum of all operator times)
        total_op_time = sum(operator_times.values())
        
        # Use total executor run time as the baseline for percentage calculation
        # total_executor_run_time is converted to microseconds (from ms), op_time is in nanoseconds
        total_baseline_time_us = self.total_executor_run_time  # microseconds 
        total_op_time_us = total_op_time / 1_000  # Convert from nanoseconds to microseconds
        
        # Log detailed statistics
        self.logger.info(f"Total metric records processed: {len(self.metric_records)}")
        self.logger.info(f"Total operator mappings: {len(self.operator_mappings)}")
        self.logger.info(f"Accumulator ID to operator mappings: {len(acc_to_operator)}")
        self.logger.info(f"Operators with time data: {sorted(operator_times.keys())}")
        self.logger.info(f"Total executor run time (μs): {total_baseline_time_us:.1f}")
        self.logger.info(f"Total operator time (μs): {total_op_time_us:.1f}")
        
        # Determine the basis for percentage calculation
        use_executor_baseline = total_op_time_us <= total_baseline_time_us
        percentage_base = total_baseline_time_us if use_executor_baseline else total_op_time_us
        
        self.logger.info(f"Using {'executor time' if use_executor_baseline else 'operator time total'} as percentage baseline")
        
        # Calculate percentages
        operator_stats = {}
        for operator, time_ns in operator_times.items():
            time_us = time_ns / 1_000  # Convert nanoseconds to microseconds
            time_ms = time_ns / 1_000_000  # Convert nanoseconds to milliseconds
            if percentage_base > 0:
                percentage = (time_us / percentage_base * 100)
            else:
                percentage = 0
            operator_stats[operator] = {
                'time_ns': time_ns,
                'time_us': time_us,
                'time_ms': time_ms,
                'time_seconds': time_ns / 1_000_000_000,
                'percentage': percentage
            }
        
        # Add "Others" category ONLY if operator time < executor time
        if use_executor_baseline and total_op_time_us < total_baseline_time_us:
            others_time_us = total_baseline_time_us - total_op_time_us
            others_percentage = (others_time_us / percentage_base * 100)
            
            operator_stats['Others'] = {
                'time_ns': int(others_time_us * 1_000),  # Convert microseconds to nanoseconds
                'time_us': others_time_us,
                'time_ms': others_time_us / 1_000,
                'time_seconds': others_time_us / 1_000_000,
                'percentage': others_percentage
            }
            
            self.logger.info(f"Added 'Others' category: {others_time_us:.1f} μs ({others_percentage:.2f}%)")
        
        return operator_stats, total_op_time
    
    def generate_report(self, operator_stats, total_op_time):
        """Generate and print a detailed report."""
        print("\n" + "="*80)
        print("SPARK OPERATOR TIME ANALYSIS REPORT")
        print("="*80)
        
        # Convert from microseconds to seconds for display
        total_executor_time_sec = self.total_executor_run_time / 1_000_000
        total_op_time_sec = total_op_time / 1_000_000_000
        
        print(f"\nTotal Executor Run Time: {total_executor_time_sec:.3f} seconds")
        print(f"Total Operator Time: {total_op_time_sec:.3f} seconds")
        
        # Calculate which time baseline we're using for percentages (in microseconds)
        baseline_time_us = self.total_executor_run_time
        operator_time_us = total_op_time / 1_000
        
        if baseline_time_us > 0:
            if operator_time_us > baseline_time_us:
                print(f"Note: Operator time exceeds executor time (likely due to parallel execution)")
                print(f"Percentages calculated based on executor run time: {baseline_time_us:.1f} μs")
            else:
                gap_us = baseline_time_us - operator_time_us
                gap_percentage = (gap_us / baseline_time_us * 100)
                print(f"Gap between operator time and executor time: {gap_us:.1f} μs ({gap_percentage:.1f}%)")
        
        # Count actual operators (exclude Others if present)
        num_operators = len([op for op in operator_stats.keys() if op != 'Others'])
        print(f"\nNumber of Operators: {num_operators}")
        if 'Others' in operator_stats:
            print(f"Including 'Others' category: {len(operator_stats)} total categories")
        
        print(f"Number of Metric Records: {len(self.metric_records)}")
        print(f"Number of Operator Mappings: {len(self.operator_mappings)}")
        
        # Determine what baseline was used for percentage calculation
        baseline_type = "executor time" if operator_time_us <= baseline_time_us else "operator time total"
        
        print("\n" + "-"*80)
        print(f"OPERATOR TIME BREAKDOWN (% based on {baseline_type})")
        print("-"*80)
        print(f"{'Operator':<30} {'Time (ms)':<12} {'Time (s)':<10} {'Percentage':<12}")
        print("-"*80)
        
        # Sort by percentage descending, but put Others at the end
        operators_without_others = {k: v for k, v in operator_stats.items() if k != 'Others'}
        sorted_operators = sorted(operators_without_others.items(), 
                                key=lambda x: x[1]['percentage'], reverse=True)
        
        # Add Others at the end if it exists
        if 'Others' in operator_stats:
            sorted_operators.append(('Others', operator_stats['Others']))
        
        for operator, stats in sorted_operators:
            print(f"{operator:<30} {stats['time_ms']:<12.3f} "
                  f"{stats['time_seconds']:<10.3f} {stats['percentage']:<12.2f}%")
    
    def create_pie_chart(self, operator_stats, output_path=None):
        """Create a pie chart visualization of operator time distribution."""
        if not operator_stats:
            self.logger.warning("No operator statistics to visualize")
            return
        
        # Prepare data for pie chart
        operators = []
        percentages = []
        
        # Sort by percentage descending
        sorted_operators = sorted(operator_stats.items(), 
                                key=lambda x: x[1]['percentage'], reverse=True)
        
        for operator, stats in sorted_operators:
            operators.append(operator)
            percentages.append(stats['percentage'])
        
        # Create the pie chart
        plt.figure(figsize=(12, 8))
        
        # Use a color palette
        colors = plt.cm.Set3(range(len(operators)))
        
        wedges, texts, autotexts = plt.pie(percentages, 
                                         labels=operators, 
                                         autopct='%1.1f%%',
                                         colors=colors,
                                         startangle=90)
        
        # Enhance the appearance
        plt.title('Spark Operator Time Distribution', fontsize=16, fontweight='bold')
        
        # Make percentage text more readable
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
        
        plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle
        
        # Add a legend outside the pie chart
        plt.legend(wedges, [f"{op}: {stats['time_seconds']:.3f}s" 
                           for op, stats in sorted_operators],
                  title="Operators",
                  loc="center left",
                  bbox_to_anchor=(1, 0, 0.5, 1))
        
        plt.tight_layout()
        
        # Save the chart
        if output_path:
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            self.logger.info(f"Pie chart saved to: {output_path}")
        
        plt.show()
    
    def run_analysis(self, output_chart_path=None):
        """Run the complete analysis pipeline."""
        self.parse_event_log()
        operator_stats, total_op_time = self.calculate_operator_statistics()
        
        self.generate_report(operator_stats, total_op_time)
        self.create_pie_chart(operator_stats, output_chart_path)
        
        return operator_stats, total_op_time


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description='Analyze Spark event logs for operator time distribution')
    parser.add_argument('event_log_path', 
                       help='Path to the Spark event log file')
    parser.add_argument('--output', '-o', 
                       help='Output path for the pie chart image')
    parser.add_argument('--verbose', '-v', 
                       action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Run the analysis
    analyzer = SparkEventLogAnalyzer(args.event_log_path)
    analyzer.run_analysis(args.output)


if __name__ == '__main__':
    main()
