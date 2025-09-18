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
        self.sister_metric_records = []  # For excl. SemWait metrics
        self.operator_mappings = []
        self.sister_operator_mappings = []  # For excl. SemWait operator mappings
        self.total_executor_run_time = 0
        self.total_semwait_time = 0  # Total gpuSemaphoreWait time
        self.semwait_record_count = 0  # Count of gpuSemaphoreWait records
        
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
            
            # Include original op time metrics
            if self._is_time_metric(metric_name) and accumulator_id is not None:
                mappings.append(OperatorMapping(
                    accumulator_id=accumulator_id,
                    operator_name=node_name,
                    metric_name=metric_name
                ))
            
            # Include sister op time metrics (excl. SemWait)
            elif self._is_sister_time_metric(metric_name) and accumulator_id is not None:
                self.sister_operator_mappings.append(OperatorMapping(
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
        
        self.logger.info(f"Parsed {len(self.metric_records)} original metric records")
        self.logger.info(f"Parsed {len(self.sister_metric_records)} sister metric records")
        self.logger.info(f"Found {len(self.operator_mappings)} original operator mappings")
        self.logger.info(f"Found {len(self.sister_operator_mappings)} sister operator mappings")
        self.logger.info(f"Total semWait time: {self.total_semwait_time / 1_000_000_000:.3f} seconds")
        self.logger.info(f"Total gpuSemaphoreWait records: {self.semwait_record_count}")
    
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
            
            # Process original op time metrics
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
            
            # Process sister op time metrics (excl. SemWait)
            elif self._is_sister_time_metric(name) and update_value and accumulator_id:
                try:
                    op_time_ns = self._parse_time_value(update_value)
                    if op_time_ns > 0:  # Only add positive time values
                        self.sister_metric_records.append(MetricRecord(
                            accumulator_id=accumulator_id,
                            op_time_delta=op_time_ns,
                            stage_id=stage_id
                        ))
                except (ValueError, TypeError):
                    continue
            
            # Process gpuSemaphoreWait - use Update field
            elif name == 'gpuSemaphoreWait' and update_value:
                self.semwait_record_count += 1  # Count all gpuSemaphoreWait records
                try:
                    semwait_ns = self._parse_time_string(update_value)
                    if semwait_ns > 0:
                        self.total_semwait_time += semwait_ns
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
    
    def _is_sister_time_metric(self, metric_name):
        """Check if a metric name represents a sister op time measurement (excl. SemWait)."""
        # Accept sister metrics with " (excl. SemWait)" suffix
        sister_time_patterns = [
            'op time (excl. SemWait)',
            'op time (shuffle read) (excl. SemWait)',
            'op time (shuffle write partition & serial) (excl. SemWait)',
            'shuffle write time (excl. SemWait)'
        ]
        metric_lower = metric_name.lower()
        return any(pattern.lower() == metric_lower for pattern in sister_time_patterns)
    
    def _parse_time_value(self, time_value):
        """Parse time value from nanoseconds (no conversion needed)."""
        try:
            # op time Update values are already in nanoseconds
            nanoseconds = int(float(str(time_value).strip()))
            return nanoseconds
        except (ValueError, TypeError):
            return 0
    
    def _parse_time_string(self, time_string):
        """Parse time string in format '00:00:00.433' to nanoseconds."""
        try:
            time_str = str(time_string).strip()
            if ':' in time_str:
                # Time format like "00:00:00.433"
                time_parts = time_str.split(':')
                if len(time_parts) == 3:
                    hours = int(time_parts[0])
                    minutes = int(time_parts[1])
                    seconds = float(time_parts[2])
                    total_seconds = hours * 3600 + minutes * 60 + seconds
                    return int(total_seconds * 1_000_000_000)  # Convert to nanoseconds
            return 0
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
    
    def calculate_sister_operator_statistics(self):
        """Calculate time statistics for sister operators (excl. SemWait) + semWait."""
        # Create mapping from accumulator ID to operator name
        acc_to_operator = {}
        for mapping in self.sister_operator_mappings:
            acc_to_operator[mapping.accumulator_id] = mapping.operator_name
        
        # Aggregate time by operator
        operator_times = defaultdict(int)
        unmapped_accumulators = set()
        
        for record in self.sister_metric_records:
            operator_name = acc_to_operator.get(record.accumulator_id)
            if operator_name:
                operator_times[operator_name] += record.op_time_delta
            else:
                unmapped_accumulators.add(record.accumulator_id)
        
        if unmapped_accumulators:
            self.logger.warning(f"Found {len(unmapped_accumulators)} unmapped sister accumulator IDs: "
                              f"{sorted(unmapped_accumulators)}")
            # Also show what operators we DO have mappings for
            mapped_operators = set(acc_to_operator.values())
            self.logger.info(f"Found sister mappings for operators: {sorted(mapped_operators)}")
        
        # Calculate total sister op time (sum of all operator times)
        total_sister_op_time = sum(operator_times.values())
        
        # Use total executor run time as the baseline for percentage calculation
        total_baseline_time_us = self.total_executor_run_time  # microseconds 
        total_sister_op_time_us = total_sister_op_time / 1_000  # Convert from nanoseconds to microseconds
        semwait_time_us = self.total_semwait_time / 1_000  # Convert from nanoseconds to microseconds
        
        # Combined sister + semwait time
        combined_time_us = total_sister_op_time_us + semwait_time_us
        
        # Log detailed statistics
        self.logger.info(f"Total sister metric records processed: {len(self.sister_metric_records)}")
        self.logger.info(f"Total sister operator mappings: {len(self.sister_operator_mappings)}")
        self.logger.info(f"Total semWait time (μs): {semwait_time_us:.1f}")
        self.logger.info(f"Total sister operator time (μs): {total_sister_op_time_us:.1f}")
        self.logger.info(f"Combined sister + semWait time (μs): {combined_time_us:.1f}")
        
        # Determine the basis for percentage calculation
        use_executor_baseline = combined_time_us <= total_baseline_time_us
        percentage_base = total_baseline_time_us if use_executor_baseline else combined_time_us
        
        self.logger.info(f"Using {'executor time' if use_executor_baseline else 'combined time total'} as sister percentage baseline")
        
        # Calculate percentages for operators
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
        
        # Add semWait category
        if self.total_semwait_time > 0:
            semwait_percentage = (semwait_time_us / percentage_base * 100) if percentage_base > 0 else 0
            operator_stats['semWait'] = {
                'time_ns': self.total_semwait_time,
                'time_us': semwait_time_us,
                'time_ms': semwait_time_us / 1_000,
                'time_seconds': self.total_semwait_time / 1_000_000_000,
                'percentage': semwait_percentage
            }
        
        # Add "Others" category ONLY if combined time < executor time
        if use_executor_baseline and combined_time_us < total_baseline_time_us:
            others_time_us = total_baseline_time_us - combined_time_us
            others_percentage = (others_time_us / percentage_base * 100)
            
            operator_stats['Others'] = {
                'time_ns': int(others_time_us * 1_000),  # Convert microseconds to nanoseconds
                'time_us': others_time_us,
                'time_ms': others_time_us / 1_000,
                'time_seconds': others_time_us / 1_000_000,
                'percentage': others_percentage
            }
            
            self.logger.info(f"Added 'Others' category to sister chart: {others_time_us:.1f} μs ({others_percentage:.2f}%)")
        
        return operator_stats, total_sister_op_time
    
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
    
    def generate_sister_report(self, sister_stats, total_sister_time):
        """Generate and print a detailed report for sister metrics + semWait."""
        # Convert from microseconds to seconds for display
        total_executor_time_sec = self.total_executor_run_time / 1_000_000
        total_sister_time_sec = total_sister_time / 1_000_000_000
        total_semwait_sec = self.total_semwait_time / 1_000_000_000
        
        print(f"\nTotal Executor Run Time: {total_executor_time_sec:.3f} seconds")
        print(f"Total Sister Operator Time: {total_sister_time_sec:.3f} seconds")
        print(f"Total SemWait Time: {total_semwait_sec:.3f} seconds")
        
        combined_time = total_sister_time_sec + total_semwait_sec
        print(f"Combined Sister + SemWait Time: {combined_time:.3f} seconds")
        
        # Calculate which time baseline we're using for percentages (in microseconds)
        baseline_time_us = self.total_executor_run_time
        sister_time_us = total_sister_time / 1_000
        semwait_time_us = self.total_semwait_time / 1_000
        combined_time_us = sister_time_us + semwait_time_us
        
        if baseline_time_us > 0:
            if combined_time_us > baseline_time_us:
                print(f"Note: Combined time exceeds executor time (likely due to parallel execution)")
                print(f"Percentages calculated based on executor run time: {baseline_time_us:.1f} μs")
            else:
                gap_us = baseline_time_us - combined_time_us
                gap_percentage = (gap_us / baseline_time_us * 100)
                print(f"Gap between combined time and executor time: {gap_us:.1f} μs ({gap_percentage:.1f}%)")
        
        # Count actual operators (exclude Others and semWait if present)
        num_operators = len([op for op in sister_stats.keys() if op not in ['Others', 'semWait']])
        print(f"\nNumber of Sister Operators: {num_operators}")
        if 'semWait' in sister_stats:
            print(f"Including semWait category")
        if 'Others' in sister_stats:
            print(f"Including 'Others' category: {len(sister_stats)} total categories")
        
        print(f"Number of Sister Metric Records: {len(self.sister_metric_records)}")
        print(f"Number of Sister Operator Mappings: {len(self.sister_operator_mappings)}")
        
        # Determine what baseline was used for percentage calculation
        baseline_type = "executor time" if combined_time_us <= baseline_time_us else "combined time total"
        
        print("\n" + "-"*80)
        print(f"SISTER OPERATOR + SEMWAIT BREAKDOWN (% based on {baseline_type})")
        print("-"*80)
        print(f"{'Component':<30} {'Time (ms)':<12} {'Time (s)':<10} {'Percentage':<12}")
        print("-"*80)
        
        # Sort by percentage descending, but put semWait and Others at the end
        operators_without_special = {k: v for k, v in sister_stats.items() 
                                   if k not in ['Others', 'semWait']}
        sorted_operators = sorted(operators_without_special.items(), 
                                key=lambda x: x[1]['percentage'], reverse=True)
        
        # Add semWait if it exists
        if 'semWait' in sister_stats:
            sorted_operators.append(('semWait', sister_stats['semWait']))
        
        # Add Others at the end if it exists
        if 'Others' in sister_stats:
            sorted_operators.append(('Others', sister_stats['Others']))
        
        for component, stats in sorted_operators:
            print(f"{component:<30} {stats['time_ms']:<12.3f} "
                  f"{stats['time_seconds']:<10.3f} {stats['percentage']:<12.2f}%")
    
    def create_dual_pie_charts(self, original_stats, sister_stats, output_path=None):
        """Create dual pie charts: original op time vs sister metrics + semWait."""
        if not original_stats or not sister_stats:
            self.logger.warning("No operator statistics to visualize")
            return
        
        # Create figure with two subplots
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 10))
        
        # Chart 1: Original op time metrics
        self._create_single_pie_chart(original_stats, ax1, 
                                    "Original Op Time Distribution", 
                                    plt.cm.Set3)
        
        # Chart 2: Sister metrics + semWait
        self._create_single_pie_chart(sister_stats, ax2, 
                                    "Op Time (excl. SemWait) + SemWait Distribution", 
                                    plt.cm.Set2)
        
        plt.tight_layout()
        
        # Save the chart
        if output_path:
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            self.logger.info(f"Dual pie charts saved to: {output_path}")
        
        plt.show()
    
    def _create_single_pie_chart(self, operator_stats, ax, title, colormap):
        """Create a single pie chart on the given axis."""
        # Prepare data for pie chart
        operators = []
        percentages = []
        
        # Sort by percentage descending, but put Others at the end
        operators_without_others = {k: v for k, v in operator_stats.items() 
                                  if k not in ['Others', 'semWait']}
        sorted_operators = sorted(operators_without_others.items(), 
                                key=lambda x: x[1]['percentage'], reverse=True)
        
        # Add semWait if it exists
        if 'semWait' in operator_stats:
            sorted_operators.append(('semWait', operator_stats['semWait']))
        
        # Add Others at the end if it exists
        if 'Others' in operator_stats:
            sorted_operators.append(('Others', operator_stats['Others']))
        
        for operator, stats in sorted_operators:
            operators.append(operator)
            percentages.append(stats['percentage'])
        
        # Use a color palette
        colors = colormap(range(len(operators)))
        
        # Highlight special categories
        color_list = list(colors)
        for i, operator in enumerate(operators):
            if operator == 'semWait':
                color_list[i] = '#FFD700'  # Gold for semWait
            elif operator == 'Others':
                color_list[i] = '#CCCCCC'  # Gray for Others
        
        wedges, texts, autotexts = ax.pie(percentages, 
                                        labels=operators, 
                                        autopct='%1.1f%%',
                                        colors=color_list,
                                        startangle=90)
        
        # Enhance the appearance
        ax.set_title(title, fontsize=14, fontweight='bold')
        
        # Make percentage text more readable
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
            autotext.set_fontsize(10)
        
        # Highlight special categories with borders
        for i, (operator, wedge) in enumerate(zip(operators, wedges)):
            if operator in ['semWait', 'Others']:
                wedge.set_edgecolor('black')
                wedge.set_linewidth(2)
        
        ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle
        
        # Add a legend outside the pie chart
        ax.legend(wedges, [f"{op}: {stats['time_seconds']:.3f}s" 
                          for op, stats in sorted_operators],
                 title="Components",
                 loc="center left",
                 bbox_to_anchor=(1, 0, 0.5, 1),
                 fontsize=9)
    
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
        """Run the complete analysis pipeline with dual pie charts."""
        self.parse_event_log()
        
        # Calculate both original and sister operator statistics
        operator_stats, total_op_time = self.calculate_operator_statistics()
        sister_stats, total_sister_time = self.calculate_sister_operator_statistics()
        
        # Generate reports for both
        print("\n" + "="*80)
        print("ORIGINAL OP TIME ANALYSIS")
        print("="*80)
        self.generate_report(operator_stats, total_op_time)
        
        print("\n" + "="*80) 
        print("SISTER METRICS (EXCL. SEMWAIT) + SEMWAIT ANALYSIS")
        print("="*80)
        self.generate_sister_report(sister_stats, total_sister_time)
        
        # Create dual pie charts
        self.create_dual_pie_charts(operator_stats, sister_stats, output_chart_path)
        
        return operator_stats, sister_stats, total_op_time, total_sister_time


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
