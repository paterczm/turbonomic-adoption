#!/usr/bin/env python3
"""
Turbonomic Time Bucket Analyzer

This script analyzes Turbonomic commodity changes over time buckets (e.g., weekly periods)
to show how Total Impact Changes evolve over time. It uses the existing turbonomic_commodity_analyzer.py
as a library to perform analysis on each time bucket.

Output: CSV with columns: from, to, VCPU, VCPURequest, VMem, VMemRequest
"""

import os
import sys
import argparse
import csv
import subprocess
import tempfile
import json
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
import pandas as pd

# Add current directory to path to import the analyzer
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from turbonomic_commodity_analyzer import CommodityAnalyzer
except ImportError:
    print("Error: Could not import turbonomic_commodity_analyzer.py")
    print("Make sure turbonomic_commodity_analyzer.py is in the same directory.")
    sys.exit(1)

def parse_time_duration(duration_str: str) -> timedelta:
    """
    Parse a time duration string into a timedelta object using pandas.Timedelta.
    
    Supported formats (much more flexible than our previous parser):
    - '7d', '7 days' -> 7 days
    - '24h', '24 hours' -> 24 hours  
    - '30m', '30 minutes' -> 30 minutes
    - '60s', '60 seconds' -> 60 seconds
    - '1.5h', '1.5 hours' -> 1.5 hours
    - '2d 4h', '2 days 4 hours' -> 2 days 4 hours
    - And many more human-friendly formats
    
    Args:
        duration_str: Time duration string to parse
        
    Returns:
        timedelta object
        
    Raises:
        ValueError: If the duration string cannot be parsed
    """
    try:
        # Use pandas.Timedelta for robust parsing
        pd_timedelta = pd.Timedelta(duration_str)
        # Convert to standard Python timedelta
        return timedelta(seconds=pd_timedelta.total_seconds())
    except (ValueError, TypeError) as e:
        raise ValueError(f"Invalid time duration format: '{duration_str}'. "
                        f"Expected format like '7d', '24h', '30m', '60s', '2h 30m', etc. "
                        f"Error: {str(e)}")

class TimeBucketAnalyzer:
    def __init__(self, csv_file_path: str, bucket_size: str = "7d", cluster_filters: Optional[List[str]] = None):
        self.csv_file_path = csv_file_path
        self.bucket_duration = parse_time_duration(bucket_size)
        self.bucket_size = bucket_size  # Keep original string for display
        self.cluster_filters = cluster_filters or []
        self.bucket_results = []
        
    def find_time_range(self) -> Tuple[datetime, datetime]:
        """Find the overall time range of the data."""
        print(f"Analyzing time range in {self.csv_file_path}...")
        
        analyzer = CommodityAnalyzer(self.csv_file_path, self.cluster_filters)
        analyzer.load_data()
        
        if not analyzer.actions:
            raise ValueError("No valid actions found in the data")
        
        # Find min and max execution times
        min_time = min(action.execution_datetime for action in analyzer.actions)
        max_time = max(action.execution_datetime for action in analyzer.actions)
        
        print(f"Data spans from {min_time.strftime('%Y-%m-%d %H:%M')} to {max_time.strftime('%Y-%m-%d %H:%M')}")
        print(f"Using {self.bucket_size} buckets")
        return min_time, max_time
    
    def generate_time_buckets(self, start_time: datetime, end_time: datetime) -> List[Tuple[datetime, datetime]]:
        """Generate time buckets of specified size."""
        buckets = []
        current_start = start_time
        
        while current_start < end_time:
            current_end = current_start + self.bucket_duration
            if current_end > end_time:
                current_end = end_time
            
            buckets.append((current_start, current_end))
            current_start = current_end
        
        print(f"Generated {len(buckets)} time buckets of {self.bucket_size} each")
        return buckets
    
    def analyze_bucket(self, bucket_start: datetime, bucket_end: datetime) -> Dict[str, float]:
        """Analyze a single time bucket and return commodity impact changes."""
        print(f"  Analyzing bucket: {bucket_start.strftime('%Y-%m-%d')} to {bucket_end.strftime('%Y-%m-%d')}")
        
        # Create analyzer with time filters
        from_date_str = bucket_start.strftime("%d %b %Y %H:%M")
        to_date_str = bucket_end.strftime("%d %b %Y %H:%M")
        
        analyzer = CommodityAnalyzer(
            self.csv_file_path, 
            self.cluster_filters, 
            None,  # namespace_filters (not used in bucket analysis)
            from_date_str, 
            to_date_str
        )
        
        # Load and analyze data for this bucket
        analyzer.load_data()
        
        if not analyzer.actions:
            print(f"    No actions in this time bucket")
            return {
                'from': bucket_start,
                'to': bucket_end,
                'VCPU': 0.0,
                'VCPURequest': 0.0,
                'VMem': 0.0,
                'VMemRequest': 0.0
            }
        
        analyzer.group_data()
        results = analyzer.analyze_changes()
        
        # Calculate total impact changes for each commodity
        commodity_totals = {
            'VCPU': 0.0,
            'VCPURequest': 0.0,
            'VMem': 0.0,
            'VMemRequest': 0.0
        }
        
        for result in results:
            for commodity in commodity_totals.keys():
                commodity_totals[commodity] += result.get(f'{commodity}_change', 0.0)
        
        print(f"    Results: VCPU={commodity_totals['VCPU']:+.0f}, VCPURequest={commodity_totals['VCPURequest']:+.0f}, VMem={commodity_totals['VMem']/1048576:+.2f}GiB, VMemRequest={commodity_totals['VMemRequest']/1048576:+.2f}GiB")
        
        return {
            'from': bucket_start,
            'to': bucket_end,
            'VCPU': commodity_totals['VCPU'],
            'VCPURequest': commodity_totals['VCPURequest'],
            'VMem': commodity_totals['VMem'],
            'VMemRequest': commodity_totals['VMemRequest']
        }
    
    def analyze_all_buckets(self) -> List[Dict]:
        """Analyze all time buckets."""
        print("Starting time bucket analysis...")
        
        # Find overall time range
        start_time, end_time = self.find_time_range()
        
        # Generate time buckets
        buckets = self.generate_time_buckets(start_time, end_time)
        
        # Analyze each bucket
        results = []
        for i, (bucket_start, bucket_end) in enumerate(buckets, 1):
            print(f"Bucket {i}/{len(buckets)}:")
            bucket_result = self.analyze_bucket(bucket_start, bucket_end)
            results.append(bucket_result)
        
        return results
    
    def export_to_csv(self, results: List[Dict], output_file: str) -> None:
        """Export time bucket results to CSV."""
        print(f"Exporting results to {output_file}...")
        
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['from', 'to', 'VCPU', 'VCPURequest', 'VMem', 'VMemRequest']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            
            for result in results:
                csv_row = {
                    'from': result['from'].strftime('%Y-%m-%d %H:%M'),
                    'to': result['to'].strftime('%Y-%m-%d %H:%M'),
                    'VCPU': f"{result['VCPU']:.0f}",
                    'VCPURequest': f"{result['VCPURequest']:.0f}",
                    'VMem': f"{result['VMem']/1048576:.2f}",  # Convert KB to GiB
                    'VMemRequest': f"{result['VMemRequest']/1048576:.2f}"  # Convert KB to GiB
                }
                writer.writerow(csv_row)
        
        print(f"✅ Time bucket analysis exported to {output_file}")
    
    def generate_summary_report(self, results: List[Dict]) -> str:
        """Generate a summary report of the time bucket analysis."""
        if not results:
            return "No results to report."
        
        report_lines = []
        report_lines.append("TURBONOMIC TIME BUCKET ANALYSIS SUMMARY")
        report_lines.append("=" * 50)
        report_lines.append(f"Analysis Period: {results[0]['from'].strftime('%Y-%m-%d')} to {results[-1]['to'].strftime('%Y-%m-%d')}")
        report_lines.append(f"Bucket Size: {self.bucket_size}")
        report_lines.append(f"Total Buckets: {len(results)}")
        if self.cluster_filters:
            report_lines.append(f"Filtered Clusters: {', '.join(self.cluster_filters)}")
        report_lines.append("")
        
        # Calculate totals and trends
        commodity_types = ['VCPU', 'VCPURequest', 'VMem', 'VMemRequest']
        
        report_lines.append("BUCKET-BY-BUCKET TRENDS")
        report_lines.append("-" * 30)
        
        for i, result in enumerate(results, 1):
            bucket_str = f"Bucket {i}: {result['from'].strftime('%m/%d')} - {result['to'].strftime('%m/%d')}"
            report_lines.append(bucket_str)
            
            for commodity in commodity_types:
                value = result[commodity]
                if commodity in ['VMem', 'VMemRequest']:
                    display_value = f"{value/1048576:+8.2f} GiB"
                else:
                    display_value = f"{value:+8.0f} mc"
                report_lines.append(f"  {commodity:>12}: {display_value}")
            report_lines.append("")
        
        # Overall totals
        report_lines.append("TOTAL IMPACT ACROSS ALL BUCKETS")
        report_lines.append("-" * 35)
        
        for commodity in commodity_types:
            total = sum(result[commodity] for result in results)
            if commodity in ['VMem', 'VMemRequest']:
                display_value = f"{total/1048576:+.2f} GiB"
            else:
                display_value = f"{total:+.0f} mc"
            report_lines.append(f"{commodity:>12}: {display_value}")
        
        return "\\n".join(report_lines)

def main():
    parser = argparse.ArgumentParser(
        description='Analyze Turbonomic commodity changes over time buckets',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Weekly analysis (default)
  python turbonomic_time_bucket_analyzer.py data.csv -o weekly_trends.csv
  
  # Daily analysis
  python turbonomic_time_bucket_analyzer.py data.csv -o daily_trends.csv --bucket-size 1d

  # Monthly analysis with cluster filtering
  python turbonomic_time_bucket_analyzer.py data.csv -o monthly_trends.csv --bucket-size 30d --cluster prod-cluster-1
  
  # With summary report
  python turbonomic_time_bucket_analyzer.py data.csv -o trends.csv -r summary_report.txt
        """
    )
    
    parser.add_argument('csv_file', help='Path to the CSV file containing Turbonomic actions')
    parser.add_argument('-o', '--output', required=True, help='Output CSV file for time bucket results')
    parser.add_argument('-r', '--report', help='Output file for summary report (optional)')
    parser.add_argument('--bucket-size', type=str, default='7d', help='Size of time buckets (default: 7d). Supports flexible formats: 24h, 1d, 30m, 60s, "2h 30m", "1 day", etc.')
    parser.add_argument('--cluster', action='append', help='Filter by specific cluster(s). Can be used multiple times.')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose output')
    
    args = parser.parse_args()
    
    # Validate inputs
    if not os.path.exists(args.csv_file):
        print(f"Error: Input file '{args.csv_file}' not found.")
        sys.exit(1)
    
    try:
        # Validate bucket size format early
        parse_time_duration(args.bucket_size)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)
    
    try:
        # Create analyzer
        analyzer = TimeBucketAnalyzer(
            args.csv_file, 
            args.bucket_size, 
            args.cluster
        )
        
        # Perform analysis
        results = analyzer.analyze_all_buckets()
        
        if not results:
            print("No data found for analysis.")
            sys.exit(1)
        
        # Export to CSV
        analyzer.export_to_csv(results, args.output)
        
        # Generate summary report if requested
        if args.report:
            summary = analyzer.generate_summary_report(results)
            with open(args.report, 'w', encoding='utf-8') as f:
                f.write(summary)
            print(f"📊 Summary report saved to {args.report}")
        
        # Print brief summary to console
        total_buckets = len(results)
        print(f"\\n📈 Analysis Complete!")
        print(f"   • Processed {total_buckets} time buckets of {args.bucket_size} each")
        print(f"   • Results exported to {args.output}")
        if args.report:
            print(f"   • Summary report saved to {args.report}")
        
    except Exception as e:
        print(f"Error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
