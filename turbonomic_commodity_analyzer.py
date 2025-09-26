#!/usr/bin/env python3
"""
Turbonomic Commodity Change Analyzer

This script analyzes commodity changes from Turbonomic action CSV files.
It groups actions by cluster, namespace, workload kind, workload name, 
container spec, and commodity, then compares the oldest current value 
with the newest new value to show how commodities have changed over time.
"""

import csv
import sys
import argparse
from datetime import datetime, timedelta
from collections import defaultdict, namedtuple
from typing import Dict, List, Tuple, Optional
import pandas as pd

# Data structure for action records
ActionRecord = namedtuple('ActionRecord', [
    'date_created', 'name', 'cluster', 'replicas', 'namespace', 
    'container_spec', 'commodity', 'resize_direction', 'current_value',
    'new_value', 'change', 'units', 'action_description', 'action_category',
    'risk_description', 'action_mode', 'user_account', 'execution_datetime',
    'execution_status', 'execution_error', 'tags'
])

class CommodityAnalyzer:
    def __init__(self, csv_file_path: str, cluster_filters: Optional[List[str]] = None,
                 namespace_filters: Optional[List[str]] = None,
                 from_date: Optional[str] = None, to_date: Optional[str] = None,
                 show_actions: bool = False, conservative: bool = False, 
                 conservative_days: int = 14):
        self.csv_file_path = csv_file_path
        self.cluster_filters = cluster_filters or []
        self.namespace_filters = namespace_filters or []
        self.from_date = self._parse_filter_datetime(from_date) if from_date else None
        self.to_date = self._parse_filter_datetime(to_date) if to_date else None
        self.show_actions = show_actions
        self.conservative = conservative
        self.conservative_days = conservative_days
        self.actions = []
        self.grouped_data = defaultdict(list)
    
    def _matches_namespace_filter(self, namespace: str) -> bool:
        """Check if namespace matches any of the namespace filters (supports * wildcard suffix)."""
        if not self.namespace_filters:
            return True  # No filters means include all
        
        for filter_pattern in self.namespace_filters:
            if filter_pattern.endswith('*'):
                # Wildcard matching - check if namespace starts with the prefix
                prefix = filter_pattern[:-1]  # Remove the * suffix
                if namespace.startswith(prefix):
                    return True
            else:
                # Exact matching
                if namespace == filter_pattern:
                    return True
        
        return False
        
    def load_data(self) -> None:
        """Load and parse CSV data."""
        print(f"Loading data from {self.csv_file_path}...")
        if self.cluster_filters:
            print(f"Filtering by clusters: {', '.join(self.cluster_filters)}")
        if self.namespace_filters:
            print(f"Filtering by namespaces: {', '.join(self.namespace_filters)}")
        if self.from_date:
            print(f"Filtering from date: {self.from_date.strftime('%d %b %Y %H:%M')}")
        if self.to_date:
            print(f"Filtering to date: {self.to_date.strftime('%d %b %Y %H:%M')}")
        
        try:
            with open(self.csv_file_path, 'r', encoding='utf-8') as file:
                csv_reader = csv.reader(file)
                headers = next(csv_reader)  # Skip header row
                
                for row_num, row in enumerate(csv_reader, start=2):
                    try:
                        if len(row) < 21:  # Ensure we have all expected columns
                            print(f"Warning: Row {row_num} has insufficient columns, skipping...")
                            continue
                            
                        # Parse the action record
                        action = ActionRecord(
                            date_created=row[0],
                            name=row[1],
                            cluster=row[2],
                            replicas=self._parse_int(row[3]),
                            namespace=row[4],
                            container_spec=row[5],
                            commodity=row[6],
                            resize_direction=row[7],
                            current_value=self._parse_float(row[8]),
                            new_value=self._parse_float(row[9]),
                            change=row[10],
                            units=row[11],
                            action_description=row[12],
                            action_category=row[13],
                            risk_description=row[14],
                            action_mode=row[15],
                            user_account=row[16],
                            execution_datetime=self._parse_datetime(row[17]),
                            execution_status=row[18],
                            execution_error=row[19],
                            tags=row[20] if len(row) > 20 else ""
                        )
                        
                        # Filter by cluster if cluster filters are specified
                        if self.cluster_filters:
                            cluster_name = action.cluster
                            # Also check cluster name without Kubernetes- prefix for user convenience
                            cluster_name_short = cluster_name[11:] if cluster_name.startswith('Kubernetes-') else cluster_name
                            if cluster_name not in self.cluster_filters and cluster_name_short not in self.cluster_filters:
                                continue
                        
                        # Filter by namespace if namespace filters are specified
                        if not self._matches_namespace_filter(action.namespace):
                            continue
                        
                        # Filter by time window if specified
                        if action.execution_datetime is not None:
                            if self.from_date and action.execution_datetime < self.from_date:
                                continue
                            if self.to_date and action.execution_datetime > self.to_date:
                                continue
                        
                        # Only include successful actions with valid data
                        if (action.execution_status == 'SUCCEEDED' and 
                            action.current_value is not None and 
                            action.new_value is not None and
                            action.replicas is not None and 
                            action.execution_datetime is not None):
                            self.actions.append(action)
                            
                    except Exception as e:
                        print(f"Warning: Error parsing row {row_num}: {e}")
                        continue
                        
            print(f"Successfully loaded {len(self.actions)} valid action records.")
            
        except FileNotFoundError:
            print(f"Error: File {self.csv_file_path} not found.")
            sys.exit(1)
        except Exception as e:
            print(f"Error loading data: {e}")
            sys.exit(1)
    
    def _parse_float(self, value: str) -> Optional[float]:
        """Parse string to float, return None if invalid."""
        try:
            return float(value) if value.strip() else None
        except (ValueError, AttributeError):
            return None
    
    def _parse_int(self, value: str) -> Optional[int]:
        """Parse string to int, return None if invalid."""
        try:
            return int(value) if value.strip() else None
        except (ValueError, AttributeError):
            return None
    
    def _parse_datetime(self, datetime_str: str) -> Optional[datetime]:
        """Parse datetime string to datetime object."""
        try:
            # Handle the format "16 Sep 2025 09:40"
            return datetime.strptime(datetime_str.strip(), "%d %b %Y %H:%M")
        except ValueError:
            try:
                # Try alternative format if needed
                return datetime.strptime(datetime_str.strip(), "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None
    
    def _parse_filter_datetime(self, datetime_str: str) -> datetime:
        """Parse user-provided datetime string for filtering."""
        try:
            # Handle the format "DD MMM YYYY HH:MM"
            return datetime.strptime(datetime_str.strip(), "%d %b %Y %H:%M")
        except ValueError:
            try:
                # Try alternative format
                return datetime.strptime(datetime_str.strip(), "%Y-%m-%d %H:%M:%S")
            except ValueError:
                print(f"Error: Unable to parse date '{datetime_str}'. Expected format: 'DD MMM YYYY HH:MM' (e.g., '01 Sep 2025 00:00')")
                sys.exit(1)
    
    def _extract_workload_kind(self, action_description: str, name: str) -> str:
        """Extract workload kind from action description or infer from context."""
        description_lower = action_description.lower()
        
        # Look for common workload types in the description
        if 'deployment' in description_lower:
            return 'Deployment'
        elif 'statefulset' in description_lower:
            return 'StatefulSet'
        elif 'daemonset' in description_lower:
            return 'DaemonSet'
        elif 'replicaset' in description_lower:
            return 'ReplicaSet'
        elif 'workload controller' in description_lower:
            return 'WorkloadController'
        else:
            # Default to WorkloadController if we can't determine
            return 'WorkloadController'
    
    def _format_raw_action(self, action: ActionRecord) -> str:
        """Format an action record exactly as it would appear in the original CSV."""
        # Format execution datetime back to string format
        execution_datetime_str = action.execution_datetime.strftime("%d %b %Y %H:%M") if action.execution_datetime else ""

        # Create CSV representation exactly like the original
        fields = [
            action.date_created,
            action.name,
            action.cluster,
            str(action.replicas) if action.replicas is not None else "",
            action.namespace,
            action.container_spec,
            action.commodity,
            action.resize_direction,
            str(action.current_value) if action.current_value is not None else "",
            str(action.new_value) if action.new_value is not None else "",
            action.change,
            action.units,
            action.action_description,
            action.action_category,
            action.risk_description,
            action.action_mode,
            action.user_account,
            execution_datetime_str,
            action.execution_status,
            action.execution_error,
            action.tags
        ]

        # Join with commas exactly as in the original CSV (no modifications or truncation)
        return ",".join(str(field) for field in fields)

    def _calculate_column_widths(self, headers: List[str], results: List[Dict]) -> List[int]:
        """Calculate optimal column widths based on header lengths and data content."""
        # Start with header lengths as minimum widths
        col_widths = [len(header) for header in headers]
        
        # Check each result row to find maximum content width for each column
        for result in results:
            # Format all the data that will be displayed (same logic as in the main function)
            cluster = result['cluster']
            if cluster.startswith('Kubernetes-'):
                cluster = cluster[11:]  # Remove 'Kubernetes-' prefix
            # Apply same max length logic as in main formatting
            
            workload = result['workload_name']
            namespace = result['namespace']
            container = result['container_spec']
            # Note: Truncation will be applied in actual display, we calculate based on full length here
            
            replicas = str(result['replicas'])
            
            # Format commodity changes (same formatting as main function)
            vcpu_change = f"{result['VCPU_change']:+.0f}" if result['VCPU_change'] != 0 else ""
            vcpu_request_change = f"{result['VCPURequest_change']:+.0f}" if result['VCPURequest_change'] != 0 else ""
            vmem_change = f"{result['VMem_change'] / 1048576:+.2f}" if result['VMem_change'] != 0 else ""
            vmem_request_change = f"{result['VMemRequest_change'] / 1048576:+.2f}" if result['VMemRequest_change'] != 0 else ""
            
            # Data values for each column
            row_data = [
                cluster, workload, namespace, container, replicas,
                vcpu_change, vcpu_request_change, vmem_change, vmem_request_change
            ]
            
            # Update maximum width for each column
            for i, data in enumerate(row_data):
                col_widths[i] = max(col_widths[i], len(str(data)))
        
        # Set reasonable minimum widths for readability (must accommodate headers)
        min_widths = [10, 15, 10, 10, 8, 10, 17, 11, 17]  # Minimum widths for each column
        col_widths = [max(current, minimum) for current, minimum in zip(col_widths, min_widths)]
        
        # Allow columns to be as wide as needed to fit the data
        # No maximum width constraints - columns will expand to accommodate content
        
        return col_widths
    
    def group_data(self) -> None:
        """Group actions by cluster, namespace, workload kind, workload name, container spec, and commodity."""
        print("Grouping data by cluster, namespace, workload kind, workload name, container spec, and commodity...")
        
        for action in self.actions:
            workload_kind = self._extract_workload_kind(action.action_description, action.name)
            
            # Create grouping key
            group_key = (
                action.cluster,
                action.namespace, 
                workload_kind,
                action.name,
                action.container_spec,
                action.commodity
            )
            
            self.grouped_data[group_key].append(action)
        
        # Sort each group by execution time
        for group_key in self.grouped_data:
            self.grouped_data[group_key].sort(key=lambda x: x.execution_datetime)
        
        print(f"Created {len(self.grouped_data)} groups.")
    
    def apply_conservative_filtering(self) -> None:
        """Apply conservative filtering to only include workloads with recent actions for any container spec and commodity."""
        if not self.conservative:
            return
        
        # Calculate the cutoff date: conservative_days back from --to or today
        if self.to_date:
            cutoff_date = self.to_date - timedelta(days=self.conservative_days)
        else:
            cutoff_date = datetime.now() - timedelta(days=self.conservative_days)
        
        print(f"Applying conservative filtering: only including workloads with actions since {cutoff_date.strftime('%d %b %Y %H:%M')}")
        
        # Group by workload (without container spec and commodity) to check for recent actions
        workload_recent_actions = {}
        for group_key, actions in self.grouped_data.items():
            cluster, namespace, workload_kind, workload_name, container_spec, commodity = group_key
            workload_key = (cluster, namespace, workload_kind, workload_name)

            # Check if any action in this group is within the conservative timeframe
            has_recent_action = any(action.execution_datetime >= cutoff_date for action in actions)

            # If this workload doesn't have recent actions recorded yet, or if this group has recent actions
            if workload_key not in workload_recent_actions:
                workload_recent_actions[workload_key] = has_recent_action
            else:
                # If any container spec/commodity for this workload has recent actions, mark the workload as having recent actions
                workload_recent_actions[workload_key] = workload_recent_actions[workload_key] or has_recent_action

        # Count unique workloads before filtering
        original_count = len(self.grouped_data)
        original_workloads = len(workload_recent_actions)
        active_workloads = len([active for active in workload_recent_actions.values() if active])

        # Filter grouped_data to only include groups for workloads with recent actions
        filtered_groups = {}
        
        for group_key, actions in self.grouped_data.items():
            cluster, namespace, workload_kind, workload_name, container_spec, commodity = group_key
            workload_key = (cluster, namespace, workload_kind, workload_name)
            
            # Include this group if the workload has recent actions for any container spec/commodity
            if workload_recent_actions.get(workload_key, False):
                filtered_groups[group_key] = actions
        
        self.grouped_data = filtered_groups
        filtered_count = len(self.grouped_data)
        
        print(f"Conservative filtering: {original_count} groups → {filtered_count} groups "
              f"({original_count - filtered_count} groups filtered out)")
        print(f"Workload-level filtering: {original_workloads} total workloads → {active_workloads} workloads with recent actions")
    
    def analyze_changes(self) -> List[Dict]:
        """Analyze commodity changes for each group."""
        print("Analyzing commodity changes...")
        
        # Group by workload (without commodity) to consolidate commodity changes per workload
        workload_groups = defaultdict(lambda: defaultdict(list))
        
        for group_key, actions in self.grouped_data.items():
            if len(actions) < 1:
                continue
                
            cluster, namespace, workload_kind, workload_name, container_spec, commodity = group_key
            workload_key = (cluster, namespace, workload_kind, workload_name, container_spec)
            workload_groups[workload_key][commodity] = actions
        
        results = []
        
        for workload_key, commodity_actions in workload_groups.items():
            cluster, namespace, workload_kind, workload_name, container_spec = workload_key
            
            result = {
                'cluster': cluster,
                'namespace': namespace,
                'workload_kind': workload_kind,
                'workload_name': workload_name,
                'container_spec': container_spec,
                'replicas': None,
                'oldest_date': None,
                'newest_date': None,
                'time_span_days': 0,
                'total_absolute_impact': 0  # For sorting
            }
            
            # Initialize commodity columns
            commodity_types = ['VCPU', 'VCPURequest', 'VMem', 'VMemRequest']
            for commodity in commodity_types:
                result[f'{commodity}_change'] = 0
                result[f'{commodity}_change_pct'] = 0
                result[f'{commodity}_units'] = ''
            
            # Process each commodity for this workload
            earliest_date = None
            latest_date = None
            
            # First pass: find overall oldest and newest actions across ALL commodities for this workload
            all_actions = []
            for commodity, actions in commodity_actions.items():
                all_actions.extend(actions)
            
            # Sort all actions by execution time to find workload-level oldest/newest
            all_actions.sort(key=lambda x: x.execution_datetime)
            workload_oldest_action = all_actions[0]
            workload_newest_action = all_actions[-1]
            
            # Track replicas at workload level (across all commodities)
            if workload_oldest_action.replicas == workload_newest_action.replicas:
                result['replicas'] = workload_oldest_action.replicas
            else:
                result['replicas'] = f"{workload_oldest_action.replicas}→{workload_newest_action.replicas}"
            
            for commodity, actions in commodity_actions.items():
                # Get oldest and newest actions for this commodity
                oldest_action = actions[0]
                newest_action = actions[-1]
                
                # Track time span
                if earliest_date is None or oldest_action.execution_datetime < earliest_date:
                    earliest_date = oldest_action.execution_datetime
                if latest_date is None or newest_action.execution_datetime > latest_date:
                    latest_date = newest_action.execution_datetime
                
                # Calculate total resource impact (per-replica * replica count)
                # Use the workload's most recent replica count for both calculations to show impact at current scale
                oldest_total_impact = oldest_action.current_value * workload_newest_action.replicas
                newest_total_impact = newest_action.new_value * workload_newest_action.replicas
                total_impact_change = newest_total_impact - oldest_total_impact
                total_impact_change_pct = ((newest_total_impact - oldest_total_impact) / oldest_total_impact * 100) if oldest_total_impact != 0 else 0
                
                # Store commodity-specific changes
                result[f'{commodity}_change'] = total_impact_change
                result[f'{commodity}_change_pct'] = total_impact_change_pct
                result[f'{commodity}_units'] = oldest_action.units
                
                # Add to total absolute impact for sorting
                result['total_absolute_impact'] += abs(total_impact_change)
            
            # Set time information
            result['oldest_date'] = earliest_date
            result['newest_date'] = latest_date
            if earliest_date and latest_date:
                result['time_span_days'] = (latest_date - earliest_date).days
            
            results.append(result)
        
        return results
    
    def generate_report(self, results: List[Dict], output_file: Optional[str] = None, show_all: bool = False) -> None:
        """Generate a detailed report of commodity changes."""
        
        if not results:
            print("No results to report.")
            return
        
        # Sort results by VCPURequest change (descending)
        results.sort(key=lambda x: abs(x['VCPURequest_change']), reverse=True)
        
        report_lines = []
        report_lines.append("TURBONOMIC COMMODITY CHANGE ANALYSIS REPORT")
        report_lines.append("=" * 60)
        report_lines.append(f"Total Groups Analyzed: {len(results)}")
        report_lines.append(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        if self.cluster_filters:
            report_lines.append(f"Filtered Clusters: {', '.join(self.cluster_filters)}")
        if self.namespace_filters:
            report_lines.append(f"Filtered Namespaces: {', '.join(self.namespace_filters)}")
        if self.from_date or self.to_date:
            time_filter = "Time Window: "
            if self.from_date:
                time_filter += f"from {self.from_date.strftime('%d %b %Y %H:%M')}"
            if self.from_date and self.to_date:
                time_filter += " to "
            elif self.to_date:
                time_filter += "up to "
            if self.to_date:
                time_filter += f"{self.to_date.strftime('%d %b %Y %H:%M')}"
            report_lines.append(time_filter)
        if self.conservative:
            cutoff_date = (self.to_date or datetime.now()) - timedelta(days=self.conservative_days)
            report_lines.append(f"Conservative Mode: Only workloads with actions for any container spec and commodity since {cutoff_date.strftime('%d %b %Y %H:%M')} ({self.conservative_days} days)")
        report_lines.append("")
        
        # Summary statistics
        total_workloads = len(results)
        workloads_with_changes = len([r for r in results if r['total_absolute_impact'] > 0])
        
        report_lines.append("SUMMARY STATISTICS (Total Impact Considering Replicas)")
        report_lines.append("-" * 55)
        report_lines.append(f"Total Workloads: {total_workloads}")
        report_lines.append(f"Workloads with Changes: {workloads_with_changes}")
        
        # Calculate commodity-specific statistics
        commodity_types = ['VCPU', 'VCPURequest', 'VMem', 'VMemRequest']
        report_lines.append("")
        report_lines.append("COMMODITY-SPECIFIC STATISTICS")
        report_lines.append("-" * 35)
        
        for commodity in commodity_types:
            changes = [r[f'{commodity}_change'] for r in results if r[f'{commodity}_change'] != 0]
            if changes:
                increases = [c for c in changes if c > 0]
                decreases = [c for c in changes if c < 0]
                report_lines.append(f"{commodity}: {len(changes)} changes ({len(increases)} increases, {len(decreases)} decreases)")
        
        report_lines.append("")
        
        # Commodity summary table
        report_lines.append("TOTAL IMPACT CHANGES BY COMMODITY TYPE")
        report_lines.append("-" * 65)
        
        commodity_headers = ["Commodity", "Workloads with Changes", "Total Impact Change"]
        commodity_widths = [15, 22, 25]
        
        commodity_header_line = " | ".join(header.ljust(width) for header, width in zip(commodity_headers, commodity_widths))
        report_lines.append(commodity_header_line)
        report_lines.append("-" * len(commodity_header_line))
        
        for commodity in commodity_types:
            workloads_with_commodity_changes = len([r for r in results if r[f'{commodity}_change'] != 0])
            total_commodity_change = sum(r[f'{commodity}_change'] for r in results)
            
            # Convert memory values to GiB for display
            if commodity in ['VMem', 'VMemRequest']:
                display_value = f"{total_commodity_change / 1048576:+.2f} GiB"
            else:
                display_value = f"{total_commodity_change:+.0f} mc"
            
            commodity_row = [
                commodity.ljust(15),
                str(workloads_with_commodity_changes).ljust(22),
                display_value.ljust(25)
            ]
            commodity_row_line = " | ".join(commodity_row)
            report_lines.append(commodity_row_line)
        
        report_lines.append("")
        if show_all:
            report_lines.append("DETAILED RESULTS TABLE (All results sorted by VCPURequest change)")
        else:
            report_lines.append("DETAILED RESULTS TABLE (Top 10 by VCPURequest change)")
        
        # Table headers with commodity columns (added Cluster at front)
        headers = [
            "Cluster", "Workload", "Namespace", "Container", "Replicas", 
            "VCPU (mc)", "VCPURequest (mc)", "VMem (GiB)", "VMemRequest (GiB)"
        ]
        
        # Detailed results to show
        results_to_show = results if show_all else results[:10]
        
        # Calculate dynamic column widths based on data content and headers
        col_widths = self._calculate_column_widths(headers, results_to_show)
        
        # Calculate total table width for the separator line
        total_width = sum(col_widths) + (len(headers) - 1) * 3  # 3 chars for " | " separators
        report_lines.append("=" * total_width)
        
        # Print table header
        header_line = " | ".join(header.ljust(width) for header, width in zip(headers, col_widths))
        report_lines.append(header_line)
        report_lines.append("-" * len(header_line))
        
        # Detailed results in table format
        for result in results_to_show:
            # Format cluster info - remove Kubernetes- prefix (no truncation)
            cluster = result['cluster']
            if cluster.startswith('Kubernetes-'):
                cluster = cluster[11:]  # Remove 'Kubernetes-' prefix (11 characters)
            
            # Format workload info (no truncation - allow full length)
            workload = result['workload_name']
            namespace = result['namespace']
            container = result['container_spec']
            
            # Format replica info
            replicas = str(result['replicas'])
            
            # Format commodity changes (convert memory from KB to GiB)
            vcpu_change = f"{result['VCPU_change']:+.0f}" if result['VCPU_change'] != 0 else ""
            vcpu_request_change = f"{result['VCPURequest_change']:+.0f}" if result['VCPURequest_change'] != 0 else ""
            # Convert memory from KB to GiB (1 GiB = 1,048,576 KB)
            vmem_change = f"{result['VMem_change'] / 1048576:+.2f}" if result['VMem_change'] != 0 else ""
            vmem_request_change = f"{result['VMemRequest_change'] / 1048576:+.2f}" if result['VMemRequest_change'] != 0 else ""
            
            # Create table row (added cluster at front)
            row_data = [
                cluster, workload, namespace, container, replicas,
                vcpu_change, vcpu_request_change, vmem_change, vmem_request_change
            ]
            
            row_line = " | ".join(data.ljust(width) for data, width in zip(row_data, col_widths))
            report_lines.append(row_line)
        
        # Output report
        report_text = "\n".join(report_lines)
        
        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(report_text)
            print(f"Report saved to {output_file}")
        else:
            print(report_text)
        
        return report_text
    
    def export_to_csv(self, results: List[Dict], output_file: str) -> None:
        """Export results to CSV file."""
        if not results:
            print("No results to export.")
            return
            
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'cluster', 'namespace', 'workload_kind', 'workload_name', 
                'container_spec', 'replicas',
                'VCPU_change_mc', 'VCPU_change_pct', 
                'VCPURequest_change_mc', 'VCPURequest_change_pct', 
                'VMem_change_GiB', 'VMem_change_pct', 
                'VMemRequest_change_GiB', 'VMemRequest_change_pct', 
                'oldest_date', 'newest_date', 'time_span_days', 'total_absolute_impact'
            ]
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for result in results:
                # Create CSV row with only the fields we want
                csv_row = {
                    'cluster': result['cluster'],
                    'namespace': result['namespace'],
                    'workload_kind': result['workload_kind'],
                    'workload_name': result['workload_name'],
                    'container_spec': result['container_spec'],
                    'replicas': result['replicas'],
                    'VCPU_change_mc': result['VCPU_change'],
                    'VCPU_change_pct': result['VCPU_change_pct'],
                    'VCPURequest_change_mc': result['VCPURequest_change'],
                    'VCPURequest_change_pct': result['VCPURequest_change_pct'],
                    'VMem_change_GiB': result['VMem_change'] / 1048576,  # Convert KB to GiB
                    'VMem_change_pct': result['VMem_change_pct'],
                    'VMemRequest_change_GiB': result['VMemRequest_change'] / 1048576,  # Convert KB to GiB
                    'VMemRequest_change_pct': result['VMemRequest_change_pct'],
                    'time_span_days': result['time_span_days'],
                    'total_absolute_impact': result['total_absolute_impact']
                }
                
                # Format dates
                if result['oldest_date']:
                    csv_row['oldest_date'] = result['oldest_date'].strftime('%Y-%m-%d %H:%M:%S')
                else:
                    csv_row['oldest_date'] = ''
                if result['newest_date']:
                    csv_row['newest_date'] = result['newest_date'].strftime('%Y-%m-%d %H:%M:%S')
                else:
                    csv_row['newest_date'] = ''
                
                writer.writerow(csv_row)
        
        print(f"Results exported to {output_file}")

    def show_actions_report(self) -> None:
        """Display detailed information about actions used for calculating resource changes."""
        if not self.show_actions:
            return

        if not self.actions:
            print("No actions available to display.")
            return

        print("\n" + "=" * 80)
        print("DETAILED ACTIONS USED FOR RESOURCE CHANGE CALCULATIONS")
        print("=" * 80)
        print(f"Total Actions Analyzed: {len(self.actions)}")
        print(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # Display filter information
        if self.cluster_filters:
            print(f"Filtered Clusters: {', '.join(self.cluster_filters)}")
        if self.namespace_filters:
            print(f"Filtered Namespaces: {', '.join(self.namespace_filters)}")
        if self.from_date or self.to_date:
            time_filter = "Time Window: "
            if self.from_date:
                time_filter += f"from {self.from_date.strftime('%d %b %Y %H:%M')}"
            if self.from_date and self.to_date:
                time_filter += " to "
            elif self.to_date:
                time_filter += "up to "
            if self.to_date:
                time_filter += f"{self.to_date.strftime('%d %b %Y %H:%M')}"
            print(time_filter)
        if self.conservative:
            cutoff_date = (self.to_date or datetime.now()) - timedelta(days=self.conservative_days)
            print(f"Conservative Mode: Only workloads with actions for any container spec and commodity since {cutoff_date.strftime('%d %b %Y %H:%M')} ({self.conservative_days} days)")

        print("\n" + "-" * 80)
        print("ACTIONS BREAKDOWN BY WORKLOAD")
        print("-" * 80)

        # Group actions by workload for better organization
        workload_actions = defaultdict(list)
        for action in self.actions:
            workload_kind = self._extract_workload_kind(action.action_description, action.name)
            workload_key = (action.cluster, action.namespace, workload_kind, action.name)
            workload_actions[workload_key].append(action)

        # Sort workloads alphabetically
        sorted_workloads = sorted(workload_actions.keys())

        for workload_key in sorted_workloads:
            cluster, namespace, workload_kind, workload_name = workload_key
            actions = workload_actions[workload_key]

            # Display cluster name without Kubernetes- prefix
            display_cluster = cluster[11:] if cluster.startswith('Kubernetes-') else cluster

            print(f"\n{workload_kind}: {workload_name}")
            print(f"  Cluster: {display_cluster}")
            print(f"  Namespace: {namespace}")
            print(f"  Actions: {len(actions)}")

            # Sort actions by execution time
            actions.sort(key=lambda x: x.execution_datetime)

            # Group actions by commodity for this workload
            commodity_actions = defaultdict(list)
            for action in actions:
                commodity_actions[action.commodity].append(action)

            # Display actions by commodity
            for commodity in sorted(commodity_actions.keys()):
                commodity_action_list = commodity_actions[commodity]
                oldest = commodity_action_list[0]
                newest = commodity_action_list[-1]

                print(f"    {commodity}:")
                print(f"      Container: {oldest.container_spec}")
                print(f"      Actions: {len(commodity_action_list)}")
                print(f"      Time Span: {oldest.execution_datetime.strftime('%d %b %Y %H:%M')} -> {newest.execution_datetime.strftime('%d %b %Y %H:%M')}")
                print(f"      Value Change: {oldest.current_value:.2f} -> {newest.new_value:.2f} {oldest.units}")
                print(f"      Replicas: {oldest.replicas} -> {newest.replicas}")

                # Calculate total impact change
                total_impact_change = (newest.new_value * newest.replicas) - (oldest.current_value * oldest.replicas)
                print(f"      Total Impact Change: {total_impact_change:+.2f} {oldest.units}")

                # Show individual actions if there are more than 2
                if len(commodity_action_list) > 2:
                    print(f"      Individual Actions:")
                    for i, action in enumerate(commodity_action_list, 1):
                        print(f"        {i}. {action.execution_datetime.strftime('%d %b %Y %H:%M')}: "
                              f"{action.current_value:.2f} -> {action.new_value:.2f} {action.units} "
                              f"(Replicas: {action.replicas})")

                # Show raw CSV data for this commodity
                print(f"      Raw CSV Actions:")
                for action in commodity_action_list:
                    print(f"        {self._format_raw_action(action)}")

        # Summary by commodity type
        print(f"\n{'-' * 80}")
        print("SUMMARY BY COMMODITY TYPE")
        print(f"{'-' * 80}")

        commodity_summary = defaultdict(list)
        for action in self.actions:
            commodity_summary[action.commodity].append(action)

        for commodity in sorted(commodity_summary.keys()):
            actions = commodity_summary[commodity]
            print(f"{commodity}: {len(actions)} actions")

            # Time range
            actions.sort(key=lambda x: x.execution_datetime)
            earliest = actions[0].execution_datetime
            latest = actions[-1].execution_datetime
            print(f"  Time Range: {earliest.strftime('%d %b %Y %H:%M')} -> {latest.strftime('%d %b %Y %H:%M')}")

            # Unique workloads affected
            unique_workloads = set((a.cluster, a.namespace, a.name) for a in actions)
            print(f"  Unique Workloads: {len(unique_workloads)}")

            # Value range
            values = [a.current_value for a in actions if a.current_value is not None]
            new_values = [a.new_value for a in actions if a.new_value is not None]
            if values and new_values:
                print(f"  Value Range: {min(values):.2f} - {max(values):.2f} -> {min(new_values):.2f} - {max(new_values):.2f} {actions[0].units}")

        # Show all raw actions in CSV format
        print(f"\n{'-' * 80}")
        print("ALL RAW ACTIONS (CSV FORMAT)")
        print(f"{'-' * 80}")
        print("CSV Header:")
        csv_headers = [
            "date_created", "name", "cluster", "replicas", "namespace", "container_spec",
            "commodity", "resize_direction", "current_value", "new_value", "change", "units",
            "action_description", "action_category", "risk_description", "action_mode",
            "user_account", "execution_datetime", "execution_status", "execution_error", "tags"
        ]
        print(",".join(csv_headers))
        print()

        # Sort all actions by execution time for chronological order
        sorted_actions = sorted(self.actions, key=lambda x: x.execution_datetime)

        print(f"Raw Action Data ({len(sorted_actions)} actions):")
        for action in sorted_actions:
            print(self._format_raw_action(action))

def main():
    parser = argparse.ArgumentParser(description='Analyze Turbonomic commodity changes from CSV data')
    parser.add_argument('csv_file', help='Path to the CSV file containing Turbonomic actions')
    parser.add_argument('--output-report', '-r', help='Output file for the text report')
    parser.add_argument('--output-csv', '-c', help='Output CSV file for detailed results')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose output')
    parser.add_argument('--show-all', action='store_true', help='Show all results in detailed table instead of top 10')
    parser.add_argument('--cluster', action='append', help='Filter by specific cluster(s). Can be used multiple times to include multiple clusters. Supports both full names (e.g., "Kubernetes-prod-cluster") and short names (e.g., "prod-cluster").')
    parser.add_argument('--namespace', action='append', help='Filter by specific namespace(s). Can be used multiple times to include multiple namespaces. Supports wildcard suffix (e.g., "app-*" matches "app-prod", "app-test").')
    parser.add_argument('--from', dest='from_date', help='Filter actions from this date onwards (format: "DD MMM YYYY HH:MM", e.g., "01 Sep 2025 00:00")')
    parser.add_argument('--to', dest='to_date', help='Filter actions up to this date (format: "DD MMM YYYY HH:MM", e.g., "30 Sep 2025 23:59")')
    parser.add_argument('--conservative', action='store_true', help='Enable conservative mode: only analyze workloads that have had actions for any container spec and commodity in the last N days (see --conservative-days)')
    parser.add_argument('--conservative-days', type=int, default=14, help='Number of days to look back for recent actions in conservative mode (default: 14)')
    parser.add_argument('--show-actions', action='store_true', help='Show detailed list of actions used for calculating resource changes')
    
    args = parser.parse_args()
    
    # Create analyzer instance
    analyzer = CommodityAnalyzer(args.csv_file, args.cluster, args.namespace, args.from_date, args.to_date, args.show_actions, args.conservative, args.conservative_days)
    
    # Load and process data
    analyzer.load_data()
    analyzer.group_data()
    analyzer.apply_conservative_filtering()
    results = analyzer.analyze_changes()
    
    # Show detailed actions if requested
    analyzer.show_actions_report()

    # Generate reports
    if args.output_report:
        analyzer.generate_report(results, args.output_report, args.show_all)
    else:
        analyzer.generate_report(results, show_all=args.show_all)
    
    if args.output_csv:
        analyzer.export_to_csv(results, args.output_csv)
    
    print(f"\nAnalysis complete! Processed {len(results)} commodity change groups.")

if __name__ == "__main__":
    main()
