#!/usr/bin/env python3
"""
Remove Duplicate Actions Script

This script processes Turbonomic action CSV files to remove consecutive duplicate actions.
A duplicate is defined as an action for the same container spec on the same cluster that has
the same commodity, current value, and new value as the previous consecutive action.

Usage:
    python remove_duplicate_actions.py input.csv output.csv [--report duplicates_report.csv]
"""

import csv
import sys
import argparse
from datetime import datetime
from collections import defaultdict, namedtuple
from enum import Enum
from typing import Dict, List, Tuple, Optional

# Data structure for action records
ActionRecord = namedtuple('ActionRecord', [
    'date_created', 'name', 'cluster', 'replicas', 'namespace', 
    'container_spec', 'commodity', 'resize_direction', 'current_value',
    'new_value', 'change', 'units', 'action_description', 'action_category',
    'risk_description', 'action_mode', 'user_account', 'execution_datetime',
    'execution_status', 'execution_error', 'tags', 'original_row'
])

class DuplicateRemover:
    def __init__(self, input_file: str, output_file: str, duplicates_report: Optional[str] = None, conservative: bool = False):
        self.input_file = input_file
        self.output_file = output_file
        self.duplicates_report = duplicates_report
        self.conservative = conservative
        self.actions = []
        self.removed_duplicates = []
        self.headers = []
    
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
    
    def _parse_float(self, value: str) -> Optional[float]:
        """Parse string to float, return None if invalid."""
        try:
            return float(value) if value.strip() else None
        except (ValueError, AttributeError):
            return None
    
    def load_data(self) -> None:
        """Load and parse CSV data."""
        print(f"Loading data from {self.input_file}...")
        
        try:
            with open(self.input_file, 'r', encoding='utf-8') as file:
                csv_reader = csv.reader(file)
                self.headers = next(csv_reader)  # Store header row
                
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
                            replicas=row[3],
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
                            tags=row[20] if len(row) > 20 else "",
                            original_row=row  # Keep original row data for output
                        )
                        
                        # Only process successful actions with valid data
                        if (action.execution_status == 'SUCCEEDED' and 
                            action.current_value is not None and 
                            action.new_value is not None and
                            action.execution_datetime is not None):
                            self.actions.append(action)
                            
                    except Exception as e:
                        print(f"Warning: Error parsing row {row_num}: {e}")
                        continue
                        
            print(f"Successfully loaded {len(self.actions)} valid action records.")
            
        except FileNotFoundError:
            print(f"Error: File {self.input_file} not found.")
            sys.exit(1)
        except Exception as e:
            print(f"Error loading data: {e}")
            sys.exit(1)
    
    def remove_duplicates(self) -> None:
        """Remove consecutive duplicate actions."""
        mode_text = "conservative mode (removing ALL actions from groups with duplicates)" if self.conservative else "standard mode (keeping first occurrence)"
        print(f"Identifying and removing consecutive duplicate actions in {mode_text}...")
        
        # Group actions by (cluster, namespace, workload_name, container_spec, commodity)
        # This ensures we only deduplicate within the exact same workload/commodity combination
        groups = defaultdict(list)
        for action in self.actions:
            group_key = (action.cluster, action.namespace, action.name, action.container_spec, action.commodity)
            groups[group_key].append(action)
        
        print(f"Found {len(groups)} unique (cluster, namespace, workload_name, container_spec, commodity) groups.")
        
        # Process each group separately
        deduplicated_actions = []
        total_removed = 0
        groups_with_duplicates = 0
        
        for group_key, group_actions in groups.items():
            cluster, namespace, workload_name, container_spec, commodity = group_key
            
            # Sort by execution time
            group_actions.sort(key=lambda x: x.execution_datetime)
            
            if self.conservative:
                # First pass: identify if this group has any duplicates
                has_duplicates = False
                for i, action in enumerate(group_actions):
                    if i > 0:  # Check against previous action
                        prev_action = group_actions[i - 1]
                        if (action.current_value == prev_action.current_value and
                                action.new_value == prev_action.new_value):
                            has_duplicates = True
                            break
                # Conservative mode: remove ALL actions from groups with any duplicates
                if has_duplicates:
                    self.removed_duplicates.extend(group_actions)
                    total_removed += len(group_actions)
                    groups_with_duplicates += 1
                    print(f"  {cluster}/{namespace}/{workload_name}/{container_spec}/{commodity}: removed ALL {len(group_actions)} actions (conservative mode - group has duplicates)")
            else:
                only_duplicates = True
                first_action = group_actions[0]
                for i, action in enumerate(group_actions):
                    if (i > 0 and action.current_value != first_action.current_value) or len(group_actions) == 1:
                        only_duplicates = False

                if only_duplicates:
                    self.removed_duplicates.extend(group_actions)
                    total_removed += len(group_actions)
                    groups_with_duplicates += 1
                    print(f"  {cluster}/{namespace}/{workload_name}/{container_spec}/{commodity}: removed ALL {len(group_actions)} actions (conservative mode - group has duplicates)")

                else:
                    # Standard mode: remove only consecutive duplicates
                    kept_actions = []
                    group_removed = 0

                    for i, action in enumerate(group_actions):
                        is_duplicate = False

                        if i > 0:  # Check against previous action
                            prev_action = group_actions[i - 1]

                            # Check if this action is identical to the previous one
                            if action.current_value == prev_action.current_value:
                                is_duplicate = True
                                self.removed_duplicates.append(action)
                                group_removed += 1

                        if not is_duplicate:
                            kept_actions.append(action)

                    # CRITICAL: Ensure we always keep at least the first and last unique actions
                    # This preserves the analytical capability to compare first vs last
                    if len(kept_actions) == 0:
                        # Should never happen, but if it does, keep the first original action
                        kept_actions = [group_actions[0]]
                        print(f"Warning: Group {cluster}/{namespace}/{workload_name}/{container_spec}/{commodity} had no unique actions, keeping first action")
                    elif len(kept_actions) == 1 and len(group_actions) > 1:
                        # If we only have one unique action but started with multiple,
                        # ensure we preserve the time span by keeping the chronologically last action too
                        last_action = group_actions[-1]
                        if last_action not in kept_actions:
                            kept_actions.append(last_action)
                            # Remove from duplicates list since we're keeping it
                            if last_action in self.removed_duplicates:
                                self.removed_duplicates.remove(last_action)
                                group_removed -= 1

                    deduplicated_actions.extend(kept_actions)
                    total_removed += group_removed

                    if group_removed > 0:
                        print(f"  {cluster}/{namespace}/{workload_name}/{container_spec}/{commodity}: removed {group_removed} duplicates, kept {len(kept_actions)}")
        
        print(f"\nSummary:")
        print(f"  Original actions: {len(self.actions)}")
        print(f"  Removed duplicates: {total_removed}")
        print(f"  Remaining actions: {len(deduplicated_actions)}")
        print(f"  Duplicate percentage: {total_removed / len(self.actions) * 100:.1f}%")
        if self.conservative:
            print(f"  Groups with duplicates (all actions removed): {groups_with_duplicates}")
        
        self.actions = deduplicated_actions
    
    def write_cleaned_data(self) -> None:
        """Write the cleaned data to output file."""
        print(f"Writing cleaned data to {self.output_file}...")
        
        # Sort all remaining actions by execution time for consistent output
        self.actions.sort(key=lambda x: x.execution_datetime)
        
        try:
            with open(self.output_file, 'w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                
                # Write header
                writer.writerow(self.headers)
                
                # Write cleaned action rows
                for action in self.actions:
                    writer.writerow(action.original_row)
            
            print(f"✅ Cleaned data written to {self.output_file}")
            
        except Exception as e:
            print(f"Error writing cleaned data: {e}")
            sys.exit(1)
    
    def write_duplicates_report(self) -> None:
        """Write a report of removed duplicates."""
        if not self.duplicates_report or not self.removed_duplicates:
            return
            
        print(f"Writing duplicates report to {self.duplicates_report}...")
        
        try:
            with open(self.duplicates_report, 'w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                
                # Write header with additional info
                extended_headers = self.headers + ['duplicate_reason']
                writer.writerow(extended_headers)
                
                # Write removed duplicate rows
                for action in self.removed_duplicates:
                    row = list(action.original_row)
                    if self.conservative:
                        row.append(f"Conservative mode removal: group had duplicates ({action.current_value}→{action.new_value})")
                    else:
                        row.append(f"Consecutive duplicate: {action.current_value}→{action.new_value}")
                    writer.writerow(row)
            
            print(f"📊 Duplicates report written to {self.duplicates_report}")
            
        except Exception as e:
            print(f"Error writing duplicates report: {e}")
    
    def process(self) -> None:
        """Main processing pipeline."""
        self.load_data()
        self.remove_duplicates()
        self.write_cleaned_data()
        self.write_duplicates_report()

def main():
    parser = argparse.ArgumentParser(
        description='Remove consecutive duplicate actions from Turbonomic CSV data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic duplicate removal (keeps first occurrence of duplicates)
  python remove_duplicate_actions.py input.csv output_clean.csv
  
  # With duplicates report
  python remove_duplicate_actions.py input.csv output_clean.csv --report duplicates.csv
  
  # Conservative mode (removes ALL actions from groups with duplicates)
  python remove_duplicate_actions.py input.csv output_clean.csv --conservative
  
  # Process the main dataset
  python remove_duplicate_actions.py "Resize Workload Controller.csv" "Resize Workload Controller_clean.csv" --report duplicates_removed.csv

Duplicate Definition:
  A duplicate is an action for the same (cluster, container_spec, commodity) 
  that has identical current_value and new_value as the immediately previous
  action in chronological order.

Conservative Mode:
  When --conservative is used, ALL actions are removed from groups that contain
  any duplicates, including the original action. This is based on the rationale
  that duplicates indicate retries, suggesting the changes were not effective.
        """
    )
    
    parser.add_argument('input_file', help='Input CSV file with Turbonomic actions')
    parser.add_argument('output_file', help='Output CSV file with duplicates removed')
    parser.add_argument('--report', help='Optional CSV file to save details of removed duplicates')
    parser.add_argument('--conservative', action='store_true', help='Remove ALL actions from groups with duplicates (including originals). Use when duplicates indicate ineffective retries.')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose output')
    
    args = parser.parse_args()
    
    try:
        # Create and run the duplicate remover
        remover = DuplicateRemover(args.input_file, args.output_file, args.report, args.conservative)
        remover.process()
        
        print(f"\n🎯 Success! Cleaned data is ready for analysis.")
        print(f"   Use '{args.output_file}' with your Turbonomic analyzers for consistent results.")
        
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
