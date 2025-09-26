# Turbonomic Commodity Change Analysis Tools

This repository contains tools for analyzing Turbonomic commodity changes over time, providing insights into resource scaling patterns and trends.

## Tools Overview

### 1. `turbonomic_commodity_analyzer.py`
**Main commodity change analyzer** - Analyzes commodity changes by comparing oldest vs newest values per workload.

**Key Features:**
- Groups actions by workload and commodity type
- Tracks replica changes at workload level (not per-commodity)
- Shows total impact considering replica scaling
- Supports cluster and time window filtering
- Exports to CSV and text reports

### 2. `turbonomic_time_bucket_analyzer.py`
**Time bucket trend analyzer** - Analyzes how Total Impact Changes evolve over time periods.

**Key Features:**
- Analyzes data in configurable time buckets (daily, weekly, monthly)
- Tracks commodity trends over time
- Outputs CSV with time series data
- Perfect for identifying patterns and trends

### 3. `remove_duplicate_actions.py`
**Data preprocessing tool** - Removes consecutive duplicate actions from Turbonomic CSV files.

**Key Features:**
- Identifies consecutive duplicate actions (same cluster/container/commodity/values)
- Two modes: basic (keeps first occurrence) or conservative (removes entire groups)
- Generates detailed duplicate removal reports
- Essential for clean data analysis and consistent results

## Quick Start

### Basic Commodity Analysis
```bash
# Analyze all commodity changes
python turbonomic_commodity_analyzer.py data.csv

# Filter by cluster and time window
python turbonomic_commodity_analyzer.py data.csv --cluster prod-cluster-1 --from "01 Sep 2025 00:00" --to "30 Sep 2025 23:59"

# Export results
python turbonomic_commodity_analyzer.py data.csv -r report.txt -c results.csv --show-all
```

### Time Bucket Trend Analysis
```bash
# Weekly trend analysis (default)
python turbonomic_time_bucket_analyzer.py data.csv -o weekly_trends.csv

# Daily trends with summary report
python turbonomic_time_bucket_analyzer.py data.csv -o daily_trends.csv --bucket-days 1 -r summary.txt

# Monthly trends for specific clusters
python turbonomic_time_bucket_analyzer.py data.csv -o monthly_trends.csv --bucket-days 30 --cluster prod-cluster-1
```

### Data Preprocessing (Remove Duplicates)
```bash
# Basic duplicate removal (recommended first step)
python remove_duplicate_actions.py raw_data.csv clean_data.csv

# With duplicate removal report
python remove_duplicate_actions.py raw_data.csv clean_data.csv --report duplicates_removed.csv

# Conservative mode (removes entire groups with duplicates)
python remove_duplicate_actions.py raw_data.csv clean_data.csv --conservative --report conservative_removals.csv
```

## Input Data Format

Both tools expect CSV files with Turbonomic action data containing these columns:

| Column | Description |
|--------|-------------|
| Date created | Action creation date |
| Name | Workload name |
| Container cluster | Kubernetes cluster name |
| Number of replicas | Current replica count |
| Namespace | Kubernetes namespace |
| Container Spec | Container specification name |
| Impacted Commodity | Resource type (VCPU, VCPURequest, VMem, VMemRequest) |
| Resize Direction | Upsize/Downsize |
| Current Value | Current resource value |
| New Value | Proposed new resource value |
| Units | Resource units (mCores, KB) |
| Action Description | Description of the action |
| Execution Date and Time | When action was executed |
| Execution Status | SUCCESS/FAILED status |

## Output Formats

### Commodity Analyzer Output

**Text Report:**
- Summary statistics with total workloads and changes
- Commodity-specific statistics 
- Total impact changes by commodity type
- Detailed results table sorted by VCPURequest change

**CSV Export:**
- Cluster, namespace, workload details
- Replica change information (X→Y format)
- Impact changes for all 4 commodity types
- Percentage changes and time spans

### Time Bucket Analyzer Output

**CSV Format (6 columns):**
```csv
from,to,VCPU,VCPURequest,VMem,VMemRequest
2025-09-01 10:00,2025-09-08 10:00,500,250,3.00,1.50
2025-09-08 10:00,2025-09-15 10:00,1600,800,4.00,2.00
```

**Summary Report:**
- Analysis period and bucket configuration
- Bucket-by-bucket trends
- Total impact across all buckets

## Key Features

### 🎯 **Workload-Level Replica Tracking**
- Replica changes tracked across ALL commodities (not per-commodity)
- Uses most recent replica count for total impact calculations
- Clear X→Y notation shows scaling changes

### 🔍 **Advanced Filtering**
- **Cluster filtering**: Support for both short names (`prod-cluster`) and full names (`Kubernetes-prod-cluster`)
- **Time windows**: `--from` and `--to` date filtering with robust date parsing
- **Multiple clusters**: Use `--cluster` multiple times to include specific clusters
- **Conservative mode**: `--conservative` flag filters to only recent activity (last N days)

### 🧹 **Data Quality**
- **Duplicate removal**: Identify and remove consecutive duplicate actions
- **Conservative cleaning**: Option to remove entire action groups with duplicates
- **Detailed reporting**: Track exactly what duplicates were removed

### 📊 **Time Series Analysis**
- Configurable time buckets (daily, weekly, monthly, custom)
- Trend analysis showing how resource demands change over time
- Identifies patterns in resource scaling

### 🧪 **Comprehensive Testing**
- 130+ automated tests covering all functionality  
- Anonymized test data for safe testing
- Conservative filtering test coverage
- Edge case validation and error handling tests

## Command Line Options

### Commodity Analyzer
```bash
python turbonomic_commodity_analyzer.py [OPTIONS] CSV_FILE

Options:
  -r, --output-report FILE    Output text report file
  -c, --output-csv FILE      Output CSV results file  
  --cluster CLUSTER          Filter by cluster (can be used multiple times)
  --namespace NAMESPACE      Filter by namespace (supports wildcards like "app-*")
  --from DATE                Filter from date ("DD MMM YYYY HH:MM")
  --to DATE                  Filter to date ("DD MMM YYYY HH:MM")
  --conservative             Only analyze workloads with recent actions
  --conservative-days DAYS   Days to look back for recent actions (default: 14)
  --show-all                 Show all results (not just top 10)
  --show-actions             Show detailed action breakdown
  -v, --verbose              Enable verbose output
```

### Time Bucket Analyzer
```bash
python turbonomic_time_bucket_analyzer.py [OPTIONS] CSV_FILE

Options:
  -o, --output FILE          Output CSV file (required)
  -r, --report FILE          Summary report file (optional)
  --bucket-days DAYS         Time bucket size in days (default: 7)
  --cluster CLUSTER          Filter by cluster (can be used multiple times)
  -v, --verbose              Enable verbose output
```

### Duplicate Remover
```bash
python remove_duplicate_actions.py [OPTIONS] INPUT_FILE OUTPUT_FILE

Options:
  --report FILE              Save duplicate removal report to CSV file
  --conservative             Remove ALL actions from groups with duplicates
  -v, --verbose              Enable verbose output

Arguments:
  INPUT_FILE                 Input CSV file with Turbonomic actions
  OUTPUT_FILE                Output CSV file with duplicates removed
```

## Testing

Run the comprehensive test suite:
```bash
cd tests
python test_runner.py
```

**Test Coverage:**
- ✅ Basic functionality and report generation
- ✅ Cluster filtering (short/full names, multiple clusters)
- ✅ Namespace filtering (exact match, wildcards, multiple)
- ✅ Time window filtering (--from, --to, combined)
- ✅ Conservative filtering (workload-level, various time windows)
- ✅ Replica change tracking (workload-level)
- ✅ Time bucket analysis
- ✅ Output file generation
- ✅ Combined filter scenarios
- ✅ Edge cases and error handling

## Use Cases

### 1. **Complete Analysis Workflow**
```bash
# Step 1: Clean the data first (recommended)
python remove_duplicate_actions.py raw_data.csv clean_data.csv --report duplicates.csv

# Step 2: Analyze recent activity only
python turbonomic_commodity_analyzer.py clean_data.csv --conservative --from "01 Oct 2025 00:00" --show-all

# Step 3: Generate trend analysis
python turbonomic_time_bucket_analyzer.py clean_data.csv -o trends.csv -r weekly_summary.txt
```

### 2. **Resource Optimization Analysis**
```bash
# Analyze recent changes to see optimization impact (conservative mode)
python turbonomic_commodity_analyzer.py data.csv --conservative --conservative-days 7 --show-all
```

### 3. **Cluster-Specific Trends**
```bash
# Compare trends across production clusters
python turbonomic_time_bucket_analyzer.py data.csv -o prod_trends.csv --cluster prod-cluster-1 --cluster prod-cluster-2
```

### 4. **Data Quality Assessment**
```bash
# Check for duplicates and clean data
python remove_duplicate_actions.py raw_data.csv clean_data.csv --report quality_report.csv --verbose

# Conservative cleaning (removes entire groups with duplicates)
python remove_duplicate_actions.py raw_data.csv ultra_clean.csv --conservative --report conservative_report.csv
```

### 5. **Daily Monitoring**
```bash
# Daily resource change monitoring
python turbonomic_time_bucket_analyzer.py data.csv -o daily_monitor.csv --bucket-days 1
```

## Dependencies

- Python 3.6+
- Standard library only (csv, datetime, argparse, subprocess, etc.)
- No external dependencies required

## Contributing

1. Run tests before submitting changes: `cd tests && python test_runner.py`
2. Add test cases for new features
3. Follow existing code style and documentation patterns
4. Ensure all anonymized test data uses fictional names/dates

## License

[Add your license information here]




