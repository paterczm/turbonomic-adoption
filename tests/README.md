# Turbonomic Commodity Analyzer Test Suite

This directory contains comprehensive tests for the Turbonomic Commodity Analyzer with anonymized test data.

## Test Files

### Test Data Files
- **test_basic.csv** - Basic functionality test data with multiple workloads, clusters, and commodity types
- **test_time_filtering.csv** - Data spanning different time periods for testing time window filtering
- **test_replica_changes.csv** - Data with replica count changes to test replica handling logic
- **test_edge_cases.csv** - Edge cases including failed actions, missing data, and error conditions
- **test_single_action.csv** - Single action scenarios per commodity and isolated commodities
- **test_multi_week_data.csv** - Multi-week data for time bucket analysis testing

### Test Scripts
- **test_runner.py** - Main test runner that executes all test scenarios
- **README.md** - This documentation file

## Test Scenarios

### 1. Basic Functionality Tests
- Report generation with correct headers and structure
- Workload data processing and grouping
- Commodity change calculations
- Replica change notation (X→Y format)
- Cluster name processing (removing Kubernetes- prefix)

### 2. Cluster Filtering Tests
- Filter by short cluster names (e.g., "test-cluster-1")
- Filter by full cluster names (e.g., "Kubernetes-test-cluster-1")
- Multiple cluster filtering (--cluster multiple times)
- Non-existent cluster handling

### 3. Time Window Filtering Tests
- --from date filtering (actions from date onwards)
- --to date filtering (actions up to date)
- Combined time window (--from and --to together)
- Invalid date format error handling

### 4. Replica Change Tests
- Correct handling of replica count changes between oldest and newest actions
- Using most recent replica count for total impact calculations
- Preserving X→Y notation in reports

### 5. Workload-Level Replica Tracking Tests
- Replica changes tracked at workload level across ALL commodities (not per-commodity)
- Cross-commodity replica change detection (earliest action to latest action across all commodities)
- Single workload row consolidation with correct replica change attribution
- Consistent replica handling when no changes occur

### 6. Single Action Scenarios Tests
- Single action per commodity (oldest = newest action)
- Current value vs new value comparison for single actions
- Mixed scenarios (single actions alongside multi-action workloads)
- Isolated commodity handling (workload with only one commodity type)
- Correct total impact calculation using replica multiplication

### 7. Output Options Tests
- --show-all flag (show all results vs top 10)
- Text report output (-r flag)
- CSV export output (-c flag)
- Sorting by VCPURequest change

### 8. Time Bucket Analyzer Tests
- Weekly, daily, and custom time bucket analysis
- CSV output format validation (6 columns: from, to, 4 commodities)
- Different bucket sizes and cluster filtering
- Integration with existing analyzer functionality

### 9. Edge Cases and Error Handling
- Failed actions (EXECUTION_STATUS != 'SUCCEEDED') exclusion
- Missing or invalid data handling
- Single commodity workloads
- Non-existent input files
- Invalid command line arguments

### 10. Combined Filter Tests
- Cluster + time window filtering together
- Multiple filters with expected intersections

## Test Data Structure

All test data uses anonymized names but realistic patterns:

- **Clusters**: `Kubernetes-test-cluster-1`, `Kubernetes-test-cluster-2`, etc.
- **Workloads**: `web-app-alpha`, `db-service-beta`, `api-gateway-gamma`, etc.
- **Namespaces**: `app-namespace`, `database-namespace`, `gateway-namespace`, etc.
- **Timestamps**: September 2025 dates with realistic time patterns
- **Resource Values**: Realistic CPU (mCores) and Memory (KB) values

## Running Tests

### Run All Tests
```bash
cd tests
python test_runner.py
```

### Run Individual Tests
You can also run the analyzer manually with test data:

```bash
# Basic functionality
python ../turbonomic_commodity_analyzer.py test_basic.csv

# Cluster filtering
python ../turbonomic_commodity_analyzer.py test_basic.csv --cluster test-cluster-1

# Time filtering
python ../turbonomic_commodity_analyzer.py test_time_filtering.csv --from "14 Sep 2025 00:00" --to "16 Sep 2025 23:59"

# Combined filters
python ../turbonomic_commodity_analyzer.py test_time_filtering.csv --cluster time-test-cluster --from "14 Sep 2025 00:00"

# Output to files
python ../turbonomic_commodity_analyzer.py test_basic.csv -r test_report.txt -c test_results.csv --show-all
```

## Expected Test Results

### Successful Test Run
- All basic functionality tests should pass
- Filtering tests should correctly include/exclude expected data
- Time window tests should respect date boundaries
- Output files should be generated with correct content
- Edge cases should be handled gracefully

### Test Output Format
```
🧪 Starting Turbonomic Commodity Analyzer Test Suite
============================================================

=== Testing Basic Functionality ===
✓ PASS: Basic functionality: Command succeeded
✓ PASS: Report header: Contains 'TURBONOMIC COMMODITY CHANGE ANALYSIS REPORT'
✓ PASS: Test workload present: Contains 'web-app-alpha'
...

============================================================
🏁 TEST SUMMARY
============================================================
✓ Passed: 45
✗ Failed: 0
📊 Total:  45

🎉 ALL TESTS PASSED! 🎉
```

## Adding New Tests

To add new test scenarios:

1. Create new test data CSV files following the existing format
2. Add test methods to `test_runner.py`
3. Use the provided assertion methods for consistent testing
4. Update this README with new test descriptions

## Test Data Privacy

All test data uses completely anonymized/fictional:
- Application names
- Cluster names  
- Namespace names
- User accounts
- Timestamps (future dates)

No real production data is included in these tests.
