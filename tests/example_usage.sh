#!/bin/bash
# Example usage patterns for Turbonomic Commodity Analyzer
# Using anonymized test data

echo "🔍 Turbonomic Commodity Analyzer - Example Usage Patterns"
echo "=========================================================="
echo ""

# Change to test directory
cd "$(dirname "$0")"

echo "1️⃣  Basic Analysis (all data, top 10 results)"
echo "----------------------------------------------"
python ../turbonomic_commodity_analyzer.py test_basic.csv
echo ""

echo "2️⃣  Show All Results (no limit)"
echo "-------------------------------"
python ../turbonomic_commodity_analyzer.py test_basic.csv --show-all
echo ""

echo "3️⃣  Filter by Cluster (short name)"
echo "-----------------------------------"
python ../turbonomic_commodity_analyzer.py test_basic.csv --cluster test-cluster-1
echo ""

echo "4️⃣  Filter by Multiple Clusters"
echo "--------------------------------"
python ../turbonomic_commodity_analyzer.py test_basic.csv --cluster test-cluster-1 --cluster test-cluster-2
echo ""

echo "5️⃣  Time Window Filtering (from date)"
echo "--------------------------------------"
python ../turbonomic_commodity_analyzer.py test_time_filtering.csv --from "14 Sep 2025 00:00"
echo ""

echo "6️⃣  Time Window Filtering (to date)"
echo "------------------------------------"
python ../turbonomic_commodity_analyzer.py test_time_filtering.csv --to "16 Sep 2025 00:00"
echo ""

echo "7️⃣  Time Window Filtering (both dates)"
echo "---------------------------------------"
python ../turbonomic_commodity_analyzer.py test_time_filtering.csv --from "14 Sep 2025 00:00" --to "16 Sep 2025 00:00"
echo ""

echo "8️⃣  Combined Filters (cluster + time)"
echo "--------------------------------------"
python ../turbonomic_commodity_analyzer.py test_time_filtering.csv --cluster time-test-cluster --from "14 Sep 2025 00:00"
echo ""

echo "9️⃣  Export to Files"
echo "-------------------"
python ../turbonomic_commodity_analyzer.py test_basic.csv -r example_report.txt -c example_results.csv
echo "Files created: example_report.txt, example_results.csv"
echo ""

echo "🔟 Replica Changes Analysis"
echo "---------------------------"
python ../turbonomic_commodity_analyzer.py test_replica_changes.csv
echo ""

echo "✅ All examples completed!"
echo ""
echo "💡 Tips:"
echo "   - Use short cluster names (without 'Kubernetes-' prefix) for convenience"
echo "   - Date format: 'DD MMM YYYY HH:MM' (e.g., '15 Sep 2025 12:00')"
echo "   - Multiple --cluster flags can be used to include multiple clusters"
echo "   - Results are sorted by VCPURequest change (absolute value, descending)"
echo "   - Replica changes show X→Y notation while using most recent count for calculations"





