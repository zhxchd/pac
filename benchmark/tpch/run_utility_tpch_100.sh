#!/bin/bash
# Usage from the main project folder:
#   bash benchmark/tpch/run_utility_tpch_100.sh [database] [duckdb_binary]
#
# Examples:
#   bash benchmark/tpch/run_utility_tpch_100.sh tpch_sf30.db
#   bash benchmark/tpch/run_utility_tpch_100.sh tpch_sf30.db ./build/release/duckdb

DB="${1:-tpch_sf30.db}"
DUCKDB="${2:-./build/release/duckdb}"
SCRIPT="benchmark/tpch/utility_tpch.sql"
RUNS=100

SETUP="INSTALL tpch; LOAD tpch;"

for i in $(seq 1 $RUNS); do
    printf "\rRun %d/%d" "$i" "$RUNS"
    echo "$SETUP" | cat - "$SCRIPT" | "$DUCKDB" "$DB" > /dev/null
done
printf "\rDone: %d runs completed.\n" "$RUNS"
