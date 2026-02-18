//
// Created by ila on 02/18/26.
//

#ifndef PAC_SQLSTORM_BENCHMARK_HPP
#define PAC_SQLSTORM_BENCHMARK_HPP

#include <string>
#include <vector>
#include "duckdb.hpp"

namespace duckdb {

// Run the SQLStorm TPC-H SF1 benchmark (baseline, no PAC).
// This benchmark:
// 1. Creates an in-memory DuckDB with TPC-H SF1 data
// 2. Runs all SQLStorm v1.0 TPC-H queries
// 3. Reports success/failure/timeout statistics
//
// Parameters:
// - queries_dir: directory containing SQLStorm .sql query files
// - out_csv: output CSV path (if empty, auto-named)
// - timeout_s: per-query timeout in seconds
//
// Returns 0 on success, non-zero on error.
int RunSQLStormBenchmark(const string &queries_dir = "",
                         const string &out_csv = "",
                         double timeout_s = 10.0);

} // namespace duckdb

#endif // PAC_SQLSTORM_BENCHMARK_HPP