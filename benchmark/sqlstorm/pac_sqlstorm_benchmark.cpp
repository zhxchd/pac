//
// SQLStorm TPC-H benchmark runner for DuckDB (no Docker, no OLAPBench).
// Generates TPC-H SF1 data using DuckDB's built-in generator, then runs
// all SQLStorm v1.0 TPC-H queries and reports success/failure/timeout stats.
//

#include "pac_sqlstorm_benchmark.hpp"

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/printer.hpp"

#include <chrono>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <map>
#include <algorithm>
#include <ctime>
#include <iomanip>
#include <thread>
#include <atomic>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include <limits.h>

namespace duckdb {

static string Timestamp() {
	auto now = std::chrono::system_clock::now();
	std::time_t t = std::chrono::system_clock::to_time_t(now);
	char buf[64];
	std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&t));
	return string(buf);
}

static void Log(const string &msg) {
	Printer::Print("[" + Timestamp() + "] " + msg);
}

static string FormatNumber(double v) {
	std::ostringstream oss;
	oss << std::setprecision(5) << std::defaultfloat << v;
	string s = oss.str();
	auto pos = s.find('.');
	if (pos != string::npos) {
		while (!s.empty() && s.back() == '0') { s.pop_back(); }
		if (!s.empty() && s.back() == '.') { s.pop_back(); }
	}
	return s;
}

static bool FileExists(const string &path) {
	struct stat buffer;
	return (stat(path.c_str(), &buffer) == 0);
}

static string ReadFileToString(const string &path) {
	std::ifstream in(path);
	if (!in.is_open()) {
		return string();
	}
	std::ostringstream ss;
	ss << in.rdbuf();
	return ss.str();
}

// Collect all .sql files from a directory
static vector<string> CollectQueryFiles(const string &dir) {
	vector<string> files;
	DIR *d = opendir(dir.c_str());
	if (!d) {
		return files;
	}
	struct dirent *entry;
	while ((entry = readdir(d)) != nullptr) {
		string name = entry->d_name;
		if (name.size() > 4 && name.substr(name.size() - 4) == ".sql") {
			files.push_back(dir + "/" + name);
		}
	}
	closedir(d);
	std::sort(files.begin(), files.end());
	return files;
}

// Extract query name from path (e.g. "/path/to/10001.sql" -> "10001")
static string QueryName(const string &path) {
	auto pos = path.find_last_of('/');
	string fname = (pos != string::npos) ? path.substr(pos + 1) : path;
	if (fname.size() > 4) {
		fname = fname.substr(0, fname.size() - 4);
	}
	return fname;
}

static string FindSchemaFile(const string &filename) {
	// Try common relative locations
	vector<string> candidates = {
		filename,
		"./" + filename,
		"../" + filename,
		"../../" + filename,
		"benchmark/" + filename,
		"benchmark/tpch/" + filename,
		"../benchmark/" + filename,
		"../benchmark/tpch/" + filename,
		"../../benchmark/" + filename,
		"../../benchmark/tpch/" + filename,
	};

	for (auto &cand : candidates) {
		if (FileExists(cand)) {
			return cand;
		}
	}

	// Try relative to executable
	char exe_path[PATH_MAX];
	ssize_t len = readlink("/proc/self/exe", exe_path, sizeof(exe_path) - 1);
	if (len != -1) {
		exe_path[len] = '\0';
		string dir = string(exe_path);
		auto pos = dir.find_last_of('/');
		if (pos != string::npos) {
			dir = dir.substr(0, pos);
		}

		for (int i = 0; i < 6; ++i) {
			string cand = dir + "/benchmark/tpch/" + filename;
			if (FileExists(cand)) {
				return cand;
			}
			auto p2 = dir.find_last_of('/');
			if (p2 == string::npos) break;
			dir = dir.substr(0, p2);
		}
	}

	return "";
}

// Try to find the SQLStorm queries directory
static string FindQueriesDir() {
	// Common relative paths from where the binary might be run
	vector<string> candidates = {
		"SQLStorm/v1.0/tpch/queries",
	    "benchmark/sqlstorm/SQLStorm/v1.0/tpch/queries",
	    "../benchmark/sqlstorm/SQLStorm/v1.0/tpch/queries",
	    "../../benchmark/sqlstorm/SQLStorm/v1.0/tpch/queries",
	    "../../../benchmark/sqlstorm/SQLStorm/v1.0/tpch/queries",
	};

	for (auto &c : candidates) {
		if (FileExists(c)) {
			return c;
		}
	}

	// Try relative to executable
	char exe_path[PATH_MAX];
	ssize_t len = readlink("/proc/self/exe", exe_path, sizeof(exe_path) - 1);
	if (len != -1) {
		exe_path[len] = '\0';
		string dir = string(exe_path);
		auto pos = dir.find_last_of('/');
		if (pos != string::npos) {
			dir = dir.substr(0, pos);
		}
		for (int i = 0; i < 6; ++i) {
			string cand = dir + "/benchmark/SQLStorm/v1.0/tpch/queries";
			if (FileExists(cand)) {
				return cand;
			}
			auto p2 = dir.find_last_of('/');
			if (p2 == string::npos) break;
			dir = dir.substr(0, p2);
		}
	}

	return "";
}

struct BenchmarkQueryResult {
	string name;
	string state;   // "success", "error", "timeout"
	double time_ms;
	int64_t rows;
	string error;
};

int RunSQLStormBenchmark(const string &queries_dir, const string &out_csv, double timeout_s) {
	try {
		// Resolve queries directory
		string qdir = queries_dir;
		if (qdir.empty()) {
			qdir = FindQueriesDir();
		}
		if (qdir.empty() || !FileExists(qdir)) {
			Log("ERROR: Cannot find SQLStorm TPC-H queries directory.");
			Log("Provide it with --queries <path>");
			return 1;
		}

		auto query_files = CollectQueryFiles(qdir);
		if (query_files.empty()) {
			Log("ERROR: No .sql files found in " + qdir);
			return 1;
		}

		Log("SQLStorm TPC-H SF1 benchmark (baseline, no PAC)");
		Log("Queries directory: " + qdir);
		Log("Found " + std::to_string(query_files.size()) + " queries");
		Log("Timeout: " + FormatNumber(timeout_s) + "s");

		// Open file-backed DuckDB database (reuse if it already exists)
		string db_actual = "tpch_sqlstorm.db";
		bool db_exists = FileExists(db_actual);

		if (db_exists) {
			Log("Connecting to existing DuckDB database: " + db_actual + " (skipping data generation).");
		} else {
			Log("Will create/populate DuckDB database: " + db_actual);
		}

		unique_ptr<DuckDB> db;
		unique_ptr<Connection> con;

		auto reconnect = [&]() {
			con.reset();
			db.reset();
			db = make_uniq<DuckDB>(db_actual.c_str());
			con = make_uniq<Connection>(*db);
			con->Query("INSTALL icu; LOAD icu;");
			con->Query("INSTALL tpch; LOAD tpch;");
			// Load PAC extension
			auto r = con->Query("LOAD pac");
			if (r->HasError()) {
				throw std::runtime_error("Failed to load PAC extension: " + r->GetError());
			}
		};

		reconnect();

		if (!db_exists) {
			Log("Generating TPC-H SF1 data...");
			auto t0 = std::chrono::steady_clock::now();
			auto r_dbgen = con->Query("CALL dbgen(sf=0.00001);");
			auto t1 = std::chrono::steady_clock::now();
			double gen_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
			if (r_dbgen && r_dbgen->HasError()) {
				Log("CALL dbgen error: " + r_dbgen->GetError());
			}
			Log("TPC-H SF0.00001 generated in " + FormatNumber(gen_ms) + " ms");

			// Execute PAC schema (same file used by TPC-H benchmark)
			Log("Executing PAC TPC-H schema...");
			string schema_file = FindSchemaFile("pac_tpch_schema.sql");
			if (schema_file.empty()) {
				Log("WARNING: Cannot find pac_tpch_schema.sql, skipping schema execution");
			} else {
				string schema_sql = ReadFileToString(schema_file);
				if (!schema_sql.empty()) {
					auto r_schema = con->Query(schema_sql);
					if (r_schema && r_schema->HasError()) {
						Log("Schema execution error: " + r_schema->GetError());
					} else {
						Log("PAC TPC-H schema applied successfully from: " + schema_file);
					}
				}
			}
		} else {
			Log("Skipping CALL dbgen since database file already exists: " + db_actual);
		}

		// Run all queries
		vector<BenchmarkQueryResult> results;
		int success = 0, failed = 0, timed_out = 0, crashed = 0;
		double total_success_time = 0;
		int total = static_cast<int>(query_files.size());
		int log_interval = std::max(1, total / 20); // log ~20 times

		for (int i = 0; i < total; ++i) {
			auto &qf = query_files[i];
			string name = QueryName(qf);
			string sql = ReadFileToString(qf);
			if (sql.empty()) {
				results.push_back({name, "error", 0, 0, "empty query file"});
				failed++;
				continue;
			}

			BenchmarkQueryResult qr;
			qr.name = name;
			qr.rows = 0;

			try {
				// Run query in a separate thread so we can enforce the timeout
				// by calling con->Interrupt() from the main thread.
				unique_ptr<MaterializedQueryResult> result;
				std::atomic<bool> query_done {false};
				std::exception_ptr query_exception;

				auto start = std::chrono::steady_clock::now();
				std::thread query_thread([&]() {
					try {
						result = con->Query(sql);
					} catch (...) {
						query_exception = std::current_exception();
					}
					query_done.store(true, std::memory_order_release);
				});

				// Poll until query finishes or timeout expires
				auto deadline = start + std::chrono::duration<double>(timeout_s);
				while (!query_done.load(std::memory_order_acquire)) {
					if (std::chrono::steady_clock::now() >= deadline) {
						con->Interrupt();
						break;
					}
					std::this_thread::sleep_for(std::chrono::milliseconds(10));
				}
				query_thread.join();
				auto end = std::chrono::steady_clock::now();
				qr.time_ms = std::chrono::duration<double, std::milli>(end - start).count();

				// Re-throw any exception from the query thread
				if (query_exception) {
					std::rethrow_exception(query_exception);
				}

				if (result && result->HasError()) {
					qr.state = "error";
					qr.error = result->GetError();
					// Truncate long error messages
					if (qr.error.size() > 200) {
						qr.error = qr.error.substr(0, 200);
					}
					// Detect fatal errors that invalidate the database
					if (qr.error.find("FATAL") != string::npos ||
					    qr.error.find("database has been invalidated") != string::npos) {
						qr.state = "crash";
						crashed++;
						Log("Database invalidated by query " + name + " (" + qf + "), reconnecting...");
						reconnect();
					} else if (qr.error.find("INTERRUPT") != string::npos ||
					           qr.error.find("Interrupted") != string::npos) {
						qr.state = "timeout";
						timed_out++;
					} else {
						failed++;
					}
				} else if (qr.time_ms > timeout_s * 1000) {
					qr.state = "timeout";
					timed_out++;
				} else {
					qr.state = "success";
					if (result) {
						// Count rows by fetching
						int64_t row_count = 0;
						while (auto chunk = result->Fetch()) {
							row_count += chunk->size();
						}
						qr.rows = row_count;
					}
					success++;
					total_success_time += qr.time_ms;
				}
			} catch (std::exception &e) {
				qr.state = "crash";
				qr.time_ms = 0;
				string err = e.what();
				if (err.size() > 200) {
					err = err.substr(0, 200);
				}
				qr.error = err;
				crashed++;
				Log("Exception on query " + name + " (" + qf + "): " + err + ", reconnecting...");
				reconnect();
			} catch (...) {
				qr.state = "crash";
				qr.time_ms = 0;
				qr.error = "unexpected crash (non-std::exception)";
				crashed++;
				Log("Unknown crash on query " + name + " (" + qf + "), reconnecting...");
				reconnect();
			}

			results.push_back(qr);

			if ((i + 1) % log_interval == 0 || i + 1 == total) {
				Log("[" + std::to_string(i + 1) + "/" + std::to_string(total) +
				    "] success=" + std::to_string(success) +
				    " failed=" + std::to_string(failed) +
				    " crash=" + std::to_string(crashed) +
				    " timeout=" + std::to_string(timed_out));
			}
		}

		// Write CSV
		string csv_path = out_csv.empty() ? "benchmark/sqlstorm/sqlstorm_results.csv" : out_csv;
		std::ofstream csv(csv_path, std::ofstream::out | std::ofstream::trunc);
		if (!csv.is_open()) {
			Log("ERROR: Failed to open output CSV: " + csv_path);
			return 1;
		}
		csv << "query,state,time_ms,rows,error\n";
		for (auto &r : results) {
			// Escape quotes in error messages
			string escaped_error = r.error;
			size_t pos = 0;
			while ((pos = escaped_error.find('"', pos)) != string::npos) {
				escaped_error.replace(pos, 1, "\"\"");
				pos += 2;
			}
			csv << r.name << "," << r.state << "," << FormatNumber(r.time_ms) << ","
			    << r.rows << ",\"" << escaped_error << "\"\n";
		}
		csv.close();

		// Summary
		Log("=== Results ===");
		Log("Results written to: " + csv_path);
		Log("Total:   " + std::to_string(total));
		Log("Success: " + std::to_string(success) + " (" + FormatNumber(100.0 * success / total) + "%)");
		Log("Failed:  " + std::to_string(failed) + " (" + FormatNumber(100.0 * failed / total) + "%)");
		Log("Crash:   " + std::to_string(crashed) + " (" + FormatNumber(100.0 * crashed / total) + "%)");
		Log("Timeout: " + std::to_string(timed_out) + " (" + FormatNumber(100.0 * timed_out / total) + "%)");
		if (success > 0) {
			Log("Total time (successful): " + FormatNumber(total_success_time) + " ms");
			Log("Avg time per successful query: " + FormatNumber(total_success_time / success) + " ms");
		}

		// Error breakdown by message
		if (failed + crashed > 0) {
			std::map<string, int> error_counts;
			for (auto &r : results) {
				if (r.state == "error" || r.state == "crash") {
					error_counts[r.error]++;
				}
			}
			// Sort by count descending
			vector<std::pair<string, int>> sorted_errors(error_counts.begin(), error_counts.end());
			std::sort(sorted_errors.begin(), sorted_errors.end(),
			          [](const std::pair<string, int> &a, const std::pair<string, int> &b) {
				          return a.second > b.second;
			          });

			Log("=== Error Breakdown ===");
			for (size_t ei = 0; ei < sorted_errors.size(); ++ei) {
				Log("  " + std::to_string(sorted_errors[ei].second) + "x " + sorted_errors[ei].first);
			}

			// Print queries with INTERNAL errors
			bool has_internal = false;
			for (size_t ri = 0; ri < results.size(); ++ri) {
				if (results[ri].error.find("INTERNAL") != string::npos) {
					if (!has_internal) {
						Log("=== Internal Errors (queries) ===");
						has_internal = true;
					}
					Log("  query " + results[ri].name + ": " + results[ri].error);
				}
			}
		}

		return 0;

	} catch (std::exception &ex) {
		Log(string("Fatal error: ") + ex.what());
		return 2;
	}
}

} // namespace duckdb

static void PrintUsage() {
	std::cout << "Usage: pac_sqlstorm_benchmark [options]\n"
	          << "Options:\n"
	          << "  --queries <dir>   SQLStorm TPC-H queries directory\n"
	          << "                    (default: auto-detect benchmark/SQLStorm/v1.0/tpch/queries)\n"
	          << "  --out <csv>       Output CSV path (default: benchmark/sqlstorm/sqlstorm_results.csv)\n"
	          << "  --timeout <sec>   Per-query timeout in seconds (default: 10)\n"
	          << "  -h, --help        Show this help message\n";
}

int main(int argc, char **argv) {
	std::string queries_dir;
	std::string out_csv;
	double timeout_s = 1.0;

	for (int i = 1; i < argc; ++i) {
		std::string arg = argv[i];
		if (arg == "-h" || arg == "--help") {
			PrintUsage();
			return 0;
		} else if (arg == "--queries" && i + 1 < argc) {
			queries_dir = argv[++i];
		} else if (arg == "--out" && i + 1 < argc) {
			out_csv = argv[++i];
		} else if (arg == "--timeout" && i + 1 < argc) {
			timeout_s = std::stod(argv[++i]);
		} else {
			std::cerr << "Unknown option: " << arg << "\n";
			PrintUsage();
			return 1;
		}
	}

	return duckdb::RunSQLStormBenchmark(queries_dir, out_csv, timeout_s);
}
