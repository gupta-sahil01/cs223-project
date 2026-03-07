#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <atomic>
#include <mutex>
#include "Database.h"
#include "LockManager.h"
#include "CommitLog.h"
#include "json.hpp"

using json = nlohmann::json;

enum class Protocol { OCC, TWO_PL };

struct TxStats {
    long long response_time_us;
    bool was_retried;
};

class WorkloadRunner {
public:
    WorkloadRunner(
        Database& db,
        Protocol protocol,
        int num_threads,
        double hotset_probability,
        int hotset_size,
        int transactions_per_thread
    );

    void loadInitialData(const std::string& insert_file);
    void run();

private:
    Database& db_;
    Protocol protocol_;
    int num_threads_;
    double hotset_probability_;
    int hotset_size_;
    int transactions_per_thread_;

    std::vector<std::string> all_keys_;
    std::vector<std::string> hotset_keys_;

    LockManager lock_manager_;
    CommitLog commit_log_;

    std::atomic<int> next_tx_id_{0};
    std::mutex stats_mutex_;
    std::vector<TxStats> all_stats_;

    void workerThread();
    std::string pickKey();
    bool runTransferOCC(const std::string& from_key, const std::string& to_key, int tx_id);
    bool runTransferTwoPL(const std::string& from_key, const std::string& to_key, int tx_id);
    void printStats();
};