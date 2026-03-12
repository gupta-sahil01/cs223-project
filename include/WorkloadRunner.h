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

enum class Protocol  { OCC, TWO_PL };
enum class WorkloadId { WORKLOAD1, WORKLOAD2 };

struct TxStats {
    long long response_time_us;
    int       retry_count;
    int       tx_type;
};

class WorkloadRunner {
public:
    WorkloadRunner(
        Database&   db,
        Protocol    protocol,
        WorkloadId  workload,
        int         num_threads,
        double      hotset_probability,
        int         hotset_size,
        int         transactions_per_thread
    );

    void loadInitialData(const std::string& insert_file);
    void run(const std::string& dist_file = "");

private:
    Database&   db_;
    Protocol    protocol_;
    WorkloadId  workload_;
    int         num_threads_;
    double      hotset_probability_;
    int         hotset_size_;
    int         transactions_per_thread_;

    std::vector<std::string> account_keys_;
    std::vector<std::string> warehouse_keys_;
    std::vector<std::string> district_keys_;
    std::vector<std::string> customer_keys_;
    std::vector<std::string> stock_keys_;

    std::vector<std::string> hot_account_keys_;
    std::vector<std::string> hot_warehouse_keys_;
    std::vector<std::string> hot_district_keys_;
    std::vector<std::string> hot_customer_keys_;
    std::vector<std::string> hot_stock_keys_;

    LockManager           lock_manager_;
    CommitLog             commit_log_;
    std::atomic<int>      next_tx_id_{0};
    std::mutex            stats_mutex_;
    std::vector<TxStats>  all_stats_;

    void workerThread();
    void workerThreadW1();
    void workerThreadW2();

    std::string pickFrom(const std::vector<std::string>& hot,
                         const std::vector<std::string>& all);

    bool runTransferOCC   (const std::string& from_key,
                           const std::string& to_key, int tx_id);
    bool runTransferTwoPL (const std::string& from_key,
                           const std::string& to_key, int tx_id);

    bool runNewOrderOCC   (const std::string& d_key,
                           const std::string& s1, const std::string& s2,
                           const std::string& s3, int tx_id);
    bool runNewOrderTwoPL (const std::string& d_key,
                           const std::string& s1, const std::string& s2,
                           const std::string& s3, int tx_id);
    bool runPaymentOCC    (const std::string& w_key, const std::string& d_key,
                           const std::string& c_key, int tx_id);
    bool runPaymentTwoPL  (const std::string& w_key, const std::string& d_key,
                           const std::string& c_key, int tx_id);

    void printStats(const std::string& dist_file = "");
};