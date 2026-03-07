#include "WorkloadRunner.h"
#include "OCCTransaction.h"
#include "TwoPLTransaction.h"
#include <thread>
#include <chrono>
#include <random>
#include <iostream>
#include <fstream>
#include <sstream>
#include <regex>

WorkloadRunner::WorkloadRunner(
    Database& db, Protocol protocol, int num_threads,
    double hotset_probability, int hotset_size, int transactions_per_thread)
    : db_(db), protocol_(protocol), num_threads_(num_threads),
      hotset_probability_(hotset_probability), hotset_size_(hotset_size),
      transactions_per_thread_(transactions_per_thread)
{}

// Converts the non-standard format {name: "X", balance: 123}
// into valid JSON {"name": "X", "balance": 123}
static std::string normalizeJSON(const std::string& raw) {
    std::string result = raw;
    // Quote unquoted keys: word characters followed by a colon
    result = std::regex_replace(result, std::regex(R"((\w+)\s*:)"), "\"$1\":");
    return result;
}

void WorkloadRunner::loadInitialData(const std::string& insert_file) {
    std::ifstream file(insert_file);
    std::string line;

    while (std::getline(file, line)) {
        if (line == "INSERT" || line == "END" || line.empty()) continue;

        if (line.rfind("KEY:", 0) == 0) {
            // Split on first ", VALUE: "
            size_t sep = line.find(", VALUE: ");
            std::string key   = line.substr(5, sep - 5);         // after "KEY: "
            std::string value = line.substr(sep + 9);            // after ", VALUE: "

            std::string json_str = normalizeJSON(value);
            db_.put(key, json_str);
            all_keys_.push_back(key);
        }
    }

    // Build hotset from first N keys
    int hs = std::min(hotset_size_, (int)all_keys_.size());
    hotset_keys_ = std::vector<std::string>(all_keys_.begin(), all_keys_.begin() + hs);

    std::cout << "Loaded " << all_keys_.size() << " accounts, hotset size: "
              << hotset_keys_.size() << std::endl;
}

std::string WorkloadRunner::pickKey() {
    thread_local std::mt19937 rng(std::random_device{}());
    std::uniform_real_distribution<double> prob(0.0, 1.0);

    if (prob(rng) < hotset_probability_ && !hotset_keys_.empty()) {
        std::uniform_int_distribution<int> d(0, (int)hotset_keys_.size() - 1);
        return hotset_keys_[d(rng)];
    } else {
        std::uniform_int_distribution<int> d(0, (int)all_keys_.size() - 1);
        return all_keys_[d(rng)];
    }
}

// The actual workload1 logic: transfer 1 unit from FROM_KEY to TO_KEY
bool WorkloadRunner::runTransferOCC(
    const std::string& from_key, const std::string& to_key, int tx_id)
{
    OCCTransaction tx(tx_id, db_, commit_log_);

    auto from_raw = tx.read(from_key);
    auto to_raw   = tx.read(to_key);

    if (!from_raw.has_value() || !to_raw.has_value()) return false;

    json from_acc = json::parse(from_raw.value());
    json to_acc   = json::parse(to_raw.value());

    from_acc["balance"] = from_acc["balance"].get<int>() - 1;
    to_acc["balance"]   = to_acc["balance"].get<int>()   + 1;

    tx.write(from_key, from_acc.dump());
    tx.write(to_key,   to_acc.dump());

    return tx.commit();
}

bool WorkloadRunner::runTransferTwoPL(
    const std::string& from_key, const std::string& to_key, int tx_id)
{
    TwoPLTransaction tx(tx_id, db_, lock_manager_);
    tx.acquireLocks({from_key, to_key});

    auto from_raw = tx.read(from_key);
    auto to_raw   = tx.read(to_key);

    if (!from_raw.has_value() || !to_raw.has_value()) {
        tx.abort();
        return false;
    }

    json from_acc = json::parse(from_raw.value());
    json to_acc   = json::parse(to_raw.value());

    from_acc["balance"] = from_acc["balance"].get<int>() - 1;
    to_acc["balance"]   = to_acc["balance"].get<int>()   + 1;

    tx.write(from_key, from_acc.dump());
    tx.write(to_key,   to_acc.dump());

    tx.commit();
    return true;
}

void WorkloadRunner::workerThread() {
    thread_local std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> wait_dist(10, 50);

    for (int i = 0; i < transactions_per_thread_; i++) {
        // Pick two different keys
        std::string from_key, to_key;
        do {
            from_key = pickKey();
            to_key   = pickKey();
        } while (from_key == to_key);

        bool committed = false;
        bool retried   = false;
        auto start = std::chrono::steady_clock::now();

        while (!committed) {
            int tx_id = next_tx_id_.fetch_add(1);

            if (protocol_ == Protocol::OCC)
                committed = runTransferOCC(from_key, to_key, tx_id);
            else
                committed = runTransferTwoPL(from_key, to_key, tx_id);

            if (!committed) {
                retried = true;
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(wait_dist(rng)));
            }
        }

        auto end = std::chrono::steady_clock::now();
        long long rt = std::chrono::duration_cast<std::chrono::microseconds>
                       (end - start).count();

        std::lock_guard<std::mutex> guard(stats_mutex_);
        all_stats_.push_back({rt, retried});
    }
}

void WorkloadRunner::run() {
    std::cout << "\nRunning workload | protocol: "
              << (protocol_ == Protocol::OCC ? "OCC" : "2PL")
              << " | threads: " << num_threads_
              << " | hotset_prob: " << hotset_probability_ << "\n";

    auto start = std::chrono::steady_clock::now();

    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads_; i++)
        threads.emplace_back(&WorkloadRunner::workerThread, this);
    for (auto& t : threads) t.join();

    auto end = std::chrono::steady_clock::now();
    double seconds = std::chrono::duration_cast<std::chrono::microseconds>
                     (end - start).count() / 1e6;

    printStats();
    int total_tx = num_threads_ * transactions_per_thread_;
    std::cout << "Throughput: " << (total_tx / seconds)
              << " transactions/sec\n";
}

void WorkloadRunner::printStats() {
    int total = all_stats_.size();
    int retried = 0;
    long long total_time = 0;

    for (const auto& s : all_stats_) {
        if (s.was_retried) retried++;
        total_time += s.response_time_us;
    }

    double avg_rt    = total > 0 ? total_time / (double)total : 0;
    double retry_pct = total > 0 ? 100.0 * retried / total : 0;

    std::cout << "Total transactions: " << total << "\n"
              << "Retried:            " << retried
              << " (" << retry_pct << "%)\n"
              << "Avg response time:  " << avg_rt << " microseconds\n";
}