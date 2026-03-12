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
#include <algorithm>

static std::string normalizeJSON(const std::string& raw) {
    std::string result = raw;
    result = std::regex_replace(result, std::regex(R"((\w+)\s*:)"), "\"$1\":");
    return result;
}

WorkloadRunner::WorkloadRunner(
    Database& db, Protocol protocol, WorkloadId workload,
    int num_threads, double hotset_probability,
    int hotset_size, int transactions_per_thread)
    : db_(db), protocol_(protocol), workload_(workload),
      num_threads_(num_threads), hotset_probability_(hotset_probability),
      hotset_size_(hotset_size), transactions_per_thread_(transactions_per_thread)
{}

void WorkloadRunner::loadInitialData(const std::string& insert_file) {
    std::ifstream file(insert_file);
    if (!file.is_open())
        throw std::runtime_error("Cannot open insert file: " + insert_file);

    std::string line;
    while (std::getline(file, line)) {
        if (!line.empty() && line.back() == '\r') line.pop_back();
        if (line == "INSERT" || line == "END" || line.empty()) continue;

        if (line.rfind("KEY:", 0) == 0) {
            size_t sep        = line.find(", VALUE: ");
            std::string key   = line.substr(5, sep - 5);
            std::string value = line.substr(sep + 9);

            db_.put(key, normalizeJSON(value));

            if      (key.rfind("A_", 0) == 0) account_keys_.push_back(key);
            else if (key.rfind("W_", 0) == 0) warehouse_keys_.push_back(key);
            else if (key.rfind("D_", 0) == 0) district_keys_.push_back(key);
            else if (key.rfind("C_", 0) == 0) customer_keys_.push_back(key);
            else if (key.rfind("S_", 0) == 0) stock_keys_.push_back(key);
        }
    }

    auto buildHot = [&](const std::vector<std::string>& src,
                        std::vector<std::string>& dst) {
        int n = std::min(hotset_size_, (int)src.size());
        dst   = std::vector<std::string>(src.begin(), src.begin() + n);
    };

    buildHot(account_keys_,   hot_account_keys_);
    buildHot(district_keys_,  hot_district_keys_);
    buildHot(stock_keys_,     hot_stock_keys_);
    buildHot(customer_keys_,  hot_customer_keys_);
    buildHot(warehouse_keys_, hot_warehouse_keys_);

    std::cout << "Loaded data:"
              << "  accounts="   << account_keys_.size()
              << "  warehouses=" << warehouse_keys_.size()
              << "  districts="  << district_keys_.size()
              << "  customers="  << customer_keys_.size()
              << "  stock="      << stock_keys_.size()
              << "  hotset_size=" << hotset_size_ << "\n";
}

std::string WorkloadRunner::pickFrom(const std::vector<std::string>& hot,
                                     const std::vector<std::string>& all) {
    thread_local std::mt19937 rng(std::random_device{}());
    std::uniform_real_distribution<double> prob(0.0, 1.0);

    const std::vector<std::string>& pool =
        (!hot.empty() && prob(rng) < hotset_probability_) ? hot : all;

    std::uniform_int_distribution<int> d(0, (int)pool.size() - 1);
    return pool[d(rng)];
}

bool WorkloadRunner::runTransferOCC(
    const std::string& from_key, const std::string& to_key, int tx_id)
{
    OCCTransaction tx(tx_id, db_, commit_log_);

    auto from_raw = tx.read(from_key);
    auto to_raw   = tx.read(to_key);
    if (!from_raw || !to_raw) return false;

    json from_acc = json::parse(*from_raw);
    json to_acc   = json::parse(*to_raw);

    from_acc["balance"] = from_acc["balance"].get<int>() - 1;
    to_acc  ["balance"] = to_acc  ["balance"].get<int>() + 1;

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
    if (!from_raw || !to_raw) { tx.abort(); return false; }

    json from_acc = json::parse(*from_raw);
    json to_acc   = json::parse(*to_raw);

    from_acc["balance"] = from_acc["balance"].get<int>() - 1;
    to_acc  ["balance"] = to_acc  ["balance"].get<int>() + 1;

    tx.write(from_key, from_acc.dump());
    tx.write(to_key,   to_acc.dump());
    tx.commit();
    return true;
}

bool WorkloadRunner::runNewOrderOCC(
    const std::string& d_key,
    const std::string& s1, const std::string& s2, const std::string& s3,
    int tx_id)
{
    OCCTransaction tx(tx_id, db_, commit_log_);

    auto d_raw  = tx.read(d_key);
    auto s1_raw = tx.read(s1);
    auto s2_raw = tx.read(s2);
    auto s3_raw = tx.read(s3);
    if (!d_raw || !s1_raw || !s2_raw || !s3_raw) return false;

    json d   = json::parse(*d_raw);
    json js1 = json::parse(*s1_raw);
    json js2 = json::parse(*s2_raw);
    json js3 = json::parse(*s3_raw);

    d["next_o_id"] = d["next_o_id"].get<int>() + 1;
    for (json* s : {&js1, &js2, &js3}) {
        (*s)["qty"]       = (*s)["qty"].get<int>()       - 1;
        (*s)["ytd"]       = (*s)["ytd"].get<int>()       + 1;
        (*s)["order_cnt"] = (*s)["order_cnt"].get<int>() + 1;
    }

    tx.write(d_key, d.dump());
    tx.write(s1,    js1.dump());
    tx.write(s2,    js2.dump());
    tx.write(s3,    js3.dump());
    return tx.commit();
}

bool WorkloadRunner::runNewOrderTwoPL(
    const std::string& d_key,
    const std::string& s1, const std::string& s2, const std::string& s3,
    int tx_id)
{
    TwoPLTransaction tx(tx_id, db_, lock_manager_);
    tx.acquireLocks({d_key, s1, s2, s3});

    auto d_raw  = tx.read(d_key);
    auto s1_raw = tx.read(s1);
    auto s2_raw = tx.read(s2);
    auto s3_raw = tx.read(s3);
    if (!d_raw || !s1_raw || !s2_raw || !s3_raw) { tx.abort(); return false; }

    json d   = json::parse(*d_raw);
    json js1 = json::parse(*s1_raw);
    json js2 = json::parse(*s2_raw);
    json js3 = json::parse(*s3_raw);

    d["next_o_id"] = d["next_o_id"].get<int>() + 1;
    for (json* s : {&js1, &js2, &js3}) {
        (*s)["qty"]       = (*s)["qty"].get<int>()       - 1;
        (*s)["ytd"]       = (*s)["ytd"].get<int>()       + 1;
        (*s)["order_cnt"] = (*s)["order_cnt"].get<int>() + 1;
    }

    tx.write(d_key, d.dump());
    tx.write(s1,    js1.dump());
    tx.write(s2,    js2.dump());
    tx.write(s3,    js3.dump());
    tx.commit();
    return true;
}

bool WorkloadRunner::runPaymentOCC(
    const std::string& w_key, const std::string& d_key,
    const std::string& c_key, int tx_id)
{
    OCCTransaction tx(tx_id, db_, commit_log_);

    auto w_raw = tx.read(w_key);
    auto d_raw = tx.read(d_key);
    auto c_raw = tx.read(c_key);
    if (!w_raw || !d_raw || !c_raw) return false;

    json w = json::parse(*w_raw);
    json d = json::parse(*d_raw);
    json c = json::parse(*c_raw);

    w["ytd"]         = w["ytd"].get<int>()         + 5;
    d["ytd"]         = d["ytd"].get<int>()         + 5;
    c["balance"]     = c["balance"].get<int>()     - 5;
    c["ytd_payment"] = c["ytd_payment"].get<int>() + 5;
    c["payment_cnt"] = c["payment_cnt"].get<int>() + 1;

    tx.write(w_key, w.dump());
    tx.write(d_key, d.dump());
    tx.write(c_key, c.dump());
    return tx.commit();
}

bool WorkloadRunner::runPaymentTwoPL(
    const std::string& w_key, const std::string& d_key,
    const std::string& c_key, int tx_id)
{
    TwoPLTransaction tx(tx_id, db_, lock_manager_);
    tx.acquireLocks({w_key, d_key, c_key});

    auto w_raw = tx.read(w_key);
    auto d_raw = tx.read(d_key);
    auto c_raw = tx.read(c_key);
    if (!w_raw || !d_raw || !c_raw) { tx.abort(); return false; }

    json w = json::parse(*w_raw);
    json d = json::parse(*d_raw);
    json c = json::parse(*c_raw);

    w["ytd"]         = w["ytd"].get<int>()         + 5;
    d["ytd"]         = d["ytd"].get<int>()         + 5;
    c["balance"]     = c["balance"].get<int>()     - 5;
    c["ytd_payment"] = c["ytd_payment"].get<int>() + 5;
    c["payment_cnt"] = c["payment_cnt"].get<int>() + 1;

    tx.write(w_key, w.dump());
    tx.write(d_key, d.dump());
    tx.write(c_key, c.dump());
    tx.commit();
    return true;
}

void WorkloadRunner::workerThread() {
    if (workload_ == WorkloadId::WORKLOAD1) workerThreadW1();
    else                                    workerThreadW2();
}

void WorkloadRunner::workerThreadW1() {
    thread_local std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> wait_dist(10, 50);

    for (int i = 0; i < transactions_per_thread_; i++) {
        std::string from_key, to_key;
        do {
            from_key = pickFrom(hot_account_keys_, account_keys_);
            to_key   = pickFrom(hot_account_keys_, account_keys_);
        } while (from_key == to_key);

        int  retry_count = 0;
        bool committed   = false;
        auto start       = std::chrono::steady_clock::now();

        while (!committed) {
            int tx_id = next_tx_id_.fetch_add(1);
            committed = (protocol_ == Protocol::OCC)
                ? runTransferOCC  (from_key, to_key, tx_id)
                : runTransferTwoPL(from_key, to_key, tx_id);

            if (!committed) {
                retry_count++;
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(wait_dist(rng)));
            }
        }

        auto end = std::chrono::steady_clock::now();
        long long rt = std::chrono::duration_cast<std::chrono::microseconds>
                       (end - start).count();

        std::lock_guard<std::mutex> guard(stats_mutex_);
        all_stats_.push_back({rt, retry_count, 0});
    }
}

void WorkloadRunner::workerThreadW2() {
    thread_local std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> wait_dist(10, 50);
    std::uniform_int_distribution<int> tx_type_dist(0, 1);

    for (int i = 0; i < transactions_per_thread_; i++) {
        int  tx_type     = tx_type_dist(rng);
        int  retry_count = 0;
        bool committed   = false;
        auto start       = std::chrono::steady_clock::now();

        while (!committed) {
            int tx_id = next_tx_id_.fetch_add(1);

            if (tx_type == 0) {
                std::string d_key = pickFrom(hot_district_keys_, district_keys_);
                std::string s1, s2, s3;
                s1 = pickFrom(hot_stock_keys_, stock_keys_);
                do { s2 = pickFrom(hot_stock_keys_, stock_keys_); } while (s2 == s1);
                do { s3 = pickFrom(hot_stock_keys_, stock_keys_); } while (s3 == s1 || s3 == s2);

                committed = (protocol_ == Protocol::OCC)
                    ? runNewOrderOCC  (d_key, s1, s2, s3, tx_id)
                    : runNewOrderTwoPL(d_key, s1, s2, s3, tx_id);
            } else {
                std::string w_key = pickFrom(hot_warehouse_keys_, warehouse_keys_);
                std::string d_key = pickFrom(hot_district_keys_,  district_keys_);
                std::string c_key = pickFrom(hot_customer_keys_,  customer_keys_);

                committed = (protocol_ == Protocol::OCC)
                    ? runPaymentOCC  (w_key, d_key, c_key, tx_id)
                    : runPaymentTwoPL(w_key, d_key, c_key, tx_id);
            }

            if (!committed) {
                retry_count++;
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(wait_dist(rng)));
            }
        }

        auto end = std::chrono::steady_clock::now();
        long long rt = std::chrono::duration_cast<std::chrono::microseconds>
                       (end - start).count();

        std::lock_guard<std::mutex> guard(stats_mutex_);
        all_stats_.push_back({rt, retry_count, tx_type});
    }
}

void WorkloadRunner::run(const std::string& dist_file) {
    std::cout << "\nRunning workload " << (workload_ == WorkloadId::WORKLOAD1 ? "1" : "2")
              << " | protocol: "    << (protocol_ == Protocol::OCC ? "OCC" : "2PL")
              << " | threads: "     << num_threads_
              << " | hotset_prob: " << hotset_probability_
              << " | hotset_size: " << hotset_size_ << "\n";

    auto wall_start = std::chrono::steady_clock::now();

    std::vector<std::thread> threads;
    threads.reserve(num_threads_);
    for (int i = 0; i < num_threads_; i++)
        threads.emplace_back(&WorkloadRunner::workerThread, this);
    for (auto& t : threads) t.join();

    auto   wall_end = std::chrono::steady_clock::now();
    double secs     = std::chrono::duration_cast<std::chrono::microseconds>
                      (wall_end - wall_start).count() / 1e6;

    printStats(dist_file);

    int total_tx = num_threads_ * transactions_per_thread_;
    std::cout << "Throughput: " << (total_tx / secs)
              << " transactions/sec\n";
}

void WorkloadRunner::printStats(const std::string& dist_file) {
    int       total         = (int)all_stats_.size();
    int       retried       = 0;
    int       total_retries = 0;
    long long total_time    = 0;

    struct TypeStats { int count = 0; long long time = 0; int retries = 0; };
    TypeStats type_stats[2];

    for (const auto& s : all_stats_) {
        if (s.retry_count > 0) retried++;
        total_retries += s.retry_count;
        total_time    += s.response_time_us;

        int t = (s.tx_type == 0 || s.tx_type == 1) ? s.tx_type : 0;
        type_stats[t].count++;
        type_stats[t].time    += s.response_time_us;
        type_stats[t].retries += s.retry_count;
    }

    double avg_rt    = total > 0 ? total_time / (double)total : 0;
    double retry_pct = total > 0 ? 100.0 * retried / total    : 0;

    std::cout << "─────────────────────────────────────\n"
              << "Total transactions:   " << total         << "\n"
              << "Had at least 1 retry: " << retried
              << " (" << retry_pct << "%)\n"
              << "Total retries:        " << total_retries << "\n"
              << "Avg response time:    " << avg_rt        << " µs\n";

    if (workload_ == WorkloadId::WORKLOAD2) {
        const char* names[2] = {"NewOrder", "Payment"};
        for (int i = 0; i < 2; i++) {
            if (type_stats[i].count == 0) continue;
            double avg = type_stats[i].time / (double)type_stats[i].count;
            std::cout << "  [" << names[i] << "]"
                      << "  count="   << type_stats[i].count
                      << "  avg_rt="  << avg << " µs"
                      << "  retries=" << type_stats[i].retries << "\n";
        }
    }
    std::cout << "─────────────────────────────────────\n";

    if (!dist_file.empty()) {
        std::ofstream f(dist_file);
        if (f.is_open()) {
            f << "tx_type,response_time_us,retry_count\n";
            for (const auto& s : all_stats_) {
                std::string type_name;
                if (workload_ == WorkloadId::WORKLOAD1)
                    type_name = "transfer";
                else
                    type_name = (s.tx_type == 0) ? "NewOrder" : "Payment";

                f << type_name << ","
                  << s.response_time_us << ","
                  << s.retry_count      << "\n";
            }
        }
    }
}