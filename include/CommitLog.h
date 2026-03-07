#pragma once

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <mutex>
#include <chrono>

struct CommitRecord {
    int tx_id;
    std::unordered_set<std::string> write_set;
    std::chrono::steady_clock::time_point commit_time;
};

class CommitLog {
public:
    void recordCommit(int tx_id, const std::unordered_set<std::string>& write_set);

    bool hasConflict(
        const std::unordered_set<std::string>& read_set,
        const std::chrono::steady_clock::time_point& start_time
    );

    std::mutex validation_mutex;

private:
    std::mutex log_mutex_;
    std::vector<CommitRecord> log_;
};