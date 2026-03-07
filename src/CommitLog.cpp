#include "CommitLog.h"

void CommitLog::recordCommit(int tx_id, const std::unordered_set<std::string>& write_set) {
    std::lock_guard<std::mutex> guard(log_mutex_);

    log_.push_back({
        tx_id,
        write_set,
        std::chrono::steady_clock::now()
    });
}

bool CommitLog::hasConflict(
    const std::unordered_set<std::string>& read_set,
    const std::chrono::steady_clock::time_point& start_time)
{
    std::lock_guard<std::mutex> guard(log_mutex_);

    for (const auto& record : log_) {
        if (record.commit_time <= start_time) continue;

        for (const auto& key : record.write_set) {
            if (read_set.count(key) > 0) {
                return true;
            }
        }
    }

    return false;
}