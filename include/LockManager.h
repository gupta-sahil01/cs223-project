#pragma once

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <mutex>

class LockManager {
public:
    
    bool acquireAll(int tx_id, const std::unordered_set<std::string>& keys);

    void releaseAll(int tx_id);

private:
    std::mutex mutex_;

    std::unordered_map<std::string, int> lock_table_;

    std::unordered_map<int, std::unordered_set<std::string>> held_locks_;
};