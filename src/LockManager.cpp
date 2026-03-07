#include "LockManager.h"

bool LockManager::acquireAll(int tx_id, const std::unordered_set<std::string>& keys) {
    std::lock_guard<std::mutex> guard(mutex_);

    for (const auto& key : keys) {
        auto it = lock_table_.find(key);
        if (it != lock_table_.end() && it->second != tx_id) {
            return false;
        }
    }

    for (const auto& key : keys) {
        lock_table_[key] = tx_id;
        held_locks_[tx_id].insert(key);
    }

    return true;
}

void LockManager::releaseAll(int tx_id) {
    std::lock_guard<std::mutex> guard(mutex_);

    auto it = held_locks_.find(tx_id);
    if (it == held_locks_.end()) return;

    for (const auto& key : it->second) {
        lock_table_.erase(key);
    }

    held_locks_.erase(tx_id);
}