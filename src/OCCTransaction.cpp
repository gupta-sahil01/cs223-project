#include "OCCTransaction.h"
#include <thread>
#include <chrono>
#include <random>
#include <iostream>

OCCTransaction::OCCTransaction(int tx_id, Database& db, CommitLog& commit_log)
    : tx_id_(tx_id), tx_(db), commit_log_(commit_log)
{}

std::optional<std::string> OCCTransaction::read(const std::string& key) {
    return tx_.read(key);
}

void OCCTransaction::write(const std::string& key, const std::string& value) {
    tx_.write(key, value);
}

bool OCCTransaction::commit() {
    std::lock_guard<std::mutex> guard(commit_log_.validation_mutex);

    if (commit_log_.hasConflict(tx_.getReadSet(), tx_.getStartTime())) {
        tx_.abort();
        std::cout << "TX " << tx_id_ << " validation failed — aborting" << std::endl;
        return false;
    }

    std::unordered_set<std::string> write_set;
    for (const auto& [key, value] : tx_.getWriteBuffer()) {
        write_set.insert(key);
    }

    commit_log_.recordCommit(tx_id_, write_set);
    tx_.commit();
    std::cout << "TX " << tx_id_ << " committed successfully" << std::endl;
    return true;
}

int OCCTransaction::getId() const {
    return tx_id_;
}