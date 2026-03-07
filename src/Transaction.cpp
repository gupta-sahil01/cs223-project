#include "Transaction.h"
#include <stdexcept>

Transaction::Transaction(Database& db)
    : db_(db),
      status_(TxStatus::ACTIVE),
      start_time_(std::chrono::steady_clock::now())  
{}

std::optional<std::string> Transaction::read(const std::string& key) {
    if (status_ != TxStatus::ACTIVE)
        throw std::runtime_error("Cannot read from inactive transaction");

    auto it = write_buffer_.find(key);
    if (it != write_buffer_.end()) {
        return it->second;  
    }

    read_set_.insert(key);
    return db_.get(key);
}

void Transaction::write(const std::string& key, const std::string& value) {
    if (status_ != TxStatus::ACTIVE)
        throw std::runtime_error("Cannot write to inactive transaction");

    write_buffer_[key] = value;  
}

void Transaction::commit() {
    if (status_ != TxStatus::ACTIVE)
        throw std::runtime_error("Cannot commit inactive transaction");

    for (const auto& [key, value] : write_buffer_) {
        db_.put(key, value);
    }

    status_ = TxStatus::COMMITTED;
}

void Transaction::abort() {
    
    write_buffer_.clear();
    read_set_.clear();
    status_ = TxStatus::ABORTED;
}

const std::unordered_set<std::string>& Transaction::getReadSet() const {
    return read_set_;
}

const std::unordered_map<std::string, std::string>& Transaction::getWriteBuffer() const {
    return write_buffer_;
}

TxStatus Transaction::getStatus() const {
    return status_;
}

std::chrono::steady_clock::time_point Transaction::getStartTime() const {
    return start_time_;
}