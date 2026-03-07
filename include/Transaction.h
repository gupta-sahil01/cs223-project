#pragma once

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <chrono>
#include "Database.h"

enum class TxStatus {
    ACTIVE,
    COMMITTED,
    ABORTED
};

class Transaction {
public:
    Transaction(Database& db);

    std::optional<std::string> read(const std::string& key);

    void write(const std::string& key, const std::string& value);

    void commit();

    void abort();

    const std::unordered_set<std::string>& getReadSet() const;
    const std::unordered_map<std::string, std::string>& getWriteBuffer() const;
    TxStatus getStatus() const;
    std::chrono::steady_clock::time_point getStartTime() const;

private:
    Database& db_;                                        
    std::unordered_set<std::string> read_set_;            
    std::unordered_map<std::string, std::string> write_buffer_; 
    TxStatus status_;
    std::chrono::steady_clock::time_point start_time_;
};