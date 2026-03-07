#pragma once

#include <string>
#include <unordered_set>
#include "Transaction.h"
#include "LockManager.h"

class TwoPLTransaction {
public:
    TwoPLTransaction(int tx_id, Database& db, LockManager& lm);

    void acquireLocks(const std::unordered_set<std::string>& keys);

    std::optional<std::string> read(const std::string& key);
    void write(const std::string& key, const std::string& value);

    void commit();

    void abort();

    int getId() const;

private:
    int tx_id_;
    Transaction tx_;
    LockManager& lm_;
    std::unordered_set<std::string> keys_;
};