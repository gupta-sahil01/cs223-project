#pragma once

#include <string>
#include <unordered_set>
#include "Transaction.h"
#include "CommitLog.h"

class OCCTransaction {
public:
    OCCTransaction(int tx_id, Database& db, CommitLog& commit_log);

    std::optional<std::string> read(const std::string& key);
    void write(const std::string& key, const std::string& value);

    bool commit();

    int getId() const;

private:
    int tx_id_;
    Transaction tx_;
    CommitLog& commit_log_;
};