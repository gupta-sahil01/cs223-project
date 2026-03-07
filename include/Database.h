#pragma once

#include <string>
#include <optional>
#include "rocksdb/db.h"

class Database {
public:
    
    Database(const std::string& db_path);

    ~Database();

    std::optional<std::string> get(const std::string& key);

    void put(const std::string& key, const std::string& value);

private:
    rocksdb::DB* db_;  
};