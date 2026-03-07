#include "Database.h"
#include "rocksdb/options.h"
#include <stdexcept>
#include <iostream>

Database::Database(const std::string& db_path) {
    rocksdb::Options options;
    options.create_if_missing = true;  

    rocksdb::Status status = rocksdb::DB::Open(options, db_path, &db_);

    if (!status.ok()) {
        throw std::runtime_error("Failed to open RocksDB: " + status.ToString());
    }

    std::cout << "Database opened at: " << db_path << std::endl;
}

Database::~Database() {
    delete db_;  
}

std::optional<std::string> Database::get(const std::string& key) {
    std::string value;
    rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), key, &value);

    if (status.ok()) {
        return value;
    } else if (status.IsNotFound()) {
        return std::nullopt;  
    } else {
        throw std::runtime_error("RocksDB Get failed: " + status.ToString());
    }
}

void Database::put(const std::string& key, const std::string& value) {
    rocksdb::Status status = db_->Put(rocksdb::WriteOptions(), key, value);

    if (!status.ok()) {
        throw std::runtime_error("RocksDB Put failed: " + status.ToString());
    }
}