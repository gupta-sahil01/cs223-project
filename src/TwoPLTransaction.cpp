#include "TwoPLTransaction.h"
#include <thread>
#include <chrono>
#include <iostream>
#include <random>

TwoPLTransaction::TwoPLTransaction(int tx_id, Database& db, LockManager& lm)
    : tx_id_(tx_id), tx_(db), lm_(lm)
{}

void TwoPLTransaction::acquireLocks(const std::unordered_set<std::string>& keys) {
    keys_ = keys;

    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<int> wait_dist(10, 50);

    int retries = 0;
    while (true) {
        if (lm_.acquireAll(tx_id_, keys_)) {
            return;
        }

        lm_.releaseAll(tx_id_);
        retries++;

        if (retries % 10 == 0) {
            std::cout << "TX " << tx_id_ << " has retried " << retries << " times (possible livelock)" << std::endl;
        }

        int wait_ms = wait_dist(rng);
        std::this_thread::sleep_for(std::chrono::milliseconds(wait_ms));
    }
}

std::optional<std::string> TwoPLTransaction::read(const std::string& key) {
    return tx_.read(key);
}

void TwoPLTransaction::write(const std::string& key, const std::string& value) {
    tx_.write(key, value);
}

void TwoPLTransaction::commit() {
    lm_.releaseAll(tx_id_);
    tx_.commit();
}

void TwoPLTransaction::abort() {
    lm_.releaseAll(tx_id_);
    tx_.abort();
}

int TwoPLTransaction::getId() const {
    return tx_id_;
}