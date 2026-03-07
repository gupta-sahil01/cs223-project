#include "Database.h"
#include "WorkloadRunner.h"
#include <iostream>

int main(int argc, char* argv[]) {
    if (argc < 7) {
        std::cerr << "Usage: ./db_test <protocol: occ|2pl> <threads> "
                  << "<hotset_prob> <hotset_size> <tx_per_thread> <insert_file>\n";
        return 1;
    }

    std::string protocol_str   = argv[1];
    int num_threads            = std::stoi(argv[2]);
    double hotset_prob         = std::stod(argv[3]);
    int hotset_size            = std::stoi(argv[4]);
    int tx_per_thread          = std::stoi(argv[5]);
    std::string insert_file    = argv[6];

    Protocol protocol = (protocol_str == "occ") ? Protocol::OCC : Protocol::TWO_PL;

    Database db("./testdb");
    WorkloadRunner runner(db, protocol, num_threads, hotset_prob,
                          hotset_size, tx_per_thread);

    runner.loadInitialData(insert_file);
    runner.run();

    return 0;
}