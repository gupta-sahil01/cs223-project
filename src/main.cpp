#include "Database.h"
#include "WorkloadRunner.h"
#include <iostream>

int main(int argc, char* argv[]) {
    if (argc < 9) {
        std::cerr << "Usage: ./db_test <protocol: occ|2pl> "
                     "<workload: 1|2> "
                     "<threads> "
                     "<hotset_prob> "
                     "<hotset_size> "
                     "<tx_per_thread> "
                     "<insert_file> "
                     "<db_path> "
                     "[dist_output_csv]\n\n"
                  << "Examples:\n"
                  << "  ./db_test occ 1 4 0.5 10 200 input1.txt ./db1\n"
                  << "  ./db_test 2pl 2 8 0.7 10 200 input2.txt ./db2 dist.csv\n";
        return 1;
    }

    std::string protocol_str  = argv[1];
    int         workload_id   = std::stoi(argv[2]);
    int         num_threads   = std::stoi(argv[3]);
    double      hotset_prob   = std::stod(argv[4]);
    int         hotset_size   = std::stoi(argv[5]);
    int         tx_per_thread = std::stoi(argv[6]);
    std::string insert_file   = argv[7];
    std::string db_path       = argv[8];
    
    std::string dist_file     = (argc >= 10) ? argv[9] : "";

    Protocol   protocol = (protocol_str == "occ") ? Protocol::OCC : Protocol::TWO_PL;
    WorkloadId workload = (workload_id  == 1)     ? WorkloadId::WORKLOAD1
                                                  : WorkloadId::WORKLOAD2;

    Database db(db_path);
    WorkloadRunner runner(db, protocol, workload, num_threads,
                          hotset_prob, hotset_size, tx_per_thread);

    runner.loadInitialData(insert_file);
    runner.run(dist_file);

    return 0;
}