# CS 223 – Multi-Threaded Transaction Processing Layer

A multi-threaded transaction processing system built on top of RocksDB, implementing and comparing two concurrency control protocols:

- **Optimistic Concurrency Control (OCC)**
- **Conservative Two-Phase Locking (Conservative 2PL)**

---

## Third-Party Dependencies

| Dependency | Version | Purpose |
|---|---|---|
| [RocksDB](https://rocksdb.org/) | 10.x | Embedded key-value storage layer |
| [nlohmann/json](https://github.com/nlohmann/json) | 3.x | JSON parsing for record values (header-only, included in `include/`) |
| [CMake](https://cmake.org/) | 3.15+ | Build system (optional, Makefile also provided) |

---

## Build Instructions

### Mac (Apple Silicon / Intel)

**1. Install RocksDB via Homebrew:**
```bash
brew install rocksdb
```

**2. Build using Make:**
```bash
make
```

This produces the `db_test` executable in the project root.

**If RocksDB is installed at a non-default path**, edit the `ROCKSDB` variable at the top of the `Makefile`:
```makefile
ROCKSDB=/opt/homebrew/Cellar/rocksdb/10.10.1   # Apple Silicon default
# ROCKSDB=/usr/local/Cellar/rocksdb/10.10.1    # Intel Mac
```

**3. Build using CMake (alternative):**
```bash
mkdir build && cd build
cmake ..
make
```

### Windows (MSYS2 / ucrt64)

**1. Install RocksDB:**
```bash
pacman -S mingw-w64-ucrt-x86_64-rocksdb
```

**2. Build using CMake:**
```bash
mkdir build && cd build
cmake .. -G "MinGW Makefiles"
mingw32-make
```

---

## Running the Workloads

### Binary Usage

```
./db_test <protocol> <workload> <threads> <hotset_prob> <hotset_size> <tx_per_thread> <insert_file> <db_path> [dist_output_csv]
```

| Argument | Description |
|---|---|
| `protocol` | `occ` or `2pl` |
| `workload` | `1` (Transfer) or `2` (NewOrder + Payment) |
| `threads` | Number of worker threads |
| `hotset_prob` | Probability of picking a hot key (0.0–1.0) |
| `hotset_size` | Number of keys in the hotset |
| `tx_per_thread` | Transactions each thread executes |
| `insert_file` | Path to the initial data file (`input1.txt` or `input2.txt`) |
| `db_path` | Path where RocksDB will store data (created automatically) |
| `dist_output_csv` | *(Optional)* Path to write per-transaction response times for distribution plots |

> **Important:** Always use a fresh `db_path` for each run, or delete it between runs: `rm -rf ./mydb`

---

### Workload 1 – Bank Transfer

Each transaction transfers 1 unit of balance between two randomly selected accounts.

**OCC:**
```bash
rm -rf ./db_w1
./db_test occ 1 4 0.5 10 200 Workloads/workload1/input1.txt ./db_w1
```

**Conservative 2PL:**
```bash
rm -rf ./db_w1
./db_test 2pl 1 4 0.5 10 200 Workloads/workload1/input1.txt ./db_w1
```

---

### Workload 2 – NewOrder + Payment

Runs a 50/50 mix of two transaction types:
- **NewOrder:** increments a district's order counter and updates 3 stock items
- **Payment:** updates warehouse and district YTD totals and deducts from a customer balance

**OCC:**
```bash
rm -rf ./db_w2
./db_test occ 2 4 0.5 10 200 Workloads/workload2/input2.txt ./db_w2
```

**Conservative 2PL:**
```bash
rm -rf ./db_w2
./db_test 2pl 2 4 0.5 10 200 Workloads/workload2/input2.txt ./db_w2
```

---

## Setting Number of Threads and Contention Level

### Number of Threads
Pass the desired thread count as the 3rd argument:
```bash
# 1, 2, 4, 8, or 16 threads
./db_test occ 1 8 0.5 10 200 input1.txt ./db_w1
#                ^ threads
```

### Contention Level
Contention is controlled by two parameters:

- **`hotset_prob`** (4th argument): probability (0.0–1.0) that a transaction picks a key from the hotset. Higher = more contention.
- **`hotset_size`** (5th argument): number of keys in the hotset. Smaller = more contention.

```bash
# Low contention: keys spread across full keyspace
./db_test occ 1 4 0.1 10 200 input1.txt ./db_w1

# High contention: 90% of accesses hit the same 5 keys
./db_test occ 1 4 0.9 5 200 input1.txt ./db_w1
```

---

## Running All Experiments

A shell script is provided to run the full experiment sweep (all thread counts, all contention levels, both protocols, both workloads):

```bash
chmod +x run_experiments.sh
./run_experiments.sh
```

Results are saved to `results/`:
- `results/workload1.csv` — throughput, avg response time, retry % per run
- `results/workload2.csv` — same for workload 2
- `results/dist/` — per-transaction response time CSVs for distribution plots

### Generating Plots

```bash
pip3 install pandas matplotlib numpy
python3 plot_results.py
```

Plots are saved to `results/plots/`.

---

## Project Structure

```
.
├── src/
│   ├── main.cpp
│   ├── Database.cpp
│   ├── Transaction.cpp
│   ├── LockManager.cpp
│   ├── TwoPLTransaction.cpp
│   ├── CommitLog.cpp
│   ├── OCCTransaction.cpp
│   └── WorkloadRunner.cpp
├── include/
│   ├── Database.h
│   ├── Transaction.h
│   ├── LockManager.h
│   ├── TwoPLTransaction.h
│   ├── CommitLog.h
│   ├── OCCTransaction.h
│   ├── WorkloadRunner.h
│   └── json.hpp
├── Workloads/
│   ├── workload1/
│   │   ├── input1.txt
│   │   └── workload1.txt
│   └── workload2/
│       ├── input2.txt
│       └── workload2.txt
├── results/
│   ├── plots/
│   └── dist/
├── Makefile
├── CMakeLists.txt
├── run_experiments.sh
├── plot_results.py
└── README.md
```
