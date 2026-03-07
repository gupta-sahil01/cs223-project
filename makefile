ROCKSDB=/opt/homebrew/Cellar/rocksdb/10.10.1

CXX=g++
CXXFLAGS=-std=c++17 -I./include -I$(ROCKSDB)/include
LDFLAGS=-L$(ROCKSDB)/lib -lrocksdb -lz

all:
	$(CXX) $(CXXFLAGS) src/main.cpp src/Database.cpp src/Transaction.cpp \
	src/LockManager.cpp src/TwoPLTransaction.cpp \
	src/CommitLog.cpp src/OCCTransaction.cpp \
	src/WorkloadRunner.cpp $(LDFLAGS) -o db_test