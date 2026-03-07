cmake_minimum_required(VERSION 3.15)
project(cs223_project)

set(CMAKE_CXX_STANDARD 17)

# Find RocksDB - works on both Mac and Windows
find_path(ROCKSDB_INCLUDE_DIR rocksdb/db.h
    HINTS
    /opt/homebrew/include                          # Mac Apple Silicon
    /usr/local/include                             # Mac Intel
    "C:/rocksdb/include"                           # Windows common path
    "C:/Program Files/rocksdb/include"
)

find_library(ROCKSDB_LIB rocksdb
    HINTS
    /opt/homebrew/lib                              # Mac Apple Silicon
    /usr/local/lib                                 # Mac Intel
    "C:/rocksdb/lib"                               # Windows common path
    "C:/Program Files/rocksdb/lib"
)

if(NOT ROCKSDB_INCLUDE_DIR OR NOT ROCKSDB_LIB)
    message(FATAL_ERROR "RocksDB not found. Please set ROCKSDB_INCLUDE_DIR and ROCKSDB_LIB manually.")
endif()

message(STATUS "RocksDB include: ${ROCKSDB_INCLUDE_DIR}")
message(STATUS "RocksDB lib:     ${ROCKSDB_LIB}")

# All source files
set(SOURCES
    src/main.cpp
    src/Database.cpp
    src/Transaction.cpp
    src/LockManager.cpp
    src/TwoPLTransaction.cpp
    src/CommitLog.cpp
    src/OCCTransaction.cpp
    src/WorkloadRunner.cpp
)

add_executable(db_test ${SOURCES})

target_include_directories(db_test PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${ROCKSDB_INCLUDE_DIR}
)

target_link_libraries(db_test PRIVATE ${ROCKSDB_LIB})

# Link extra libs needed on each platform
if(WIN32)
    target_link_libraries(db_test PRIVATE rpcrt4)  # Windows needs this for RocksDB
else()
    find_package(Threads REQUIRED)
    target_link_libraries(db_test PRIVATE Threads::Threads ${CMAKE_DL_LIBS})
endif()