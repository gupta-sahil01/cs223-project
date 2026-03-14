SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BINARY="$SCRIPT_DIR/build/db_test"
INPUT1="$SCRIPT_DIR/Workloads/Workloads/workload1/input1.txt"
INPUT2="$SCRIPT_DIR/Workloads/Workloads/workload2/input2.txt"
RESULTS_DIR="$SCRIPT_DIR/results"
DIST_DIR="$RESULTS_DIR/dist"

mkdir -p "$RESULTS_DIR" "$DIST_DIR"

THREAD_COUNTS=(1 2 4 8 16)
HOTSET_PROBS=(0.0 0.2 0.4 0.6 0.8 1.0)
PROTOCOLS=("occ" "2pl")

FIXED_THREADS=4        
FIXED_HOTSET_PROB=0.5  
HOTSET_SIZE=10
TX_PER_THREAD=200

DIST_THREADS=4
DIST_HOTSET_PROB=0.5
DIST_TX_PER_THREAD=500   

CSV_HEADER="protocol,threads,hotset_prob,hotset_size,throughput_tps,avg_rt_us,retry_pct,total_retries"

run_sweep() {
    local WL=$1 PROTO=$2 THREADS=$3 HPROB=$4 HSIZE=$5 CSV=$6

    local DB_PATH="./db_tmp"
    rm -rf "$DB_PATH"

    local INPUT
    [ "$WL" = "1" ] && INPUT="$INPUT1" || INPUT="$INPUT2"

    OUTPUT=$("$BINARY" "$PROTO" "$WL" "$THREADS" "$HPROB" "$HSIZE" \
             "$TX_PER_THREAD" "$INPUT" "$DB_PATH" 2>&1)

    rm -rf "$DB_PATH"

    THROUGHPUT=$(echo "$OUTPUT" | grep "Throughput:"        | awk '{print $2}')
    AVG_RT=$(    echo "$OUTPUT" | grep "Avg response time"  | awk '{print $4}')
    RETRY_PCT=$( echo "$OUTPUT" | grep "Had at least"       | grep -oE '[0-9]+\.?[0-9]*%' | tr -d '%')
    TOTAL_RET=$( echo "$OUTPUT" | grep "Total retries"      | awk '{print $3}')

    echo "$PROTO,$THREADS,$HPROB,$HSIZE,$THROUGHPUT,$AVG_RT,$RETRY_PCT,$TOTAL_RET" >> "$CSV"

    printf "  %-4s  threads=%-3s  hprob=%-4s  → %s tx/s  rt=%s µs  retry=%s%%\n" \
           "$PROTO" "$THREADS" "$HPROB" "$THROUGHPUT" "$AVG_RT" "$RETRY_PCT"
}

run_dist() {
    local WL=$1 PROTO=$2 DIST_CSV=$3

    local DB_PATH="./db_tmp_dist"
    rm -rf "$DB_PATH"

    local INPUT
    [ "$WL" = "1" ] && INPUT="$INPUT1" || INPUT="$INPUT2"

    # The 9th argument is the dist_file path — main.cpp passes it to runner.run()
    "$BINARY" "$PROTO" "$WL" "$DIST_THREADS" "$DIST_HOTSET_PROB" "$HOTSET_SIZE" \
              "$DIST_TX_PER_THREAD" "$INPUT" "$DB_PATH" "$DIST_CSV" > /dev/null 2>&1

    rm -rf "$DB_PATH"
    echo "  distribution saved → $DIST_CSV"
}

run_workload() {
    local WL=$1 CSV=$2

    echo "$CSV_HEADER" > "$CSV"

    echo "── Varying threads  (hotset_prob=$FIXED_HOTSET_PROB) ──"
    for PROTO in "${PROTOCOLS[@]}"; do
        for T in "${THREAD_COUNTS[@]}"; do
            run_sweep "$WL" "$PROTO" "$T" "$FIXED_HOTSET_PROB" "$HOTSET_SIZE" "$CSV"
        done
    done

    echo "── Varying contention  (threads=$FIXED_THREADS) ──"
    for PROTO in "${PROTOCOLS[@]}"; do
        for HPROB in "${HOTSET_PROBS[@]}"; do
            run_sweep "$WL" "$PROTO" "$FIXED_THREADS" "$HPROB" "$HOTSET_SIZE" "$CSV"
        done
    done

    echo "── Collecting RT distributions ──"
    for PROTO in "${PROTOCOLS[@]}"; do
        DIST_OUT="$DIST_DIR/w${WL}_${PROTO}_dist.csv"
        run_dist "$WL" "$PROTO" "$DIST_OUT"
    done

    echo "Results saved → $CSV"
}

echo "════════════════════════════════════════"
echo " WORKLOAD 1"
echo "════════════════════════════════════════"
run_workload 1 "$RESULTS_DIR/workload1.csv"

echo ""
echo "════════════════════════════════════════"
echo " WORKLOAD 2"
echo "════════════════════════════════════════"
run_workload 2 "$RESULTS_DIR/workload2.csv"

echo ""
echo "════════════════════════════════════════"
echo " All done!  Results in: $RESULTS_DIR/"
echo "  workload1.csv"
echo "  workload2.csv"
echo "  dist/w1_occ_dist.csv"
echo "  dist/w1_2pl_dist.csv"
echo "  dist/w2_occ_dist.csv"
echo "  dist/w2_2pl_dist.csv"
echo "════════════════════════════════════════"