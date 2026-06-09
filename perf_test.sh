#!/bin/bash
# BRAID Performance Test Suite
# Three scenarios: mbuffer baseline, braid loopback, braid+mbuffer pipeline
set -e

BRAID="./target/release/braid"
DATA_COUNT=16384   # 16 GB in 1M blocks
CHANNELS=4
MTU=65535          # max UDP payload for loopback
BUFFER_SIZE="256m"
RUNS=3

echo "=============================================="
echo " BRAID Performance Test Suite"
echo "=============================================="
echo " Data:     $DATA_COUNT MB ($(( DATA_COUNT / 1024 )) GB)"
echo " Channels: $CHANNELS"
echo " MTU:      $MTU"
echo " Runs:     $RUNS"
echo "=============================================="
echo ""

# ---- Test 1: mbuffer baseline ----
echo "━━━ Test 1: /dev/zero | mbuffer > /dev/null ━━━"
echo "       (raw mbuffer throughput baseline)"
T1_TIMES=()
for i in $(seq 1 $RUNS); do
  echo "  Run $i..."
  # Use a subshell with time, capture real time
  TIMEFORMAT='real %3R  user %3U  sys %3S'
  elapsed=$( (time (dd if=/dev/zero bs=1M count=$DATA_COUNT 2>/dev/null | \
    mbuffer -q -m 256M -s 64k > /dev/null 2>/dev/null)) 2>&1 | grep real | awk '{print $2}')
  T1_TIMES+=("$elapsed")
  echo "    real ${elapsed}s"
done

echo ""

# ---- Test 2: braid loopback ----
echo "━━━ Test 2: /dev/zero | braid <-> braid > /dev/null ━━━"
echo "       (braid send+receive loopback)"
T2_TIMES=()
for i in $(seq 1 $RUNS); do
  echo "  Run $i..."
  
  # Start receiver
  $BRAID receive --bind 127.0.0.1:9000 --buffer-size $BUFFER_SIZE --output /dev/null -q &
  RECV_PID=$!
  # Wait for receiver to bind (control server needs to be listening)
  sleep 2
  
  TIMEFORMAT='real %3R  user %3U  sys %3S'
  elapsed=$( (time (dd if=/dev/zero bs=1M count=$DATA_COUNT 2>/dev/null | \
    $BRAID send --destination 127.0.0.1:9000 --channels $CHANNELS --mtu $MTU -q)) 2>&1 | grep real | awk '{print $2}')
  
  # Wait for receiver to finish processing
  wait $RECV_PID 2>/dev/null || true
  sleep 1
  
  T2_TIMES+=("$elapsed")
  echo "    real ${elapsed}s"
done

echo ""

# ---- Test 3: full pipeline with mbuffer ----
echo "━━━ Test 3: /dev/zero | mbuffer | braid <-> braid | mbuffer > /dev/null ━━━"
echo "       (mbuffer buffered braid pipeline)"
T3_TIMES=()
for i in $(seq 1 $RUNS); do
  echo "  Run $i..."
  
  # Start receiver (output to stdout, piped through mbuffer to /dev/null)
  $BRAID receive --bind 127.0.0.1:9001 --buffer-size $BUFFER_SIZE -q | \
    mbuffer -q -m 256M -s 64k > /dev/null 2>/dev/null &
  RECV_PID=$!
  sleep 2
  
  TIMEFORMAT='real %3R  user %3U  sys %3S'
  elapsed=$( (time (dd if=/dev/zero bs=1M count=$DATA_COUNT 2>/dev/null | \
    mbuffer -q -m 256M -s 64k 2>/dev/null | \
    $BRAID send --destination 127.0.0.1:9001 --channels $CHANNELS --mtu $MTU -q)) 2>&1 | grep real | awk '{print $2}')
  
  wait $RECV_PID 2>/dev/null || true
  sleep 1
  
  T3_TIMES+=("$elapsed")
  echo "    real ${elapsed}s"
done

echo ""
echo "=============================================="
echo " RESULTS SUMMARY"
echo "=============================================="

# Compute stats for each test
compute_stats() {
  local name="$1"
  shift
  local times=("$@")
  local count=${#times[@]}
  
  local min=999999 max=0 sum=0
  for t in "${times[@]}"; do
    sum=$(echo "$sum + $t" | bc -l)
    if (( $(echo "$t < $min" | bc -l) )); then min=$t; fi
    if (( $(echo "$t > $max" | bc -l) )); then max=$t; fi
  done
  local avg=$(echo "scale=3; $sum / $count" | bc -l)
  local mbps=$(echo "scale=1; $DATA_COUNT / $avg" | bc -l)
  local gbps=$(echo "scale=2; $mbps * 8 / 1000" | bc -l)
  
  echo ""
  echo "$name"
  echo "  Runs:      $count ($(printf '%s, ' "${times[@]}" | sed 's/, $//')s)"
  echo "  Min time:  ${min}s"
  echo "  Max time:  ${max}s"
  echo "  Avg time:  ${avg}s"
  echo "  Throughput: ${mbps} MB/s (${gbps} Gbps)"
}

compute_stats "Test 1: mbuffer baseline" "${T1_TIMES[@]}"
compute_stats "Test 2: braid loopback" "${T2_TIMES[@]}"
compute_stats "Test 3: braid + mbuffer pipeline" "${T3_TIMES[@]}"

echo ""
echo "=============================================="