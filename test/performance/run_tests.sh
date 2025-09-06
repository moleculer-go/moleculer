#!/bin/bash

# Performance and Memory Testing Script for Moleculer-Go
# This script runs comprehensive performance and memory tests

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test configuration
TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$TEST_DIR/../.." && pwd)"
OUTPUT_DIR="$TEST_DIR/results"
PROFILE_DIR="$TEST_DIR/profiles"

# Transporter configuration
TRANSPORTERS="${TRANSPORTERS:-memory,tcp,nats,redis,amqp,kafka}"
ENABLE_MULTI_TRANSPORTER="${ENABLE_MULTI_TRANSPORTER:-true}"

# Create output directories
mkdir -p "$OUTPUT_DIR"
mkdir -p "$PROFILE_DIR"

echo -e "${BLUE}Moleculer-Go Performance and Memory Testing${NC}"
echo "=============================================="
echo "Test Directory: $TEST_DIR"
echo "Project Root: $PROJECT_ROOT"
echo "Output Directory: $OUTPUT_DIR"
echo "Profile Directory: $PROFILE_DIR"
echo ""

# Function to run tests with profiling
run_test_with_profiling() {
    local test_name="$1"
    local test_function="$2"
    local duration="${3:-30s}"
    
    echo -e "${YELLOW}Running $test_name...${NC}"
    
    # Set profiling environment variables
    export GOGC=100
    export GOMEMLIMIT=1GiB
    
    # Run test with memory profiling
    cd "$PROJECT_ROOT"
    
    # Run the specific test
    go test -v -run "$test_function" -timeout "$duration" \
        -memprofile="$PROFILE_DIR/${test_name}_memory.prof" \
        -cpuprofile="$PROFILE_DIR/${test_name}_cpu.prof" \
        -blockprofile="$PROFILE_DIR/${test_name}_block.prof" \
        -mutexprofile="$PROFILE_DIR/${test_name}_mutex.prof" \
        ./test/performance/... > "$OUTPUT_DIR/${test_name}_output.log" 2>&1
    
    local exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        echo -e "${GREEN}✓ $test_name completed successfully${NC}"
    else
        echo -e "${RED}✗ $test_name failed with exit code $exit_code${NC}"
        echo "Check $OUTPUT_DIR/${test_name}_output.log for details"
    fi
    
    echo ""
    return $exit_code
}

# Function to run tests with specific transporters
run_test_with_transporters() {
    local test_name="$1"
    local test_function="$2"
    local duration="${3:-30s}"
    local transporters="$4"
    
    echo -e "${YELLOW}Running $test_name with transporters: $transporters...${NC}"
    
    # Set environment variables for transporter availability
    export GOGC=100
    export GOMEMLIMIT=1GiB
    
    # Set transporter availability based on input
    IFS=',' read -ra TRANSPORTER_ARRAY <<< "$transporters"
    for transporter in "${TRANSPORTER_ARRAY[@]}"; do
        case "$transporter" in
            "memory")
                # Memory is always available
                ;;
            "tcp")
                # TCP is always available
                ;;
            "nats")
                export NATS_AVAILABLE=true
                ;;
            "redis")
                export REDIS_AVAILABLE=true
                ;;
            "amqp")
                export AMQP_AVAILABLE=true
                ;;
            "kafka")
                export KAFKA_AVAILABLE=true
                ;;
        esac
    done
    
    # Run test with memory profiling
    cd "$PROJECT_ROOT"
    
    # Run the specific test
    go test -v -run "$test_function" -timeout "$duration" \
        -memprofile="$PROFILE_DIR/${test_name}_memory.prof" \
        -cpuprofile="$PROFILE_DIR/${test_name}_cpu.prof" \
        -blockprofile="$PROFILE_DIR/${test_name}_block.prof" \
        -mutexprofile="$PROFILE_DIR/${test_name}_mutex.prof" \
        ./test/performance/... > "$OUTPUT_DIR/${test_name}_output.log" 2>&1
    
    local exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        echo -e "${GREEN}✓ $test_name completed successfully${NC}"
    else
        echo -e "${RED}✗ $test_name failed with exit code $exit_code${NC}"
        echo "Check $OUTPUT_DIR/${test_name}_output.log for details"
    fi
    
    echo ""
    return $exit_code
}

# Function to run benchmark tests
run_benchmark() {
    local benchmark_name="$1"
    local benchmark_function="$2"
    local duration="${3:-30s}"
    
    echo -e "${YELLOW}Running benchmark $benchmark_name...${NC}"
    
    cd "$PROJECT_ROOT"
    
    # Run benchmark
    go test -bench="$benchmark_function" -benchmem -benchtime="$duration" \
        -memprofile="$PROFILE_DIR/${benchmark_name}_memory.prof" \
        -cpuprofile="$PROFILE_DIR/${benchmark_name}_cpu.prof" \
        ./test/performance/... > "$OUTPUT_DIR/${benchmark_name}_benchmark.log" 2>&1
    
    local exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        echo -e "${GREEN}✓ $benchmark_name completed successfully${NC}"
    else
        echo -e "${RED}✗ $benchmark_name failed with exit code $exit_code${NC}"
    fi
    
    echo ""
    return $exit_code
}

# Function to analyze profiles
analyze_profiles() {
    echo -e "${BLUE}Analyzing profiles...${NC}"
    
    cd "$PROJECT_ROOT"
    
    for profile in "$PROFILE_DIR"/*.prof; do
        if [ -f "$profile" ]; then
            profile_name=$(basename "$profile" .prof)
            echo "Analyzing $profile_name..."
            
            # Generate text report
            go tool pprof -text "$profile" > "$OUTPUT_DIR/${profile_name}_analysis.txt" 2>&1
            
            # Generate top report
            go tool pprof -top "$profile" > "$OUTPUT_DIR/${profile_name}_top.txt" 2>&1
            
            # Generate memory allocation report (for memory profiles)
            if [[ "$profile_name" == *"memory"* ]]; then
                go tool pprof -alloc_space -text "$profile" > "$OUTPUT_DIR/${profile_name}_alloc.txt" 2>&1
            fi
        fi
    done
    
    echo -e "${GREEN}✓ Profile analysis completed${NC}"
    echo ""
}

# Function to generate summary report
generate_summary() {
    echo -e "${BLUE}Generating summary report...${NC}"
    
    local summary_file="$OUTPUT_DIR/test_summary.md"
    
    cat > "$summary_file" << EOF
# Moleculer-Go Performance Test Summary

Generated: $(date)

## Test Results

EOF
    
    # Add test results
    for log_file in "$OUTPUT_DIR"/*_output.log; do
        if [ -f "$log_file" ]; then
            test_name=$(basename "$log_file" _output.log)
            echo "### $test_name" >> "$summary_file"
            echo "" >> "$summary_file"
            echo "**Status:** $(grep -q "PASS" "$log_file" && echo "PASS" || echo "FAIL")" >> "$summary_file"
            echo "" >> "$summary_file"
            echo "**Key Metrics:**" >> "$summary_file"
            grep -E "(Memory|Throughput|Latency|Error|Goroutine)" "$log_file" | head -10 >> "$summary_file"
            echo "" >> "$summary_file"
        fi
    done
    
    # Add benchmark results
    echo "## Benchmark Results" >> "$summary_file"
    echo "" >> "$summary_file"
    
    for bench_file in "$OUTPUT_DIR"/*_benchmark.log; do
        if [ -f "$bench_file" ]; then
            bench_name=$(basename "$bench_file" _benchmark.log)
            echo "### $bench_name" >> "$summary_file"
            echo "" >> "$summary_file"
            echo "\`\`\`" >> "$summary_file"
            cat "$bench_file" >> "$summary_file"
            echo "\`\`\`" >> "$summary_file"
            echo "" >> "$summary_file"
        fi
    done
    
    echo -e "${GREEN}✓ Summary report generated: $summary_file${NC}"
    echo ""
}

# Main execution
main() {
    echo -e "${BLUE}Starting performance and memory tests...${NC}"
    echo ""
    
    # Clean previous results
    rm -rf "$OUTPUT_DIR"/*.log
    rm -rf "$PROFILE_DIR"/*.prof
    
    local failed_tests=0
    
    # Run memory leak tests
    echo -e "${BLUE}=== Memory Leak Tests ===${NC}"
    run_test_with_profiling "MemoryLeakDetection" "TestMemoryLeakDetection" "60s" || ((failed_tests++))
    run_test_with_profiling "ServiceCatalogMemoryLeak" "TestServiceCatalogMemoryLeak" "30s" || ((failed_tests++))
    run_test_with_profiling "ConnectionBufferLeak" "TestConnectionBufferLeak" "30s" || ((failed_tests++))
    run_test_with_profiling "GoroutineLeakDetection" "TestGoroutineLeakDetection" "30s" || ((failed_tests++))
    
    # Run performance tests
    echo -e "${BLUE}=== Performance Tests ===${NC}"
    run_test_with_profiling "ConcurrentPerformance" "TestConcurrentPerformance" "30s" || ((failed_tests++))
    run_test_with_profiling "ThroughputUnderLoad" "TestThroughputUnderLoad" "30s" || ((failed_tests++))
    run_test_with_profiling "LatencyDistribution" "TestLatencyDistribution" "30s" || ((failed_tests++))
    
    # Run multi-broker tests
    echo -e "${BLUE}=== Multi-Broker Tests ===${NC}"
    run_test_with_profiling "MultiBrokerStress" "TestMultiBrokerStress" "60s" || ((failed_tests++))
    run_test_with_profiling "MultiBrokerMemoryLeak" "TestMultiBrokerMemoryLeak" "60s" || ((failed_tests++))
    run_test_with_profiling "MultiBrokerConcurrentConnections" "TestMultiBrokerConcurrentConnections" "30s" || ((failed_tests++))
    run_test_with_profiling "MultiBrokerServiceDiscovery" "TestMultiBrokerServiceDiscovery" "30s" || ((failed_tests++))
    run_test_with_profiling "MultiBrokerEventBroadcast" "TestMultiBrokerEventBroadcast" "30s" || ((failed_tests++))
    run_test_with_profiling "MultiBrokerResourceCleanup" "TestMultiBrokerResourceCleanup" "30s" || ((failed_tests++))
    
    # Run multi-transporter tests if enabled
    if [ "$ENABLE_MULTI_TRANSPORTER" = "true" ]; then
        echo -e "${BLUE}=== Multi-Transporter Tests ===${NC}"
        run_test_with_transporters "AllTransportersMemoryLeak" "TestAllTransportersMemoryLeak" "60s" "$TRANSPORTERS" || ((failed_tests++))
        run_test_with_transporters "AllTransportersPerformance" "TestAllTransportersPerformance" "30s" "$TRANSPORTERS" || ((failed_tests++))
        run_test_with_transporters "AllTransportersConcurrency" "TestAllTransportersConcurrency" "30s" "$TRANSPORTERS" || ((failed_tests++))
        run_test_with_transporters "TransporterComparison" "TestTransporterComparison" "30s" "$TRANSPORTERS" || ((failed_tests++))
        run_test_with_transporters "TransporterStress" "TestTransporterStress" "60s" "$TRANSPORTERS" || ((failed_tests++))
    fi
    
    # Run stress tests
    echo -e "${BLUE}=== Stress Tests ===${NC}"
    run_test_with_profiling "MemoryLeakStress" "TestMemoryLeakStress" "60s" || ((failed_tests++))
    run_test_with_profiling "ServiceCatalogMemoryLeakStress" "TestServiceCatalogMemoryLeak" "30s" || ((failed_tests++))
    run_test_with_profiling "ConnectionBufferLeakStress" "TestConnectionBufferLeak" "30s" || ((failed_tests++))
    run_test_with_profiling "HighFrequencyOperations" "TestHighFrequencyOperations" "30s" || ((failed_tests++))
    run_test_with_profiling "ResourceExhaustion" "TestResourceExhaustion" "30s" || ((failed_tests++))
    
    # Run benchmarks
    echo -e "${BLUE}=== Benchmark Tests ===${NC}"
    run_benchmark "ActionLookup" "BenchmarkActionLookup" "30s" || ((failed_tests++))
    run_benchmark "ServiceCall" "BenchmarkServiceCall" "30s" || ((failed_tests++))
    run_benchmark "EventEmit" "BenchmarkEventEmit" "30s" || ((failed_tests++))
    run_benchmark "ServiceRegistration" "BenchmarkServiceRegistration" "30s" || ((failed_tests++))
    run_benchmark "MemoryAllocation" "BenchmarkMemoryAllocation" "30s" || ((failed_tests++))
    run_benchmark "MultiBrokerPerformance" "BenchmarkMultiBrokerPerformance" "30s" || ((failed_tests++))
    run_benchmark "StressTest" "BenchmarkStressTest" "30s" || ((failed_tests++))
    
    # Analyze profiles
    analyze_profiles
    
    # Generate summary
    generate_summary
    
    # Final results
    echo -e "${BLUE}=== Test Summary ===${NC}"
    echo "Total failed tests: $failed_tests"
    echo "Results directory: $OUTPUT_DIR"
    echo "Profiles directory: $PROFILE_DIR"
    echo ""
    
    if [ $failed_tests -eq 0 ]; then
        echo -e "${GREEN}✓ All tests passed!${NC}"
        exit 0
    else
        echo -e "${RED}✗ $failed_tests tests failed${NC}"
        exit 1
    fi
}

# Run main function
main "$@"
