# Performance Testing Framework

This directory contains a comprehensive performance and memory profiling testing framework for the moleculer-go project.

## Overview

The performance testing framework provides tools to:
- Measure memory usage and detect memory leaks
- Profile CPU and blocking operations
- Test system behavior under high load
- Compare performance across different transporters
- Monitor goroutine usage and detect goroutine leaks
- Stress test multi-broker scenarios

## Files

### Core Framework
- `transporter_factory.go` - Centralized transporter creation and configuration
- `memory_profiling_test.go` - Memory profiling and leak detection tests
- `performance_benchmark_test.go` - Performance benchmarking tests
- `multi_broker_test.go` - Multi-broker stress testing
- `stress_test.go` - General stress testing and resource exhaustion tests
- `multi_transporter_test.go` - Cross-transporter testing

### Demo and Examples
- `simple_test.go` - Basic functionality tests
- `demo_test.go` - Comprehensive demonstration of framework capabilities

### Utilities
- `run_tests.sh` - Automated test runner with profiling support
- `README.md` - This documentation

## Quick Start

### Running Basic Tests
```bash
# Run all performance tests
go test ./test/performance/ -v

# Run specific test categories
go test ./test/performance/ -run TestBasic -v
go test ./test/performance/ -run TestMemory -v
go test ./test/performance/ -run TestStress -v
```

### Running with Profiling
```bash
# Run with memory profiling
go test ./test/performance/ -memprofile=mem.prof -v

# Run with CPU profiling
go test ./test/performance/ -cpuprofile=cpu.prof -v

# Run with blocking profiling
go test ./test/performance/ -blockprofile=block.prof -v
```

### Using the Test Runner Script
```bash
# Make script executable
chmod +x run_tests.sh

# Run all tests with profiling
./run_tests.sh

# Run tests with specific transporter
ENABLE_MULTI_TRANSPORTER=true TRANSPORTERS=memory ./run_tests.sh
```

## Framework Components

### MemoryProfiler
The `MemoryProfiler` struct provides memory monitoring capabilities:

```go
profiler := NewMemoryProfiler()
profiler.Measure(0) // Initial measurement
// ... perform operations ...
profiler.Measure(1) // Final measurement
stats := profiler.GetStats()
```

### TransporterFactory
The `TransporterFactory` enables easy switching between different transporters:

```go
// Create configuration for Memory transporter
config := CreateTestConfig(TransporterMemory, "ERROR")

// Create configuration for TCP transporter
config := CreateTestConfig(TransporterTCP, "ERROR")

// Create configuration for NATS transporter
config := CreateTestConfig(TransporterNATS, "ERROR")
```

### Multi-Broker Testing
The framework supports testing multiple brokers in a cluster:

```go
cluster := NewBrokerCluster(5) // 5 brokers
cluster.Start()
defer cluster.Stop()

// Run stress tests across the cluster
cluster.RunStressTest(1000) // 1000 operations
```

## Test Categories

### 1. Memory Profiling Tests
- `TestMemoryLeakDetection` - Detects memory leaks in service registration
- `TestConnectionBufferLeakMemory` - Tests for connection buffer leaks
- `TestServiceCatalogMemoryGrowth` - Monitors ServiceCatalog memory usage
- `TestGoroutineLeakDetection` - Detects goroutine leaks

### 2. Performance Benchmark Tests
- `BenchmarkActionCall` - Benchmarks action call performance
- `BenchmarkEventEmit` - Benchmarks event emission performance
- `BenchmarkServiceDiscovery` - Benchmarks service discovery
- `BenchmarkConcurrentCalls` - Tests concurrent call performance

### 3. Multi-Broker Tests
- `TestMultiBrokerStress` - Stress tests with multiple brokers
- `TestMultiBrokerMemoryLeak` - Memory leak detection in multi-broker scenarios
- `TestMultiBrokerConcurrentConnections` - Concurrent connection testing
- `TestMultiBrokerServiceDiscovery` - Service discovery across brokers

### 4. Stress Tests
- `TestMemoryLeakStress` - High-load memory leak testing
- `TestResourceExhaustion` - Resource exhaustion testing
- `TestLongRunningStability` - Long-running stability tests

## Configuration

### Environment Variables
- `TRANSPORTERS` - Comma-separated list of transporters to test
- `ENABLE_MULTI_TRANSPORTER` - Enable multi-transporter testing
- `LOG_LEVEL` - Logging level (DEBUG, INFO, WARN, ERROR)

### Transporter Types
- `TransporterMemory` - In-memory transporter
- `TransporterTCP` - TCP transporter
- `TransporterNATS` - NATS transporter
- `TransporterRedis` - Redis transporter
- `TransporterAMQP` - AMQP transporter
- `TransporterKafka` - Kafka transporter

## Example Usage

### Basic Memory Profiling
```go
func TestMyMemoryLeak(t *testing.T) {
    profiler := NewMemoryProfiler()
    profiler.Measure(0)
    
    // Perform operations that might leak memory
    for i := 0; i < 1000; i++ {
        // ... operations ...
    }
    
    profiler.Measure(1)
    stats := profiler.GetStats()
    
    if stats["heap_growth"].(uint64) > 10*1024*1024 { // 10MB
        t.Error("Potential memory leak detected")
    }
}
```

### Multi-Broker Stress Testing
```go
func TestMyStressScenario(t *testing.T) {
    cluster := NewBrokerCluster(3)
    cluster.Start()
    defer cluster.Stop()
    
    // Register services
    for i := 0; i < 3; i++ {
        broker := cluster.GetBroker(i)
        broker.Publish(createTestService(i))
    }
    
    // Run stress test
    cluster.RunStressTest(10000)
}
```

### Transporter Comparison
```go
func TestTransporterPerformance(t *testing.T) {
    transporters := []TransporterType{
        TransporterMemory,
        TransporterTCP,
        TransporterNATS,
    }
    
    for _, transporter := range transporters {
        config := CreateTestConfig(transporter, "ERROR")
        broker := broker.New(config)
        
        start := time.Now()
        broker.Start()
        defer broker.Stop()
        
        duration := time.Since(start)
        t.Logf("%s startup time: %v", transporter, duration)
    }
}
```

## Profiling Output

The framework generates detailed profiling information:

```
=== Performance Testing Framework Demo ===
1. Testing Memory Profiler...
   Memory growth: 208 bytes
2. Testing Transporter Factory...
   ✓ Transporter factory created successfully
3. Testing Broker Creation...
   ✓ Service call successful
4. Testing Event Emission...
   ✓ Event emitted successfully
5. Testing Memory Profiling with Broker...
   Final memory growth: 359560 bytes
=== Demo completed successfully ===
```

## Best Practices

1. **Always measure baseline**: Take initial measurements before operations
2. **Use appropriate thresholds**: Set realistic memory growth limits
3. **Test with realistic load**: Use production-like scenarios
4. **Monitor goroutines**: Check for goroutine leaks
5. **Profile regularly**: Run profiling tests in CI/CD
6. **Compare transporters**: Test across different transporter types
7. **Clean up resources**: Always stop brokers and clean up

## Troubleshooting

### Common Issues
1. **"Broker must be started"**: Ensure brokers are started before making calls
2. **"endpoint not found"**: Services may not be registered across brokers
3. **Memory leaks**: Check for unbounded slice/map growth
4. **Goroutine leaks**: Monitor goroutine count over time

### Debug Tips
1. Use `-v` flag for verbose output
2. Check logs for error messages
3. Use memory profiler to identify leak sources
4. Test with smaller loads first
5. Verify transporter configuration

## Contributing

When adding new tests:
1. Follow the existing naming conventions
2. Include proper cleanup (defer statements)
3. Add appropriate error handling
4. Document test purpose and expected behavior
5. Use the framework components (MemoryProfiler, TransporterFactory)
6. Add to appropriate test categories

## Future Enhancements

- [ ] Real-time monitoring dashboard
- [ ] Automated performance regression detection
- [ ] Integration with CI/CD pipelines
- [ ] Custom metrics collection
- [ ] Load testing with external tools
- [ ] Performance comparison reports