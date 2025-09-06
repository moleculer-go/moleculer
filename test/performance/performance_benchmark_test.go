package performance

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
)

// PerformanceMetrics tracks performance measurements
type PerformanceMetrics struct {
	ActionLookupTime    time.Duration
	ServiceCallTime     time.Duration
	EventEmitTime       time.Duration
	ServiceRegTime      time.Duration
	ConcurrentCalls     int
	ThroughputPerSecond float64
	ErrorRate           float64
	MemoryStats         MemoryStats
}


// BenchmarkActionLookup benchmarks action lookup performance
func BenchmarkActionLookup(b *testing.B) {
	// Measure initial memory
	memStats := measureMemory()
	
	config := CreateTestConfig(TransporterMemory, "ERROR")
	bkr := broker.New(config)

	bkr.Start()
	defer bkr.Stop()

	// Add services with many actions to test lookup performance
	serviceCount := 100
	actionsPerService := 50

	for i := 0; i < serviceCount; i++ {
		actions := make([]moleculer.Action, actionsPerService)
		for j := 0; j < actionsPerService; j++ {
			actions[j] = moleculer.Action{
				Name: fmt.Sprintf("action-%d", j),
				Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
					return map[string]interface{}{"result": "ok"}
				},
			}
		}

		bkr.Publish(moleculer.ServiceSchema{
			Name:    fmt.Sprintf("service-%d", i),
			Actions: actions,
		})
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		serviceName := fmt.Sprintf("service-%d", i%serviceCount)
		actionName := fmt.Sprintf("action-%d", i%actionsPerService)

		// This tests the action lookup performance
		result := <-bkr.Call(serviceName+"."+actionName, map[string]interface{}{"test": i})
		if result.IsError() {
			b.Fatalf("Action call failed: %v", result.Error())
		}
	}
	
	// Update final memory stats
	memStats.update(1) // 1 broker
	
	// Log memory statistics
	b.Logf("Memory stats - Initial: %d bytes, Peak: %d bytes, Final: %d bytes, Growth: %d bytes, Goroutines: %d",
		memStats.InitialHeap, memStats.PeakHeap, memStats.FinalHeap, memStats.HeapGrowth, memStats.GoroutineCount)
}

// BenchmarkServiceCall benchmarks service call performance
func BenchmarkServiceCall(b *testing.B) {
	config := CreateTestConfig(TransporterMemory, "ERROR")
	bkr := broker.New(config)

	bkr.Start()
	defer bkr.Stop()

	// Add a simple service
	bkr.Publish(moleculer.ServiceSchema{
		Name: "benchmark-service",
		Actions: []moleculer.Action{
			{
				Name: "test",
				Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
					return map[string]interface{}{"result": "ok", "params": params.Value()}
				},
			},
		},
	})

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result := <-bkr.Call("benchmark-service.test", map[string]interface{}{"iteration": i})
		if result.IsError() {
			b.Fatalf("Service call failed: %v", result.Error())
		}
	}
}

// BenchmarkEventEmit benchmarks event emission performance
func BenchmarkEventEmit(b *testing.B) {
	config := CreateTestConfig(TransporterMemory, "ERROR")
	bkr := broker.New(config)

	bkr.Start()
	defer bkr.Stop()

	// Add a service with event handlers
	bkr.Publish(moleculer.ServiceSchema{
		Name: "event-service",
		Events: []moleculer.Event{
			{
				Name: "test.event",
				Handler: func(ctx moleculer.Context, params moleculer.Payload) {
					// Event handler
				},
			},
		},
	})

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		bkr.Emit("test.event", map[string]interface{}{"iteration": i})
	}
}

// BenchmarkServiceRegistration benchmarks service registration performance
func BenchmarkServiceRegistration(b *testing.B) {
	config := CreateTestConfig(TransporterMemory, "ERROR")
	bkr := broker.New(config)

	bkr.Start()
	defer bkr.Stop()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		bkr.Publish(moleculer.ServiceSchema{
			Name: fmt.Sprintf("service-%d", i),
			Actions: []moleculer.Action{
				{
					Name: "test",
					Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
						return map[string]interface{}{"result": "ok"}
					},
				},
			},
		})
	}
}

// TestConcurrentPerformance tests performance under concurrent load
func TestConcurrentPerformance(t *testing.T) {
	config := CreateTestConfig(TransporterMemory, "ERROR")
	bkr := broker.New(config)

	bkr.Start()
	defer bkr.Stop()

	// Add a service
	bkr.Publish(moleculer.ServiceSchema{
		Name: "concurrent-service",
		Actions: []moleculer.Action{
			{
				Name: "test",
				Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
					// Simulate some work
					time.Sleep(1 * time.Millisecond)
					return map[string]interface{}{"result": "ok", "params": params.Value()}
				},
			},
		},
	})

	// Test different levels of concurrency
	concurrencyLevels := []int{1, 10, 50, 100, 200}

	for _, concurrency := range concurrencyLevels {
		t.Run(fmt.Sprintf("Concurrency_%d", concurrency), func(t *testing.T) {
			metrics := testConcurrentCalls(bkr, concurrency, 1000)
			t.Logf("Concurrency %d: %+v", concurrency, metrics)

			// Check that error rate is acceptable
			if metrics.ErrorRate > 0.01 { // 1% error rate
				t.Errorf("Error rate too high: %.2f%%", metrics.ErrorRate*100)
			}
		})
	}
}

// testConcurrentCalls tests concurrent service calls
func testConcurrentCalls(bkr *broker.ServiceBroker, concurrency, totalCalls int) PerformanceMetrics {
	var wg sync.WaitGroup
	var mu sync.Mutex

	var totalTime time.Duration
	var errorCount int
	var callTimes []time.Duration

	startTime := time.Now()

	// Create worker goroutines
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			callsPerWorker := totalCalls / concurrency
			if workerID < totalCalls%concurrency {
				callsPerWorker++
			}

			for j := 0; j < callsPerWorker; j++ {
				callStart := time.Now()
				result := <-bkr.Call("concurrent-service.test", map[string]interface{}{
					"worker": workerID,
					"call":   j,
				})
				callDuration := time.Since(callStart)

				mu.Lock()
				callTimes = append(callTimes, callDuration)
				totalTime += callDuration
				if result.IsError() {
					errorCount++
				}
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	totalDuration := time.Since(startTime)

	// Calculate metrics
	avgCallTime := totalTime / time.Duration(totalCalls)
	throughput := float64(totalCalls) / totalDuration.Seconds()
	errorRate := float64(errorCount) / float64(totalCalls)

	// Calculate percentiles
	if len(callTimes) > 0 {
		// Simple percentile calculation (would need proper sorting for accurate results)
		_ = callTimes[len(callTimes)/2]                  // p50
		_ = callTimes[int(float64(len(callTimes))*0.95)] // p95
		_ = callTimes[int(float64(len(callTimes))*0.99)] // p99
	}

	return PerformanceMetrics{
		ActionLookupTime:    avgCallTime,
		ServiceCallTime:     avgCallTime,
		ConcurrentCalls:     concurrency,
		ThroughputPerSecond: throughput,
		ErrorRate:           errorRate,
	}
}

// TestThroughputUnderLoad tests throughput under sustained load
func TestThroughputUnderLoad(t *testing.T) {
	config := CreateTestConfig(TransporterMemory, "ERROR")
	bkr := broker.New(config)

	bkr.Start()
	defer bkr.Stop()

	// Add multiple services
	serviceCount := 10
	for i := 0; i < serviceCount; i++ {
		bkr.Publish(moleculer.ServiceSchema{
			Name: fmt.Sprintf("throughput-service-%d", i),
			Actions: []moleculer.Action{
				{
					Name: "test",
					Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
						return map[string]interface{}{"result": "ok", "service": i}
					},
				},
			},
		})
	}

	// Test different load levels
	loadLevels := []int{100, 500, 1000, 2000}

	for _, load := range loadLevels {
		t.Run(fmt.Sprintf("Load_%d", load), func(t *testing.T) {
			startTime := time.Now()
			var wg sync.WaitGroup
			var callCount int64
			var errorCount int64

			// Run load test for 5 seconds
			duration := 5 * time.Second
			stopTime := startTime.Add(duration)

			// Start workers
			for i := 0; i < load; i++ {
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()

					for time.Now().Before(stopTime) {
						serviceName := fmt.Sprintf("throughput-service-%d", workerID%serviceCount)
						result := <-bkr.Call(serviceName+".test", map[string]interface{}{
							"worker": workerID,
							"time":   time.Now().UnixNano(),
						})

						if result.IsError() {
							errorCount++
						}
						callCount++
					}
				}(i)
			}

			wg.Wait()

			actualDuration := time.Since(startTime)
			throughput := float64(callCount) / actualDuration.Seconds()
			errorRate := float64(errorCount) / float64(callCount)

			t.Logf("Load %d: %d calls in %v (%.2f calls/sec, %.2f%% errors)",
				load, callCount, actualDuration, throughput, errorRate*100)

			// Check that throughput is reasonable
			if throughput < 100 {
				t.Errorf("Throughput too low: %.2f calls/sec", throughput)
			}

			// Check that error rate is acceptable
			if errorRate > 0.05 { // 5% error rate
				t.Errorf("Error rate too high: %.2f%%", errorRate*100)
			}
		})
	}
}

// TestLatencyDistribution tests latency distribution under load
func TestLatencyDistribution(t *testing.T) {
	config := CreateTestConfig(TransporterMemory, "ERROR")
	bkr := broker.New(config)

	bkr.Start()
	defer bkr.Stop()

	// Add a service with configurable delay
	bkr.Publish(moleculer.ServiceSchema{
		Name: "latency-service",
		Actions: []moleculer.Action{
			{
				Name: "test",
				Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
					// Simulate variable processing time
					delay := params.Get("delay").Int()
					if delay > 0 {
						time.Sleep(time.Duration(delay) * time.Millisecond)
					}
					return map[string]interface{}{"result": "ok", "delay": delay}
				},
			},
		},
	})

	// Test with different delay patterns
	delayPatterns := []struct {
		name   string
		delays []int
	}{
		{"NoDelay", []int{0}},
		{"LowDelay", []int{1, 2, 3}},
		{"MediumDelay", []int{5, 10, 15}},
		{"HighDelay", []int{50, 100, 200}},
	}

	for _, pattern := range delayPatterns {
		t.Run(pattern.name, func(t *testing.T) {
			var latencies []time.Duration
			var mu sync.Mutex

			// Make calls with different delays
			for i := 0; i < 100; i++ {
				delay := pattern.delays[i%len(pattern.delays)]

				start := time.Now()
				result := <-bkr.Call("latency-service.test", map[string]interface{}{"delay": delay})
				latency := time.Since(start)

				if result.IsError() {
					t.Errorf("Call failed: %v", result.Error())
					continue
				}

				mu.Lock()
				latencies = append(latencies, latency)
				mu.Unlock()
			}

			// Calculate latency statistics
			if len(latencies) > 0 {
				var total time.Duration
				for _, l := range latencies {
					total += l
				}
				avgLatency := total / time.Duration(len(latencies))

				// Find min/max
				minLatency := latencies[0]
				maxLatency := latencies[0]
				for _, l := range latencies {
					if l < minLatency {
						minLatency = l
					}
					if l > maxLatency {
						maxLatency = l
					}
				}

				t.Logf("Latency stats: avg=%v, min=%v, max=%v", avgLatency, minLatency, maxLatency)

				// Check that average latency is reasonable
				if avgLatency > 1*time.Second {
					t.Errorf("Average latency too high: %v", avgLatency)
				}
			}
		})
	}
}

// BenchmarkMemoryAllocationBenchmark benchmarks memory allocation during operations
func BenchmarkMemoryAllocationBenchmark(b *testing.B) {
	config := CreateTestConfig(TransporterMemory, "ERROR")
	bkr := broker.New(config)

	bkr.Start()
	defer bkr.Stop()

	// Add a service
	bkr.Publish(moleculer.ServiceSchema{
		Name: "memory-service",
		Actions: []moleculer.Action{
			{
				Name: "test",
				Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
					// Create some data to test memory allocation
					data := make([]byte, 1024)
					for i := range data {
						data[i] = byte(i % 256)
					}
					return map[string]interface{}{
						"result": "ok",
						"data":   data,
						"size":   len(data),
					}
				},
			},
		},
	})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result := <-bkr.Call("memory-service.test", map[string]interface{}{"iteration": i})
		if result.IsError() {
			b.Fatalf("Service call failed: %v", result.Error())
		}
	}
}

// TestPerformanceRegression tests for performance regressions
func TestPerformanceRegression(t *testing.T) {
	// This test establishes baseline performance metrics
	// and can be used to detect regressions in future changes

	config := CreateTestConfig(TransporterMemory, "ERROR")
	bkr := broker.New(config)

	bkr.Start()
	defer bkr.Stop()

	// Add services
	for i := 0; i < 10; i++ {
		bkr.Publish(moleculer.ServiceSchema{
			Name: fmt.Sprintf("regression-service-%d", i),
			Actions: []moleculer.Action{
				{
					Name: "test",
					Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
						return map[string]interface{}{"result": "ok", "service": i}
					},
				},
			},
		})
	}

	// Measure baseline performance
	startTime := time.Now()
	callCount := 1000

	for i := 0; i < callCount; i++ {
		serviceName := fmt.Sprintf("regression-service-%d", i%10)
		result := <-bkr.Call(serviceName+".test", map[string]interface{}{"test": i})
		if result.IsError() {
			t.Errorf("Call failed: %v", result.Error())
		}
	}

	duration := time.Since(startTime)
	throughput := float64(callCount) / duration.Seconds()
	avgLatency := duration / time.Duration(callCount)

	t.Logf("Baseline performance: %.2f calls/sec, avg latency: %v", throughput, avgLatency)

	// Define performance thresholds
	minThroughput := 100.0 // calls per second
	maxAvgLatency := 100 * time.Millisecond

	if throughput < minThroughput {
		t.Errorf("Throughput below threshold: %.2f < %.2f calls/sec", throughput, minThroughput)
	}

	if avgLatency > maxAvgLatency {
		t.Errorf("Average latency above threshold: %v > %v", avgLatency, maxAvgLatency)
	}
}
