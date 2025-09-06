package performance

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
)

// TestAllTransportersMemoryLeak tests memory leaks across all available transporters
func TestAllTransportersMemoryLeak(t *testing.T) {
	transporterTypes := GetAvailableTransporters()
	suite := NewTransporterTestSuite(transporterTypes)

	if len(suite.GetTransporterTypes()) == 0 {
		t.Skip("No transporters available for testing")
	}

	// Create test result writer
	resultWriter := NewTestResultWriter("./test_results")
	defer resultWriter.SaveResults()

	results := suite.RunTestForAllTransporters(func(transporterType TransporterType, config *moleculer.Config) error {
		t.Logf("Testing memory leak for %s transporter", GetTransporterName(transporterType))

		// Initial memory measurement
		initialGoroutines := runtime.NumGoroutine()
		var initialMem runtime.MemStats
		runtime.ReadMemStats(&initialMem)

		// Create broker
		bkr := broker.New(config)
		bkr.Start()
		defer bkr.Stop()

		// Add test service
		bkr.Publish(moleculer.ServiceSchema{
			Name: "test-service",
			Actions: []moleculer.Action{
				{
					Name: "test",
					Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
						return map[string]interface{}{"result": "ok"}
					},
				},
			},
		})

		// Run operations that could cause memory leaks
		for i := 0; i < 100; i++ {
			// Make calls
			result := <-bkr.Call("test-service.test", map[string]interface{}{"iteration": i})
			if result.IsError() {
				t.Logf("Call error: %v", result.Error())
			}

			// Emit events
			bkr.Emit("test.event", map[string]interface{}{"iteration": i})

			// Force GC every 10 iterations
			if i%10 == 0 {
				runtime.GC()
			}
		}

		// Final measurements
		runtime.GC()
		time.Sleep(100 * time.Millisecond) // Allow cleanup

		finalGoroutines := runtime.NumGoroutine()
		var finalMem runtime.MemStats
		runtime.ReadMemStats(&finalMem)

		// Check for memory leaks - handle potential uint64 overflow
		var memoryGrowth int64
		if finalMem.HeapAlloc >= initialMem.HeapAlloc {
			memoryGrowth = int64(finalMem.HeapAlloc - initialMem.HeapAlloc)
		} else {
			// Handle underflow case
			memoryGrowth = -int64(initialMem.HeapAlloc - finalMem.HeapAlloc)
		}
		goroutineLeak := finalGoroutines - initialGoroutines

		t.Logf("%s transporter - Memory growth: %d bytes, Goroutine leak: %d",
			GetTransporterName(transporterType), memoryGrowth, goroutineLeak)

		// Check for excessive memory growth
		if memoryGrowth > 10*1024*1024 { // 10MB
			return fmt.Errorf("excessive memory growth: %d bytes", memoryGrowth)
		}

		// Check for goroutine leaks
		if goroutineLeak > 20 {
			return fmt.Errorf("goroutine leak: %d goroutines", goroutineLeak)
		}

		return nil
	})

	// Report results and create test results
	for transporterType, err := range results {
		transporterName := GetTransporterName(transporterType)
		success := err == nil

		if err != nil {
			t.Errorf("%s transporter failed: %v", transporterName, err)
		} else {
			t.Logf("✓ %s transporter passed", transporterName)
		}

		// Create test settings for transporter test
		testSettings := CreateTestSettings(
			transporterName,
			map[string]interface{}{
				"type": string(transporterType),
			},
			1,             // single broker
			1,             // one test service
			1,             // one test action
			0,             // no events
			5*time.Second, // test duration
			1,             // concurrency level
			10*1024*1024,  // memory threshold (10MB)
			20,            // goroutine threshold
		)
		testSettings.AddOtherSetting("test_type", "memory_leak")
		testSettings.AddOtherSetting("operations_count", 100)

		// Create test result
		result := CreateTestResult("TestAllTransportersMemoryLeak", transporterName, success, 0, err)
		result.AddTestSettings(testSettings)
		result.AddMetric("transporter_type", string(transporterType))
		result.AddMetric("test_success", success)

		resultWriter.AddResult(result)
	}
}

// TestAllTransportersPerformance tests performance across all available transporters
func TestAllTransportersPerformance(t *testing.T) {
	transporterTypes := GetAvailableTransporters()
	suite := NewTransporterTestSuite(transporterTypes)

	if len(suite.GetTransporterTypes()) == 0 {
		t.Skip("No transporters available for testing")
	}

	results := suite.RunTestForAllTransporters(func(transporterType TransporterType, config *moleculer.Config) error {
		t.Logf("Testing performance for %s transporter", GetTransporterName(transporterType))

		// Create broker
		bkr := broker.New(config)
		bkr.Start()
		defer bkr.Stop()

		// Add test service
		bkr.Publish(moleculer.ServiceSchema{
			Name: "perf-service",
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

		// Wait for service to be ready
		time.Sleep(100 * time.Millisecond)

		// Performance test
		callCount := 100
		startTime := time.Now()

		for i := 0; i < callCount; i++ {
			result := <-bkr.Call("perf-service.test", map[string]interface{}{"iteration": i})
			if result.IsError() {
				t.Logf("Call error: %v", result.Error())
			}
		}

		duration := time.Since(startTime)
		throughput := float64(callCount) / duration.Seconds()
		avgLatency := duration / time.Duration(callCount)

		t.Logf("%s transporter - Throughput: %.2f calls/sec, Avg latency: %v",
			GetTransporterName(transporterType), throughput, avgLatency)

		// Check for reasonable performance
		if throughput < 10 {
			return fmt.Errorf("throughput too low: %.2f calls/sec", throughput)
		}

		if avgLatency > 1*time.Second {
			return fmt.Errorf("average latency too high: %v", avgLatency)
		}

		return nil
	})

	// Report results
	for transporterType, err := range results {
		if err != nil {
			t.Errorf("%s transporter failed: %v", GetTransporterName(transporterType), err)
		} else {
			t.Logf("✓ %s transporter passed", GetTransporterName(transporterType))
		}
	}
}

// TestAllTransportersConcurrency tests concurrency across all available transporters
func TestAllTransportersConcurrency(t *testing.T) {
	transporterTypes := GetAvailableTransporters()
	suite := NewTransporterTestSuite(transporterTypes)

	if len(suite.GetTransporterTypes()) == 0 {
		t.Skip("No transporters available for testing")
	}

	results := suite.RunTestForAllTransporters(func(transporterType TransporterType, config *moleculer.Config) error {
		t.Logf("Testing concurrency for %s transporter", GetTransporterName(transporterType))

		// Create broker
		bkr := broker.New(config)
		bkr.Start()
		defer bkr.Stop()

		// Add test service
		bkr.Publish(moleculer.ServiceSchema{
			Name: "concurrent-service",
			Actions: []moleculer.Action{
				{
					Name: "test",
					Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
						// Simulate some work
						time.Sleep(5 * time.Millisecond)
						return map[string]interface{}{"result": "ok", "worker": params.Get("worker").Int()}
					},
				},
			},
		})

		// Wait for service to be ready
		time.Sleep(100 * time.Millisecond)

		// Concurrency test
		concurrency := 10
		callsPerWorker := 10
		var wg sync.WaitGroup
		var errorCount int
		var mu sync.Mutex

		startTime := time.Now()

		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()

				for j := 0; j < callsPerWorker; j++ {
					result := <-bkr.Call("concurrent-service.test", map[string]interface{}{
						"worker": workerID,
						"call":   j,
					})

					if result.IsError() {
						mu.Lock()
						errorCount++
						mu.Unlock()
					}
				}
			}(i)
		}

		wg.Wait()
		duration := time.Since(startTime)

		totalCalls := concurrency * callsPerWorker
		throughput := float64(totalCalls) / duration.Seconds()
		errorRate := float64(errorCount) / float64(totalCalls)

		t.Logf("%s transporter - Concurrency: %d, Throughput: %.2f calls/sec, Error rate: %.2f%%",
			GetTransporterName(transporterType), concurrency, throughput, errorRate*100)

		// Check for reasonable performance
		if throughput < 5 {
			return fmt.Errorf("throughput too low: %.2f calls/sec", throughput)
		}

		if errorRate > 0.1 { // 10% error rate
			return fmt.Errorf("error rate too high: %.2f%%", errorRate*100)
		}

		return nil
	})

	// Report results
	for transporterType, err := range results {
		if err != nil {
			t.Errorf("%s transporter failed: %v", GetTransporterName(transporterType), err)
		} else {
			t.Logf("✓ %s transporter passed", GetTransporterName(transporterType))
		}
	}
}

// TestTransporterComparison compares performance across different transporters
func TestTransporterComparison(t *testing.T) {
	transporterTypes := GetAvailableTransporters()
	suite := NewTransporterTestSuite(transporterTypes)

	if len(suite.GetTransporterTypes()) < 2 {
		t.Skip("Need at least 2 transporters for comparison")
	}

	// Performance metrics for each transporter
	type TransporterMetrics struct {
		TransporterType TransporterType
		Throughput      float64
		AvgLatency      time.Duration
		MemoryUsage     uint64
		ErrorRate       float64
	}

	metrics := make([]TransporterMetrics, 0, len(suite.GetTransporterTypes()))

	// Test each transporter
	for i, transporterType := range suite.GetTransporterTypes() {
		config := suite.GetConfigs()[i]

		t.Logf("Testing %s transporter for comparison", GetTransporterName(transporterType))

		// Initial memory measurement
		var initialMem runtime.MemStats
		runtime.ReadMemStats(&initialMem)

		// Create broker
		bkr := broker.New(config)
		bkr.Start()
		defer bkr.Stop()

		// Add test service
		bkr.Publish(moleculer.ServiceSchema{
			Name: "comparison-service",
			Actions: []moleculer.Action{
				{
					Name: "test",
					Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
						return map[string]interface{}{"result": "ok", "transporter": string(transporterType)}
					},
				},
			},
		})

		// Wait for service to be ready
		time.Sleep(100 * time.Millisecond)

		// Performance test
		callCount := 50
		startTime := time.Now()
		var errorCount int

		for i := 0; i < callCount; i++ {
			result := <-bkr.Call("comparison-service.test", map[string]interface{}{"iteration": i})
			if result.IsError() {
				errorCount++
			}
		}

		duration := time.Since(startTime)

		// Final memory measurement
		var finalMem runtime.MemStats
		runtime.ReadMemStats(&finalMem)

		throughput := float64(callCount) / duration.Seconds()
		avgLatency := duration / time.Duration(callCount)
		memoryUsage := finalMem.HeapAlloc - initialMem.HeapAlloc
		errorRate := float64(errorCount) / float64(callCount)

		metrics = append(metrics, TransporterMetrics{
			TransporterType: transporterType,
			Throughput:      throughput,
			AvgLatency:      avgLatency,
			MemoryUsage:     memoryUsage,
			ErrorRate:       errorRate,
		})
	}

	// Report comparison results
	t.Logf("\n=== Transporter Performance Comparison ===")
	t.Logf("%-10s %12s %15s %12s %10s", "Transporter", "Throughput", "Avg Latency", "Memory", "Error Rate")
	t.Logf("%-10s %12s %15s %12s %10s", "----------", "---------", "-----------", "------", "----------")

	for _, metric := range metrics {
		t.Logf("%-10s %12.2f %15v %12d %10.2f%%",
			GetTransporterName(metric.TransporterType),
			metric.Throughput,
			metric.AvgLatency,
			metric.MemoryUsage,
			metric.ErrorRate*100)
	}

	// Find best performers
	var bestThroughput TransporterMetrics
	var bestLatency TransporterMetrics
	var bestMemory TransporterMetrics

	for _, metric := range metrics {
		if metric.Throughput > bestThroughput.Throughput {
			bestThroughput = metric
		}
		if metric.AvgLatency < bestLatency.AvgLatency || bestLatency.TransporterType == "" {
			bestLatency = metric
		}
		if metric.MemoryUsage < bestMemory.MemoryUsage || bestMemory.TransporterType == "" {
			bestMemory = metric
		}
	}

	t.Logf("\n=== Best Performers ===")
	t.Logf("Best Throughput: %s (%.2f calls/sec)", GetTransporterName(bestThroughput.TransporterType), bestThroughput.Throughput)
	t.Logf("Best Latency: %s (%v)", GetTransporterName(bestLatency.TransporterType), bestLatency.AvgLatency)
	t.Logf("Best Memory: %s (%d bytes)", GetTransporterName(bestMemory.TransporterType), bestMemory.MemoryUsage)
}

// TestTransporterStress tests stress behavior across all available transporters
func TestTransporterStress(t *testing.T) {
	transporterTypes := GetAvailableTransporters()
	suite := NewTransporterTestSuite(transporterTypes)

	if len(suite.GetTransporterTypes()) == 0 {
		t.Skip("No transporters available for testing")
	}

	results := suite.RunTestForAllTransporters(func(transporterType TransporterType, config *moleculer.Config) error {
		t.Logf("Testing stress for %s transporter", GetTransporterName(transporterType))

		// Initial measurements
		initialGoroutines := runtime.NumGoroutine()
		var initialMem runtime.MemStats
		runtime.ReadMemStats(&initialMem)

		// Create broker
		bkr := broker.New(config)
		bkr.Start()
		defer bkr.Stop()

		// Add test service
		bkr.Publish(moleculer.ServiceSchema{
			Name: "stress-service",
			Actions: []moleculer.Action{
				{
					Name: "test",
					Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
						// Simulate variable work
						delay := params.Get("delay").Int()
						if delay > 0 {
							time.Sleep(time.Duration(delay) * time.Millisecond)
						}
						return map[string]interface{}{"result": "ok", "delay": delay}
					},
				},
			},
		})

		// Wait for service to be ready
		time.Sleep(100 * time.Millisecond)

		// Stress test
		concurrency := 20
		totalCalls := 200
		var wg sync.WaitGroup
		var errorCount int
		var mu sync.Mutex

		startTime := time.Now()

		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()

				callsPerWorker := totalCalls / concurrency
				for j := 0; j < callsPerWorker; j++ {
					delay := j % 10 // Variable delay
					result := <-bkr.Call("stress-service.test", map[string]interface{}{
						"worker": workerID,
						"call":   j,
						"delay":  delay,
					})

					if result.IsError() {
						mu.Lock()
						errorCount++
						mu.Unlock()
					}
				}
			}(i)
		}

		wg.Wait()
		duration := time.Since(startTime)

		// Final measurements
		runtime.GC()
		time.Sleep(100 * time.Millisecond)

		finalGoroutines := runtime.NumGoroutine()
		var finalMem runtime.MemStats
		runtime.ReadMemStats(&finalMem)

		throughput := float64(totalCalls) / duration.Seconds()
		errorRate := float64(errorCount) / float64(totalCalls)
		memoryGrowth := finalMem.HeapAlloc - initialMem.HeapAlloc
		goroutineLeak := finalGoroutines - initialGoroutines

		t.Logf("%s transporter - Throughput: %.2f calls/sec, Error rate: %.2f%%, Memory growth: %d bytes, Goroutine leak: %d",
			GetTransporterName(transporterType), throughput, errorRate*100, memoryGrowth, goroutineLeak)

		// Check for acceptable performance under stress
		if throughput < 1 {
			return fmt.Errorf("throughput too low under stress: %.2f calls/sec", throughput)
		}

		if errorRate > 0.2 { // 20% error rate under stress
			return fmt.Errorf("error rate too high under stress: %.2f%%", errorRate*100)
		}

		if memoryGrowth > 50*1024*1024 { // 50MB
			return fmt.Errorf("excessive memory growth under stress: %d bytes", memoryGrowth)
		}

		if goroutineLeak > 50 {
			return fmt.Errorf("goroutine leak under stress: %d goroutines", goroutineLeak)
		}

		return nil
	})

	// Report results
	for transporterType, err := range results {
		if err != nil {
			t.Errorf("%s transporter failed stress test: %v", GetTransporterName(transporterType), err)
		} else {
			t.Logf("✓ %s transporter passed stress test", GetTransporterName(transporterType))
		}
	}
}

// BenchmarkAllTransporters benchmarks all available transporters
func BenchmarkAllTransporters(b *testing.B) {
	transporterTypes := GetAvailableTransporters()
	suite := NewTransporterTestSuite(transporterTypes)

	if len(suite.GetTransporterTypes()) == 0 {
		b.Skip("No transporters available for benchmarking")
	}

	for i, transporterType := range suite.GetTransporterTypes() {
		config := suite.GetConfigs()[i]

		b.Run(GetTransporterName(transporterType), func(b *testing.B) {
			// Create broker
			bkr := broker.New(config)
			bkr.Start()
			defer bkr.Stop()

			// Add test service
			bkr.Publish(moleculer.ServiceSchema{
				Name: "benchmark-service",
				Actions: []moleculer.Action{
					{
						Name: "test",
						Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
							return map[string]interface{}{"result": "ok"}
						},
					},
				},
			})

			// Wait for service to be ready
			time.Sleep(100 * time.Millisecond)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				result := <-bkr.Call("benchmark-service.test", map[string]interface{}{"iteration": i})
				if result.IsError() {
					b.Fatalf("Call failed: %v", result.Error())
				}
			}
		})
	}
}
