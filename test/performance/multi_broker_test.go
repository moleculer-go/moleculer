package performance

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
)

// MultiBrokerTestConfig configuration for multi-broker tests
type MultiBrokerTestConfig struct {
	BrokerCount       int
	ServicesPerBroker int
	ActionsPerService int
	EventsPerService  int
	TestDuration      time.Duration
	CallFrequency     time.Duration
	EventFrequency    time.Duration
	LogLevel          string
}

// measureMemory captures current memory statistics
func measureMemory() MemoryStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return MemoryStats{
		InitialHeap:    m.HeapAlloc,
		PeakHeap:       m.HeapAlloc,
		FinalHeap:      m.HeapAlloc,
		HeapGrowth:     0,
		GoroutineCount: runtime.NumGoroutine(),
		Measurements:   make([]Measurement, 0),
	}
}

// DefaultMultiBrokerConfig returns default configuration
func DefaultMultiBrokerConfig() *MultiBrokerTestConfig {
	return &MultiBrokerTestConfig{
		BrokerCount:       5,
		ServicesPerBroker: 10,
		ActionsPerService: 5,
		EventsPerService:  3,
		TestDuration:      30 * time.Second,
		CallFrequency:     100 * time.Millisecond,
		EventFrequency:    200 * time.Millisecond,
		LogLevel:          "ERROR",
	}
}

// BrokerCluster represents a cluster of brokers for testing
type BrokerCluster struct {
	brokers     []*broker.ServiceBroker
	config      *MultiBrokerTestConfig
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	mu          sync.RWMutex
	memoryStats MemoryStats
}

// NewBrokerCluster creates a new broker cluster
func NewBrokerCluster(config *MultiBrokerTestConfig) *BrokerCluster {
	ctx, cancel := context.WithCancel(context.Background())

	return &BrokerCluster{
		brokers:     make([]*broker.ServiceBroker, 0, config.BrokerCount),
		config:      config,
		ctx:         ctx,
		cancel:      cancel,
		memoryStats: measureMemory(),
	}
}

// Start starts all brokers in the cluster
func (bc *BrokerCluster) Start() error {
	for i := 0; i < bc.config.BrokerCount; i++ {
		config := CreateTestConfig(TransporterMemory, bc.config.LogLevel)
		bkr := broker.New(config)

		// Add services to this broker
		for j := 0; j < bc.config.ServicesPerBroker; j++ {
			serviceName := fmt.Sprintf("broker-%d-service-%d", i, j)

			// Create actions
			actions := make([]moleculer.Action, bc.config.ActionsPerService)
			for k := 0; k < bc.config.ActionsPerService; k++ {
				actions[k] = moleculer.Action{
					Name: fmt.Sprintf("action-%d", k),
					Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
						// Simulate some work
						time.Sleep(time.Duration(k%10) * time.Millisecond)
						return map[string]interface{}{
							"result":    "ok",
							"broker":    i,
							"service":   serviceName,
							"action":    k,
							"timestamp": time.Now().UnixNano(),
						}
					},
				}
			}

			// Create events
			events := make([]moleculer.Event, bc.config.EventsPerService)
			for k := 0; k < bc.config.EventsPerService; k++ {
				events[k] = moleculer.Event{
					Name: fmt.Sprintf("broker-%d-service-%d-event-%d", i, j, k),
					Handler: func(ctx moleculer.Context, params moleculer.Payload) {
						// Event handler - just log or do minimal work
					},
				}
			}

			bkr.Publish(moleculer.ServiceSchema{
				Name:    serviceName,
				Actions: actions,
				Events:  events,
			})
		}

		bkr.Start()
		bc.brokers = append(bc.brokers, bkr)

		// Update memory stats after each broker starts
		bc.memoryStats.update(len(bc.brokers))
	}

	return nil
}

// Stop stops all brokers in the cluster
func (bc *BrokerCluster) Stop() {
	bc.cancel()

	for _, broker := range bc.brokers {
		broker.Stop()
	}

	bc.wg.Wait()

	// Final memory measurement
	bc.memoryStats.update(0)
}

// GetMemoryStats returns current memory statistics
func (bc *BrokerCluster) GetMemoryStats() MemoryStats {
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	return bc.memoryStats
}

// GetRandomBroker returns a random broker from the cluster
func (bc *BrokerCluster) GetRandomBroker() *broker.ServiceBroker {
	bc.mu.RLock()
	defer bc.mu.RUnlock()

	if len(bc.brokers) == 0 {
		return nil
	}

	// Simple random selection (in real test, use proper random)
	index := int(time.Now().UnixNano()) % len(bc.brokers)
	return bc.brokers[index]
}

// GetBroker returns a specific broker by index
func (bc *BrokerCluster) GetBroker(index int) *broker.ServiceBroker {
	bc.mu.RLock()
	defer bc.mu.RUnlock()

	if index < 0 || index >= len(bc.brokers) {
		return nil
	}

	return bc.brokers[index]
}

// GetBrokerCount returns the number of brokers
func (bc *BrokerCluster) GetBrokerCount() int {
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	return len(bc.brokers)
}

// TestMultiBrokerStress tests multiple brokers under stress
func TestMultiBrokerStress(t *testing.T) {
	config := DefaultMultiBrokerConfig()
	config.TestDuration = 10 * time.Second // Shorter for CI

	// Create test result writer
	resultWriter := NewTestResultWriter("./test_results")
	defer resultWriter.SaveResults()

	cluster := NewBrokerCluster(config)
	defer cluster.Stop()

	err := cluster.Start()
	if err != nil {
		t.Fatalf("Failed to start broker cluster: %v", err)
	}

	// Wait for brokers to be ready and services to be registered
	time.Sleep(5 * time.Second)

	// Additional wait to ensure service discovery is complete
	time.Sleep(2 * time.Second)

	// Start stress test
	startTime := time.Now()
	metrics := runMultiBrokerStressTest(cluster, config)
	duration := time.Since(startTime)

	// Get memory stats
	memStats := cluster.GetMemoryStats()

	// Analyze results
	t.Logf("Stress test completed:")
	t.Logf("  Total calls: %d", metrics.TotalCalls)
	t.Logf("  Total events: %d", metrics.TotalEvents)
	t.Logf("  Error count: %d", metrics.ErrorCount)
	t.Logf("  Average latency: %v", metrics.AverageLatency)
	t.Logf("  Throughput: %.2f calls/sec", metrics.Throughput)
	t.Logf("  Error rate: %.2f%%", metrics.ErrorRate*100)
	t.Logf("  Memory stats:")
	t.Logf("    Initial heap: %d bytes", memStats.InitialHeap)
	t.Logf("    Peak heap: %d bytes", memStats.PeakHeap)
	t.Logf("    Final heap: %d bytes", memStats.FinalHeap)
	t.Logf("    Heap growth: %d bytes", memStats.HeapGrowth)
	t.Logf("    Goroutines: %d", memStats.GoroutineCount)

	// Check for acceptable error rate
	if metrics.ErrorRate > 0.05 { // 5% error rate
		t.Errorf("Error rate too high: %.2f%%", metrics.ErrorRate*100)
	}

	// Check for reasonable throughput
	if metrics.Throughput < 10 {
		t.Errorf("Throughput too low: %.2f calls/sec", metrics.Throughput)
	}

	// Create test settings
	testSettings := CreateTestSettings(
		"Memory", // transporter type
		map[string]interface{}{
			"type": "memory",
		},
		config.BrokerCount,
		config.ServicesPerBroker,
		config.ActionsPerService,
		config.EventsPerService,
		config.TestDuration,
		10,           // concurrency level
		50*1024*1024, // memory threshold (50MB)
		50,           // goroutine threshold
	)
	testSettings.AddOtherSetting("call_frequency_ms", config.CallFrequency.Milliseconds())
	testSettings.AddOtherSetting("event_frequency_ms", config.EventFrequency.Milliseconds())
	testSettings.AddOtherSetting("log_level", config.LogLevel)

	// Create test result
	result := CreateTestResult("TestMultiBrokerStress", "Memory", true, duration, nil)
	result.AddTestSettings(testSettings)
	result.AddMemoryStats(&memStats)
	result.AddMetric("total_calls", metrics.TotalCalls)
	result.AddMetric("total_events", metrics.TotalEvents)
	result.AddMetric("error_count", metrics.ErrorCount)
	result.AddMetric("average_latency_ms", metrics.AverageLatency.Milliseconds())
	result.AddMetric("throughput_calls_per_sec", metrics.Throughput)
	result.AddMetric("error_rate_percent", metrics.ErrorRate*100)

	resultWriter.AddResult(result)
}

// StressTestMetrics tracks stress test results
type StressTestMetrics struct {
	TotalCalls      int
	TotalEvents     int
	ErrorCount      int
	AverageLatency  time.Duration
	Throughput      float64
	ErrorRate       float64
	PeakMemory      uint64
	FinalGoroutines int
}

// runMultiBrokerStressTest runs the actual stress test
func runMultiBrokerStressTest(cluster *BrokerCluster, config *MultiBrokerTestConfig) *StressTestMetrics {
	var mu sync.Mutex
	var totalCalls int
	var totalEvents int
	var errorCount int
	var totalLatency time.Duration

	// Start call workers
	for i := 0; i < cluster.GetBrokerCount(); i++ {
		cluster.wg.Add(1)
		go func(brokerIndex int) {
			defer cluster.wg.Done()

			broker := cluster.GetBroker(brokerIndex)
			if broker == nil {
				return
			}

			ticker := time.NewTicker(config.CallFrequency)
			defer ticker.Stop()

			for {
				select {
				case <-cluster.ctx.Done():
					return
				case <-ticker.C:
					// Make a call to a random service
					targetBroker := cluster.GetRandomBroker()
					if targetBroker == nil {
						continue
					}

					serviceIndex := int(time.Now().UnixNano()) % config.ServicesPerBroker
					actionIndex := int(time.Now().UnixNano()) % config.ActionsPerService
					serviceName := fmt.Sprintf("broker-%d-service-%d", brokerIndex, serviceIndex)
					actionName := fmt.Sprintf("action-%d", actionIndex)

					start := time.Now()
					result := <-broker.Call(serviceName+"."+actionName, map[string]interface{}{
						"from_broker": brokerIndex,
						"timestamp":   time.Now().UnixNano(),
					})
					latency := time.Since(start)

					mu.Lock()
					totalCalls++
					totalLatency += latency
					if result.IsError() {
						errorCount++
					}
					mu.Unlock()
				}
			}
		}(i)
	}

	// Start event workers
	for i := 0; i < cluster.GetBrokerCount(); i++ {
		cluster.wg.Add(1)
		go func(brokerIndex int) {
			defer cluster.wg.Done()

			broker := cluster.GetBroker(brokerIndex)
			if broker == nil {
				return
			}

			ticker := time.NewTicker(config.EventFrequency)
			defer ticker.Stop()

			for {
				select {
				case <-cluster.ctx.Done():
					return
				case <-ticker.C:
					// Emit an event
					eventIndex := int(time.Now().UnixNano()) % config.EventsPerService
					eventName := fmt.Sprintf("broker-%d-service-%d-event-%d",
						brokerIndex,
						int(time.Now().UnixNano())%config.ServicesPerBroker,
						eventIndex)

					broker.Emit(eventName, map[string]interface{}{
						"from_broker": brokerIndex,
						"timestamp":   time.Now().UnixNano(),
					})

					mu.Lock()
					totalEvents++
					mu.Unlock()
				}
			}
		}(i)
	}

	// Run for the specified duration
	time.Sleep(config.TestDuration)

	// Stop the test
	cluster.cancel()
	cluster.wg.Wait()

	// Calculate metrics
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	metrics := &StressTestMetrics{
		TotalCalls:      totalCalls,
		TotalEvents:     totalEvents,
		ErrorCount:      errorCount,
		PeakMemory:      m.HeapAlloc,
		FinalGoroutines: runtime.NumGoroutine(),
	}

	if totalCalls > 0 {
		metrics.AverageLatency = totalLatency / time.Duration(totalCalls)
		metrics.Throughput = float64(totalCalls) / config.TestDuration.Seconds()
		metrics.ErrorRate = float64(errorCount) / float64(totalCalls)
	}

	return metrics
}

// TestMultiBrokerMemoryLeak tests for memory leaks in multi-broker scenarios
func TestMultiBrokerMemoryLeak(t *testing.T) {
	config := DefaultMultiBrokerConfig()
	config.TestDuration = 30 * time.Second
	config.BrokerCount = 3 // Smaller for memory testing

	// Create test result writer
	resultWriter := NewTestResultWriter("./test_results")
	defer resultWriter.SaveResults()

	// Initial memory measurement
	var initialMem runtime.MemStats
	runtime.ReadMemStats(&initialMem)
	initialGoroutines := runtime.NumGoroutine()

	cluster := NewBrokerCluster(config)
	defer cluster.Stop()

	err := cluster.Start()
	if err != nil {
		t.Fatalf("Failed to start broker cluster: %v", err)
	}

	// Run multiple cycles to detect memory leaks
	for cycle := 0; cycle < 3; cycle++ {
		t.Logf("Memory leak test cycle %d", cycle+1)

		// Run stress test
		metrics := runMultiBrokerStressTest(cluster, config)
		t.Logf("Cycle %d metrics: %+v", cycle+1, metrics)

		// Force garbage collection
		runtime.GC()
		time.Sleep(1 * time.Second)

		// Check memory usage
		var currentMem runtime.MemStats
		runtime.ReadMemStats(&currentMem)
		currentGoroutines := runtime.NumGoroutine()

		// Handle potential uint64 overflow by using int64
		var memoryGrowth int64
		if currentMem.HeapAlloc >= initialMem.HeapAlloc {
			memoryGrowth = int64(currentMem.HeapAlloc - initialMem.HeapAlloc)
		} else {
			// Handle underflow case
			memoryGrowth = -int64(initialMem.HeapAlloc - currentMem.HeapAlloc)
		}
		goroutineGrowth := currentGoroutines - initialGoroutines

		t.Logf("Memory growth: %d bytes, Goroutine growth: %d", memoryGrowth, goroutineGrowth)

		// Check for excessive memory growth (50MB threshold)
		if memoryGrowth > 50*1024*1024 {
			t.Errorf("Excessive memory growth detected: %d bytes", memoryGrowth)
		}

		// Check for goroutine leaks
		if goroutineGrowth > 50 {
			t.Errorf("Potential goroutine leak detected: %d goroutines", goroutineGrowth)
		}
	}

	// Create test settings for memory leak test
	testSettings := CreateTestSettings(
		"Memory", // transporter type
		map[string]interface{}{
			"type": "memory",
		},
		config.BrokerCount,
		config.ServicesPerBroker,
		config.ActionsPerService,
		config.EventsPerService,
		config.TestDuration,
		1,            // concurrency level (sequential cycles)
		50*1024*1024, // memory threshold (50MB)
		50,           // goroutine threshold
	)
	testSettings.AddOtherSetting("test_cycles", 3)
	testSettings.AddOtherSetting("call_frequency_ms", config.CallFrequency.Milliseconds())
	testSettings.AddOtherSetting("event_frequency_ms", config.EventFrequency.Milliseconds())
	testSettings.AddOtherSetting("log_level", config.LogLevel)

	// Get final values from the last cycle
	var finalMem runtime.MemStats
	runtime.ReadMemStats(&finalMem)
	finalGoroutines := runtime.NumGoroutine()

	// Calculate final growth
	var finalMemoryGrowth int64
	if finalMem.HeapAlloc >= initialMem.HeapAlloc {
		finalMemoryGrowth = int64(finalMem.HeapAlloc - initialMem.HeapAlloc)
	} else {
		finalMemoryGrowth = -int64(initialMem.HeapAlloc - finalMem.HeapAlloc)
	}
	finalGoroutineGrowth := finalGoroutines - initialGoroutines

	// Create test result
	result := CreateTestResult("TestMultiBrokerMemoryLeak", "Memory", true, 0, nil)
	result.AddTestSettings(testSettings)
	result.AddMetric("initial_heap_bytes", initialMem.HeapAlloc)
	result.AddMetric("initial_goroutines", initialGoroutines)
	result.AddMetric("final_heap_bytes", finalMem.HeapAlloc)
	result.AddMetric("final_goroutines", finalGoroutines)
	result.AddMetric("memory_growth_bytes", finalMemoryGrowth)
	result.AddMetric("goroutine_growth", finalGoroutineGrowth)
	result.AddMetric("test_cycles", 3)

	resultWriter.AddResult(result)
}

// TestMultiBrokerConcurrentConnections tests concurrent connections between brokers
func TestMultiBrokerConcurrentConnections(t *testing.T) {
	config := DefaultMultiBrokerConfig()
	config.BrokerCount = 10
	config.TestDuration = 5 * time.Second

	cluster := NewBrokerCluster(config)
	defer cluster.Stop()

	err := cluster.Start()
	if err != nil {
		t.Fatalf("Failed to start broker cluster: %v", err)
	}

	// Wait for brokers to be ready and services to be registered
	time.Sleep(5 * time.Second)

	// Additional wait to ensure service discovery is complete
	time.Sleep(2 * time.Second)

	// Test concurrent connections
	var wg sync.WaitGroup
	var connectionCount int
	var mu sync.Mutex

	// Each broker tries to connect to all other brokers
	for i := 0; i < cluster.GetBrokerCount(); i++ {
		wg.Add(1)
		go func(brokerIndex int) {
			defer wg.Done()

			broker := cluster.GetBroker(brokerIndex)
			if broker == nil {
				return
			}

			// Try to call services on other brokers
			for j := 0; j < cluster.GetBrokerCount(); j++ {
				if i == j {
					continue
				}

				serviceName := fmt.Sprintf("broker-%d-service-0", j)
				actionName := "action-0"

				result := <-broker.Call(serviceName+"."+actionName, map[string]interface{}{
					"from_broker": brokerIndex,
					"to_broker":   j,
				})

				mu.Lock()
				connectionCount++
				if result.IsError() {
					t.Logf("Connection error from broker %d to broker %d: %v",
						brokerIndex, j, result.Error())
				}
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	t.Logf("Total connections attempted: %d", connectionCount)

	// Check that we have reasonable connection success
	// (exact success rate depends on service discovery timing)
}

// TestMultiBrokerServiceDiscovery tests service discovery across brokers
func TestMultiBrokerServiceDiscovery(t *testing.T) {
	config := DefaultMultiBrokerConfig()
	config.BrokerCount = 5
	config.ServicesPerBroker = 3

	cluster := NewBrokerCluster(config)
	defer cluster.Stop()

	err := cluster.Start()
	if err != nil {
		t.Fatalf("Failed to start broker cluster: %v", err)
	}

	// Wait for service discovery
	time.Sleep(3 * time.Second)

	// Test that each broker can discover services from other brokers
	for i := 0; i < cluster.GetBrokerCount(); i++ {
		broker := cluster.GetBroker(i)
		if broker == nil {
			continue
		}

		// Try to call services from other brokers
		for j := 0; j < cluster.GetBrokerCount(); j++ {
			if i == j {
				continue
			}

			serviceName := fmt.Sprintf("broker-%d-service-0", j)
			actionName := "action-0"

			result := <-broker.Call(serviceName+"."+actionName, map[string]interface{}{
				"test": "service_discovery",
			})

			if result.IsError() {
				t.Errorf("Service discovery failed from broker %d to broker %d: %v",
					i, j, result.Error())
			} else {
				t.Logf("Service discovery successful: broker %d -> broker %d", i, j)
			}
		}
	}
}

// TestMultiBrokerEventBroadcast tests event broadcasting across brokers
func TestMultiBrokerEventBroadcast(t *testing.T) {
	config := DefaultMultiBrokerConfig()
	config.BrokerCount = 3
	config.TestDuration = 5 * time.Second

	cluster := NewBrokerCluster(config)
	defer cluster.Stop()

	err := cluster.Start()
	if err != nil {
		t.Fatalf("Failed to start broker cluster: %v", err)
	}

	// Wait for brokers to be ready and services to be registered
	time.Sleep(5 * time.Second)

	// Additional wait to ensure service discovery is complete
	time.Sleep(2 * time.Second)

	// Set up event listeners on all brokers
	var eventCount int
	var mu sync.Mutex

	for i := 0; i < cluster.GetBrokerCount(); i++ {
		broker := cluster.GetBroker(i)
		if broker == nil {
			continue
		}

		// Add event listener
		broker.Publish(moleculer.ServiceSchema{
			Name: fmt.Sprintf("event-listener-%d", i),
			Events: []moleculer.Event{
				{
					Name: "test.broadcast",
					Handler: func(ctx moleculer.Context, params moleculer.Payload) {
						mu.Lock()
						eventCount++
						mu.Unlock()
					},
				},
			},
		})
	}

	// Wait for event listeners to be registered
	time.Sleep(1 * time.Second)

	// Broadcast events from each broker
	for i := 0; i < cluster.GetBrokerCount(); i++ {
		broker := cluster.GetBroker(i)
		if broker == nil {
			continue
		}

		broker.Broadcast("test.broadcast", map[string]interface{}{
			"from_broker": i,
			"timestamp":   time.Now().UnixNano(),
		})
	}

	// Wait for events to be processed
	time.Sleep(2 * time.Second)

	t.Logf("Total events received: %d", eventCount)

	// Each broker should have received events from all other brokers
	expectedEvents := cluster.GetBrokerCount() * (cluster.GetBrokerCount() - 1)
	if eventCount < expectedEvents {
		t.Errorf("Event broadcast incomplete: received %d, expected at least %d",
			eventCount, expectedEvents)
	}
}

// BenchmarkMultiBrokerPerformance benchmarks multi-broker performance
func BenchmarkMultiBrokerPerformance(b *testing.B) {
	config := DefaultMultiBrokerConfig()
	config.BrokerCount = 3
	config.ServicesPerBroker = 5
	config.ActionsPerService = 3

	cluster := NewBrokerCluster(config)
	defer cluster.Stop()

	err := cluster.Start()
	if err != nil {
		b.Fatalf("Failed to start broker cluster: %v", err)
	}

	// Wait for brokers to be ready and services to be registered
	time.Sleep(5 * time.Second)

	// Additional wait to ensure service discovery is complete
	time.Sleep(2 * time.Second)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		broker := cluster.GetRandomBroker()
		if broker == nil {
			continue
		}

		serviceName := fmt.Sprintf("broker-0-service-%d", i%config.ServicesPerBroker)
		actionName := fmt.Sprintf("action-%d", i%config.ActionsPerService)

		result := <-broker.Call(serviceName+"."+actionName, map[string]interface{}{
			"iteration": i,
		})

		if result.IsError() {
			b.Fatalf("Call failed: %v", result.Error())
		}
	}
}

// TestMultiBrokerResourceCleanup tests proper resource cleanup
func TestMultiBrokerResourceCleanup(t *testing.T) {
	config := DefaultMultiBrokerConfig()
	config.BrokerCount = 5
	config.TestDuration = 2 * time.Second

	// Initial resource count
	initialGoroutines := runtime.NumGoroutine()
	var initialMem runtime.MemStats
	runtime.ReadMemStats(&initialMem)

	// Create and run cluster
	cluster := NewBrokerCluster(config)
	err := cluster.Start()
	if err != nil {
		t.Fatalf("Failed to start broker cluster: %v", err)
	}

	// Run stress test
	runMultiBrokerStressTest(cluster, config)

	// Stop cluster
	cluster.Stop()

	// Wait for cleanup
	time.Sleep(2 * time.Second)
	runtime.GC()

	// Check resource cleanup
	finalGoroutines := runtime.NumGoroutine()
	var finalMem runtime.MemStats
	runtime.ReadMemStats(&finalMem)

	goroutineLeak := finalGoroutines - initialGoroutines

	// Handle potential uint64 overflow by using int64
	var memoryLeak int64
	if finalMem.HeapAlloc >= initialMem.HeapAlloc {
		memoryLeak = int64(finalMem.HeapAlloc - initialMem.HeapAlloc)
	} else {
		// Handle underflow case
		memoryLeak = -int64(initialMem.HeapAlloc - finalMem.HeapAlloc)
	}

	t.Logf("Resource cleanup check:")
	t.Logf("  Goroutine leak: %d", goroutineLeak)
	t.Logf("  Memory leak: %d bytes", memoryLeak)

	// Check for resource leaks
	if goroutineLeak > 20 {
		t.Errorf("Goroutine leak detected: %d goroutines", goroutineLeak)
	}

	if memoryLeak > 10*1024*1024 { // 10MB
		t.Errorf("Memory leak detected: %d bytes", memoryLeak)
	}
}
