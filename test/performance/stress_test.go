package performance

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
)

// StressTestConfig configuration for stress tests
type StressTestConfig struct {
	Duration            time.Duration
	BrokerCount         int
	ServicesPerBroker   int
	ActionsPerService   int
	EventsPerService    int
	ConcurrentCalls     int
	ConcurrentEvents    int
	CallInterval        time.Duration
	EventInterval       time.Duration
	ServiceChurnRate    time.Duration
	MemoryCheckInterval time.Duration
	LogLevel            string
}

// DefaultStressTestConfig returns default stress test configuration
func DefaultStressTestConfig() *StressTestConfig {
	return &StressTestConfig{
		Duration:            60 * time.Second,
		BrokerCount:         3,
		ServicesPerBroker:   20,
		ActionsPerService:   10,
		EventsPerService:    5,
		ConcurrentCalls:     50,
		ConcurrentEvents:    30,
		CallInterval:        10 * time.Millisecond,
		EventInterval:       50 * time.Millisecond,
		ServiceChurnRate:    5 * time.Second,
		MemoryCheckInterval: 5 * time.Second,
		LogLevel:            "ERROR",
	}
}

// StressTestResult contains results from stress testing
type StressTestResult struct {
	TotalCalls             int
	TotalEvents            int
	TotalErrors            int
	AverageLatency         time.Duration
	PeakLatency            time.Duration
	Throughput             float64
	ErrorRate              float64
	PeakMemoryUsage        uint64
	FinalMemoryUsage       uint64
	MemoryGrowth           uint64
	PeakGoroutineCount     int
	FinalGoroutineCount    int
	GoroutineLeak          int
	ServiceRegistrations   int
	ServiceDeregistrations int
	ConnectionCount        int
	TestDuration           time.Duration
}

// TestMemoryLeakStress tests for memory leaks under stress
func TestMemoryLeakStress(t *testing.T) {
	config := DefaultStressTestConfig()
	config.Duration = 30 * time.Second // Shorter for CI
	config.BrokerCount = 2
	config.ServicesPerBroker = 10

	// Enable memory profiling
	if testing.Verbose() {
		f, err := os.Create("stress_memory.prof")
		if err != nil {
			t.Fatalf("Failed to create memory profile: %v", err)
		}
		defer f.Close()

		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	result := runStressTestInternal(t, config)

	// Analyze results
	t.Logf("Stress test completed:")
	t.Logf("  Duration: %v", result.TestDuration)
	t.Logf("  Total calls: %d", result.TotalCalls)
	t.Logf("  Total events: %d", result.TotalEvents)
	t.Logf("  Total errors: %d", result.TotalErrors)
	t.Logf("  Average latency: %v", result.AverageLatency)
	t.Logf("  Peak latency: %v", result.PeakLatency)
	t.Logf("  Throughput: %.2f calls/sec", result.Throughput)
	t.Logf("  Error rate: %.2f%%", result.ErrorRate*100)
	t.Logf("  Peak memory: %d bytes", result.PeakMemoryUsage)
	t.Logf("  Memory growth: %d bytes", result.MemoryGrowth)
	t.Logf("  Goroutine leak: %d", result.GoroutineLeak)
	t.Logf("  Service registrations: %d", result.ServiceRegistrations)
	t.Logf("  Service deregistrations: %d", result.ServiceDeregistrations)

	// Check for memory leaks
	if result.MemoryGrowth > 50*1024*1024 { // 50MB
		t.Errorf("Memory leak detected: %d bytes growth", result.MemoryGrowth)
	}

	// Check for goroutine leaks
	if result.GoroutineLeak > 50 {
		t.Errorf("Goroutine leak detected: %d goroutines", result.GoroutineLeak)
	}

	// Check for acceptable error rate
	if result.ErrorRate > 0.1 { // 10% error rate
		t.Errorf("Error rate too high: %.2f%%", result.ErrorRate*100)
	}
}

// TestServiceCatalogMemoryLeak tests ServiceCatalog memory leak specifically
func TestServiceCatalogMemoryLeak(t *testing.T) {
	config := DefaultStressTestConfig()
	config.Duration = 20 * time.Second
	config.BrokerCount = 1
	config.ServicesPerBroker = 100
	config.ServiceChurnRate = 1 * time.Second // High churn rate

	result := runStressTestInternal(t, config)

	t.Logf("Service catalog memory test:")
	t.Logf("  Service registrations: %d", result.ServiceRegistrations)
	t.Logf("  Service deregistrations: %d", result.ServiceDeregistrations)
	t.Logf("  Memory growth: %d bytes", result.MemoryGrowth)

	// Check for excessive memory growth due to service catalog
	if result.MemoryGrowth > 20*1024*1024 { // 20MB
		t.Errorf("Service catalog memory leak: %d bytes growth", result.MemoryGrowth)
	}
}

// TestConnectionBufferLeak tests TCP connection buffer leaks
func TestConnectionBufferLeak(t *testing.T) {
	// This test would require TCP transport
	// For now, we'll simulate connection-like behavior with memory transport

	config := DefaultStressTestConfig()
	config.Duration = 15 * time.Second
	config.BrokerCount = 5
	config.ConcurrentCalls = 100

	result := runStressTestInternal(t, config)

	t.Logf("Connection buffer test:")
	t.Logf("  Total calls: %d", result.TotalCalls)
	t.Logf("  Memory growth: %d bytes", result.MemoryGrowth)
	t.Logf("  Goroutine leak: %d", result.GoroutineLeak)

	// Check for connection-related memory leaks
	if result.MemoryGrowth > 10*1024*1024 { // 10MB
		t.Errorf("Potential connection buffer leak: %d bytes growth", result.MemoryGrowth)
	}
}

// TestHighFrequencyOperations tests high-frequency operations
func TestHighFrequencyOperations(t *testing.T) {
	config := DefaultStressTestConfig()
	config.Duration = 10 * time.Second
	config.BrokerCount = 2
	config.ConcurrentCalls = 200
	config.CallInterval = 1 * time.Millisecond
	config.EventInterval = 5 * time.Millisecond

	result := runStressTestInternal(t, config)

	t.Logf("High frequency test:")
	t.Logf("  Throughput: %.2f calls/sec", result.Throughput)
	t.Logf("  Average latency: %v", result.AverageLatency)
	t.Logf("  Peak latency: %v", result.PeakLatency)
	t.Logf("  Error rate: %.2f%%", result.ErrorRate*100)

	// Check for reasonable throughput
	if result.Throughput < 100 {
		t.Errorf("Throughput too low: %.2f calls/sec", result.Throughput)
	}

	// Check for reasonable latency
	if result.AverageLatency > 100*time.Millisecond {
		t.Errorf("Average latency too high: %v", result.AverageLatency)
	}
}

// runStressTestInternal runs the actual stress test
func runStressTestInternal(t *testing.T, config *StressTestConfig) *StressTestResult {
	// Initial measurements
	initialGoroutines := runtime.NumGoroutine()
	var initialMem runtime.MemStats
	runtime.ReadMemStats(&initialMem)

	// Create brokers
	brokers := make([]*broker.ServiceBroker, config.BrokerCount)
	for i := 0; i < config.BrokerCount; i++ {
		brokerConfig := CreateTestConfig(TransporterMemory, config.LogLevel)
		brokers[i] = broker.New(brokerConfig)
		brokers[i].Start()
	}

	// Cleanup
	defer func() {
		for _, broker := range brokers {
			broker.Stop()
		}
		time.Sleep(1 * time.Second) // Allow cleanup
	}()

	// Wait for brokers to be ready
	time.Sleep(1 * time.Second)

	// Test metrics
	var mu sync.Mutex
	var totalCalls int
	var totalEvents int
	var totalErrors int
	var totalLatency time.Duration
	var peakLatency time.Duration
	var peakMemory uint64
	var serviceRegistrations int
	var serviceDeregistrations int

	// Context for cancellation
	ctx, cancel := context.WithTimeout(context.Background(), config.Duration)
	defer cancel()

	var wg sync.WaitGroup

	// Start call workers
	for i := 0; i < config.ConcurrentCalls; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			bkr := brokers[workerID%len(brokers)]
			ticker := time.NewTicker(config.CallInterval)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					// Make a call
					serviceIndex := int(time.Now().UnixNano()) % config.ServicesPerBroker
					actionIndex := int(time.Now().UnixNano()) % config.ActionsPerService
					serviceName := fmt.Sprintf("service-%d", serviceIndex)
					actionName := fmt.Sprintf("action-%d", actionIndex)

					start := time.Now()
					result := <-bkr.Call(serviceName+"."+actionName, map[string]interface{}{
						"worker": workerID,
						"time":   time.Now().UnixNano(),
					})
					latency := time.Since(start)

					mu.Lock()
					totalCalls++
					totalLatency += latency
					if latency > peakLatency {
						peakLatency = latency
					}
					if result.IsError() {
						totalErrors++
					}
					mu.Unlock()
				}
			}
		}(i)
	}

	// Start event workers
	for i := 0; i < config.ConcurrentEvents; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			bkr := brokers[workerID%len(brokers)]
			ticker := time.NewTicker(config.EventInterval)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					// Emit an event
					eventIndex := int(time.Now().UnixNano()) % config.EventsPerService
					eventName := fmt.Sprintf("event-%d", eventIndex)

					bkr.Emit(eventName, map[string]interface{}{
						"worker": workerID,
						"time":   time.Now().UnixNano(),
					})

					mu.Lock()
					totalEvents++
					mu.Unlock()
				}
			}
		}(i)
	}

	// Start service churn worker
	wg.Add(1)
	go func() {
		defer wg.Done()

		ticker := time.NewTicker(config.ServiceChurnRate)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Register new services
				for i := 0; i < 5; i++ {
					serviceName := fmt.Sprintf("churn-service-%d-%d",
						time.Now().UnixNano(), i)

					bkr := brokers[i%len(brokers)]
					bkr.Publish(moleculer.ServiceSchema{
						Name: serviceName,
						Actions: []moleculer.Action{
							{
								Name: "test",
								Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
									return map[string]interface{}{"result": "ok"}
								},
							},
						},
					})

					mu.Lock()
					serviceRegistrations++
					mu.Unlock()
				}
			}
		}
	}()

	// Start memory monitoring
	wg.Add(1)
	go func() {
		defer wg.Done()

		ticker := time.NewTicker(config.MemoryCheckInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				var mem runtime.MemStats
				runtime.ReadMemStats(&mem)

				mu.Lock()
				if mem.HeapAlloc > peakMemory {
					peakMemory = mem.HeapAlloc
				}
				mu.Unlock()
			}
		}
	}()

	// Wait for test to complete
	wg.Wait()

	// Final measurements
	finalGoroutines := runtime.NumGoroutine()
	var finalMem runtime.MemStats
	runtime.ReadMemStats(&finalMem)

	// Calculate results
	var averageLatency time.Duration
	if totalCalls > 0 {
		averageLatency = totalLatency / time.Duration(totalCalls)
	}

	throughput := float64(totalCalls) / config.Duration.Seconds()
	errorRate := float64(totalErrors) / float64(totalCalls)

	return &StressTestResult{
		TotalCalls:             totalCalls,
		TotalEvents:            totalEvents,
		TotalErrors:            totalErrors,
		AverageLatency:         averageLatency,
		PeakLatency:            peakLatency,
		Throughput:             throughput,
		ErrorRate:              errorRate,
		PeakMemoryUsage:        peakMemory,
		FinalMemoryUsage:       finalMem.HeapAlloc,
		MemoryGrowth:           finalMem.HeapAlloc - initialMem.HeapAlloc,
		PeakGoroutineCount:     finalGoroutines,
		FinalGoroutineCount:    finalGoroutines,
		GoroutineLeak:          finalGoroutines - initialGoroutines,
		ServiceRegistrations:   serviceRegistrations,
		ServiceDeregistrations: serviceDeregistrations,
		TestDuration:           config.Duration,
	}
}

// TestResourceExhaustion tests behavior under resource exhaustion
func TestResourceExhaustion(t *testing.T) {
	config := DefaultStressTestConfig()
	config.Duration = 5 * time.Second
	config.BrokerCount = 1
	config.ServicesPerBroker = 1000 // Very high service count
	config.ConcurrentCalls = 1000   // Very high concurrency

	result := runStressTestInternal(t, config)

	t.Logf("Resource exhaustion test:")
	t.Logf("  Services: %d", config.ServicesPerBroker)
	t.Logf("  Concurrent calls: %d", config.ConcurrentCalls)
	t.Logf("  Memory growth: %d bytes", result.MemoryGrowth)
	t.Logf("  Goroutine leak: %d", result.GoroutineLeak)
	t.Logf("  Error rate: %.2f%%", result.ErrorRate*100)

	// Check that system remains stable under high load
	if result.ErrorRate > 0.5 { // 50% error rate
		t.Errorf("System unstable under load: %.2f%% error rate", result.ErrorRate*100)
	}
}

// TestLongRunningStability tests long-running stability
func TestLongRunningStability(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping long running test in short mode")
	}

	config := DefaultStressTestConfig()
	config.Duration = 5 * time.Minute
	config.BrokerCount = 2
	config.ServicesPerBroker = 50
	config.ConcurrentCalls = 100
	config.MemoryCheckInterval = 30 * time.Second

	result := runStressTestInternal(t, config)

	t.Logf("Long running stability test:")
	t.Logf("  Duration: %v", result.TestDuration)
	t.Logf("  Total calls: %d", result.TotalCalls)
	t.Logf("  Memory growth: %d bytes", result.MemoryGrowth)
	t.Logf("  Goroutine leak: %d", result.GoroutineLeak)
	t.Logf("  Error rate: %.2f%%", result.ErrorRate*100)

	// Check for memory leaks over time
	if result.MemoryGrowth > 100*1024*1024 { // 100MB
		t.Errorf("Memory leak in long running test: %d bytes growth", result.MemoryGrowth)
	}

	// Check for goroutine leaks
	if result.GoroutineLeak > 100 {
		t.Errorf("Goroutine leak in long running test: %d goroutines", result.GoroutineLeak)
	}
}

// BenchmarkStressTest benchmarks stress test performance
func BenchmarkStressTest(b *testing.B) {
	config := DefaultStressTestConfig()
	config.Duration = 10 * time.Second
	config.BrokerCount = 1
	config.ServicesPerBroker = 10
	config.ConcurrentCalls = 10

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result := runStressTestInternal(nil, config)
		_ = result // Prevent optimization
	}
}
