package growth

import (
	"testing"
	"time"
)

func TestDynamicScalingTestTCP(t *testing.T) {
	// Load configuration
	config, err := LoadDynamicScalingTestConfig("configs/dynamic_scaling_test.json")
	if err != nil {
		t.Fatalf("Failed to load configuration: %v", err)
	}

	// Create test instance
	test := NewDynamicScalingTest(config)

	// Run test with TCP transporter
	result, err := test.Run("TCP")
	if err != nil {
		t.Fatalf("Dynamic scaling test failed: %v", err)
	}

	// Validate results
	if !result.Success {
		t.Errorf("Test was not successful")
	}

	// Log results
	t.Logf("Dynamic Scaling Test Results:")
	t.Logf("  Test Name: %s", result.TestName)
	t.Logf("  Total Duration: %.2f ms", result.TotalDurationMs)
	t.Logf("  Max Brokers Reached: %d", result.MaxBrokersReached)
	t.Logf("  Total Actions Executed: %d", result.TotalActionsExecuted)
	t.Logf("  Total Events Received: %d", result.TotalEventsReceived)
	t.Logf("  Peak Memory Usage: %d bytes", result.PeakMemoryUsage)
	t.Logf("  Peak Goroutine Count: %d", result.PeakGoroutineCount)
	t.Logf("  Final Goroutine Leak: %d", result.FinalGoroutineLeak)

	// Log phase details
	t.Logf("  Phases:")
	for i, phase := range result.Phases {
		t.Logf("    Phase %d: %s (Brokers: %d, Duration: %.2f ms)",
			i+1, phase.PhaseName, phase.BrokerCount, phase.DurationMs)
	}

	// Log metrics details
	t.Logf("  Metrics:")
	for i, metric := range result.Metrics {
		t.Logf("    Metric %d: %s (Brokers: %d, Execution: %.2f ms, Memory: %d bytes, Goroutines: %d, Leak: %d)",
			i+1, metric.PhaseName, metric.BrokerCount, metric.ExecutionTimeMs,
			metric.MemoryUsageBytes, metric.GoroutineCount, metric.GoroutineLeak)
	}
}

func TestDynamicScalingTestNATS(t *testing.T) {
	// Load configuration
	config, err := LoadDynamicScalingTestConfig("configs/dynamic_scaling_test.json")
	if err != nil {
		t.Fatalf("Failed to load configuration: %v", err)
	}

	// Create test instance
	test := NewDynamicScalingTest(config)

	// Run test with NATS transporter
	result, err := test.Run("NATS")
	if err != nil {
		t.Fatalf("Dynamic scaling test failed: %v", err)
	}

	// Validate results
	if !result.Success {
		t.Errorf("Test was not successful")
	}

	// Log results
	t.Logf("Dynamic Scaling Test Results (NATS):")
	t.Logf("  Test Name: %s", result.TestName)
	t.Logf("  Total Duration: %.2f ms", result.TotalDurationMs)
	t.Logf("  Max Brokers Reached: %d", result.MaxBrokersReached)
	t.Logf("  Total Actions Executed: %d", result.TotalActionsExecuted)
	t.Logf("  Total Events Received: %d", result.TotalEventsReceived)
	t.Logf("  Peak Memory Usage: %d bytes", result.PeakMemoryUsage)
	t.Logf("  Peak Goroutine Count: %d", result.PeakGoroutineCount)
	t.Logf("  Final Goroutine Leak: %d", result.FinalGoroutineLeak)
}

func TestDynamicScalingTestAllTransporters(t *testing.T) {
	// Load configuration
	config, err := LoadDynamicScalingTestConfig("configs/dynamic_scaling_test.json")
	if err != nil {
		t.Fatalf("Failed to load configuration: %v", err)
	}

	// Test all transporters
	transporterTypes := []string{"TCP", "NATS", "Redis", "AMQP", "Kafka"}

	for _, transporterType := range transporterTypes {
		t.Run(transporterType, func(t *testing.T) {
			// Create test instance
			test := NewDynamicScalingTest(config)

			// Run test
			result, err := test.Run(transporterType)
			if err != nil {
				t.Logf("Dynamic scaling test failed for %s: %v", transporterType, err)
				return // Continue with other transporters
			}

			// Log results
			t.Logf("Dynamic Scaling Test Results (%s):", transporterType)
			t.Logf("  Success: %t", result.Success)
			t.Logf("  Total Duration: %.2f ms", result.TotalDurationMs)
			t.Logf("  Max Brokers Reached: %d", result.MaxBrokersReached)
			t.Logf("  Peak Memory Usage: %d bytes", result.PeakMemoryUsage)
			t.Logf("  Final Goroutine Leak: %d", result.FinalGoroutineLeak)
		})
	}
}

func TestDynamicScalingTestConfigValidation(t *testing.T) {
	// Test valid configuration
	config := &DynamicScalingTestConfig{
		TestName:         "Test Config Validation",
		TestDescription:  "Testing configuration validation",
		TransporterTypes: []string{"TCP", "NATS"},
		TransporterConfigs: map[string]interface{}{
			"TCP": map[string]interface{}{
				"type": "TCP",
				"port": 0,
			},
			"NATS": map[string]interface{}{
				"type": "NATS",
				"url":  "nats://localhost:4222",
			},
		},
		DynamicScalingConfig: DynamicScalingConfig{
			StartingBrokers:           2,
			MaxBrokers:                5,
			GrowthIntervalSeconds:     5,
			ReductionIntervalSeconds:  5,
			StabilizationPhaseSeconds: 10,
			EnableStabilizationPhase:  true,
		},
		ServiceConfig: ServiceConfig{
			ActionsPerService:    1,
			ActionName:           "action-0",
			ReturnPayloadSize:    1024,
			ParameterPayloadSize: 512,
			ExpectedEventCount:   1,
		},
		TestTimeoutSeconds: 300,
		LogLevel:           "INFO",
	}

	// Validate configuration
	err := config.Validate()
	if err != nil {
		t.Errorf("Valid configuration should not fail validation: %v", err)
	}

	// Test invalid configuration
	invalidConfig := &DynamicScalingTestConfig{
		TestName: "", // Invalid: empty test name
	}

	err = invalidConfig.Validate()
	if err == nil {
		t.Errorf("Invalid configuration should fail validation")
	}
}

func TestDynamicScalingTestMinimal(t *testing.T) {
	// Create minimal configuration for quick testing
	config := &DynamicScalingTestConfig{
		TestName:         "Minimal Dynamic Scaling Test",
		TestDescription:  "Minimal test for quick validation",
		TransporterTypes: []string{"TCP"},
		TransporterConfigs: map[string]interface{}{
			"TCP": map[string]interface{}{
				"type": "TCP",
				"port": 0,
			},
		},
		DynamicScalingConfig: DynamicScalingConfig{
			StartingBrokers:           2,
			MaxBrokers:                3, // Minimal scaling
			GrowthIntervalSeconds:     2, // Quick intervals
			ReductionIntervalSeconds:  2,
			StabilizationPhaseSeconds: 5,
			EnableStabilizationPhase:  false, // Skip stabilization for speed
		},
		ServiceConfig: ServiceConfig{
			ActionsPerService:    1,
			ActionName:           "action-0",
			ReturnPayloadSize:    512, // Smaller payloads
			ParameterPayloadSize: 256,
			ExpectedEventCount:   1,
		},
		TestTimeoutSeconds: 60,
		LogLevel:           "ERROR", // Less verbose logging
	}

	// Create test instance
	test := NewDynamicScalingTest(config)

	// Run test
	startTime := time.Now()
	result, err := test.Run("TCP")
	duration := time.Since(startTime)

	if err != nil {
		t.Fatalf("Minimal dynamic scaling test failed: %v", err)
	}

	// Validate results
	if !result.Success {
		t.Errorf("Minimal test was not successful")
	}

	// Log results
	t.Logf("Minimal Dynamic Scaling Test Results:")
	t.Logf("  Duration: %v", duration)
	t.Logf("  Total Duration: %.2f ms", result.TotalDurationMs)
	t.Logf("  Max Brokers Reached: %d", result.MaxBrokersReached)
	t.Logf("  Phases: %d", len(result.Phases))
	t.Logf("  Metrics: %d", len(result.Metrics))
}
