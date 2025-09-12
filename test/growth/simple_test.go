package growth

import (
	"testing"
)

func TestSimpleBrokerCreation(t *testing.T) {
	t.Log("TestSimpleBrokerCreation started")

	// Load configuration
	config, err := LoadDynamicScalingTestConfig("configs/dynamic_scaling_test.json")
	if err != nil {
		t.Fatalf("Failed to load configuration: %v", err)
	}

	// Create test instance
	test := NewDynamicScalingTest(config)
	test.transporterType = "TCP" // Set the transporter type
	t.Log("DynamicScalingTest instance created")

	// Test just creating brokers without complex discovery
	t.Log("About to create initial brokers")
	if err := test.createInitialBrokers(); err != nil {
		t.Fatalf("Failed to create initial brokers: %v", err)
	}
	t.Log("Initial brokers created successfully")

	// Initialize action chain configuration
	t.Log("Initializing action chain configuration")
	test.initializeActionChainConfig()
	t.Log("Action chain configuration initialized")

	// Test just running performance test
	t.Log("About to run performance test")
	metrics, err := test.runPerformanceTest("Simple Test")
	if err != nil {
		t.Fatalf("Failed to run performance test: %v", err)
	}
	t.Logf("Performance test completed: %+v", metrics)

	// Cleanup
	test.cleanup()
	t.Log("Test completed successfully")
}
