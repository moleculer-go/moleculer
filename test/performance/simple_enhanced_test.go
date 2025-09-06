package performance

import (
	"testing"
	"time"
)

// TestSimpleEnhanced tests a simple enhanced performance scenario
func TestSimpleEnhanced(t *testing.T) {
	t.Log("Starting simple enhanced performance test")

	// Create test result writer
	resultWriter := NewTestResultWriter("./test_results")
	defer resultWriter.SaveResults()

	// Create test settings
	testSettings := CreateTestSettings(
		"Memory",
		map[string]interface{}{"type": "memory"},
		2,              // broker count
		3,              // services per broker
		3,              // actions per service
		3,              // events per service
		10*time.Second, // test duration
		1,              // concurrency level
		50*1024*1024,   // memory threshold (50MB)
		50,             // goroutine threshold
	)
	testSettings.AddOtherSetting("test_type", "simple_enhanced")

	// Simulate test execution
	startTime := time.Now()

	// Simulate some work
	time.Sleep(100 * time.Millisecond)

	duration := time.Since(startTime)

	// Create test result
	testResult := CreateTestResult("TestSimpleEnhanced", "Memory", true, duration, nil)
	testResult.AddTestSettings(testSettings)
	testResult.AddMetric("simulated_calls", 100)
	testResult.AddMetric("simulated_events", 50)
	testResult.AddMetric("success_rate", 1.0)

	resultWriter.AddResult(testResult)

	t.Logf("Simple enhanced test completed:")
	t.Logf("  Duration: %v", duration)
	t.Logf("  Success: true")
	t.Logf("  Simulated calls: 100")
	t.Logf("  Simulated events: 50")
}

// TestEnhancedFrameworkBasics tests the basic enhanced framework components
func TestEnhancedFrameworkBasics(t *testing.T) {
	t.Log("Testing enhanced framework basics")

	// Test that we can create test settings
	testSettings := CreateTestSettings(
		"TCP",
		map[string]interface{}{"host": "localhost", "port": 3000},
		4,              // broker count
		5,              // services per broker
		10,             // actions per service
		5,              // events per service
		30*time.Second, // test duration
		10,             // concurrency level
		100*1024*1024,  // memory threshold (100MB)
		100,            // goroutine threshold
	)

	if testSettings.TransporterType != "TCP" {
		t.Errorf("Expected transporter type TCP, got %s", testSettings.TransporterType)
	}

	if testSettings.BrokerCount != 4 {
		t.Errorf("Expected broker count 4, got %d", testSettings.BrokerCount)
	}

	// Test that we can create test results
	testResult := CreateTestResult("TestEnhancedFrameworkBasics", "TCP", true, time.Second, nil)
	testResult.AddTestSettings(testSettings)
	testResult.AddMetric("test_metric", 42)

	if testResult.TestName != "TestEnhancedFrameworkBasics" {
		t.Errorf("Expected test name TestEnhancedFrameworkBasics, got %s", testResult.TestName)
	}

	if testResult.Success != true {
		t.Errorf("Expected success true, got %v", testResult.Success)
	}

	// Test that we can create result writer
	resultWriter := NewTestResultWriter("./test_results")
	resultWriter.AddResult(testResult)

	// Save results
	err := resultWriter.SaveResults()
	if err != nil {
		t.Errorf("Failed to save results: %v", err)
	}

	t.Log("Enhanced framework basics test completed successfully")
}
