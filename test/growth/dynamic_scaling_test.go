package growth

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
	"github.com/moleculer-go/moleculer/payload"
	"github.com/moleculer-go/moleculer/transit/amqp"
	"github.com/moleculer-go/moleculer/transit/redis"
	log "github.com/sirupsen/logrus"
)

// Helper functions for extracting values from map[string]interface{}
func getStringFromMap(m map[string]interface{}, key, defaultValue string) string {
	if val, ok := m[key].(string); ok {
		return val
	}
	return defaultValue
}

func getIntFromMap(m map[string]interface{}, key string, defaultValue int) int {
	if val, ok := m[key].(float64); ok {
		return int(val)
	}
	return defaultValue
}

// DynamicScalingTestConfig represents the configuration for the dynamic scaling test
type DynamicScalingTestConfig struct {
	// Test Identity
	TestName        string `json:"test_name"`
	TestDescription string `json:"test_description"`

	// Transporter Configuration
	TransporterTypes   []string               `json:"transporter_types"`
	TransporterConfigs map[string]interface{} `json:"transporter_config"`

	// Dynamic Scaling Configuration
	DynamicScalingConfig DynamicScalingConfig `json:"dynamic_scaling_config"`

	// Service Configuration
	ServiceConfig ServiceConfig `json:"service_config"`

	// Test Execution
	TestTimeoutSeconds int    `json:"test_timeout_seconds"`
	LogLevel           string `json:"log_level"`
}

// DynamicScalingConfig represents the dynamic scaling configuration
type DynamicScalingConfig struct {
	StartingBrokers           int  `json:"starting_brokers"`
	MaxBrokers                int  `json:"max_brokers"`
	Cycles                    int  `json:"cycles"`
	GrowthIntervalSeconds     int  `json:"growth_interval_seconds"`
	ReductionIntervalSeconds  int  `json:"reduction_interval_seconds"`
	StabilizationPhaseSeconds int  `json:"stabilization_phase_seconds"`
	EnableStabilizationPhase  bool `json:"enable_stabilization_phase"`
}

// ServiceConfig represents the service configuration
type ServiceConfig struct {
	ActionsPerService    int    `json:"actions_per_service"`
	ActionName           string `json:"action_name"`
	ReturnPayloadSize    int    `json:"return_payload_size"`
	ParameterPayloadSize int    `json:"parameter_payload_size"`
	ExpectedEventCount   int    `json:"expected_event_count"`
}

// ScalingPhase represents a phase in the dynamic scaling test
type ScalingPhase struct {
	PhaseName         string    `json:"phase_name"`
	BrokerCount       int       `json:"broker_count"`
	ActionChainLength int       `json:"action_chain_length"`
	StartTime         time.Time `json:"start_time"`
	EndTime           time.Time `json:"end_time"`
	DurationMs        float64   `json:"duration_ms"`
}

// ScalingMetrics represents metrics for a specific scaling phase
type ScalingMetrics struct {
	PhaseName         string  `json:"phase_name"`
	BrokerCount       int     `json:"broker_count"`
	ActionChainLength int     `json:"action_chain_length"`
	ExecutionTimeMs   float64 `json:"execution_time_ms"`
	MemoryUsageBytes  uint64  `json:"memory_usage_bytes"`
	GoroutineCount    int     `json:"goroutine_count"`
	GoroutineLeak     int     `json:"goroutine_leak"`
	ActionsExecuted   int     `json:"actions_executed"`
	EventsReceived    int     `json:"events_received"`
	ValidationSuccess bool    `json:"validation_success"`
}

// DynamicScalingTestResult represents the complete test result
type DynamicScalingTestResult struct {
	// Test Identity
	TestName        string    `json:"test_name"`
	TestDescription string    `json:"test_description"`
	Timestamp       time.Time `json:"timestamp"`

	// Configuration
	Configuration *DynamicScalingTestConfig `json:"configuration"`

	// Execution Results
	Success bool  `json:"success"`
	Error   error `json:"error,omitempty"`

	// Phases
	Phases []ScalingPhase `json:"phases"`

	// Metrics
	Metrics []ScalingMetrics `json:"metrics"`

	// Summary
	TotalDurationMs      float64 `json:"total_duration_ms"`
	MaxBrokersReached    int     `json:"max_brokers_reached"`
	TotalActionsExecuted int     `json:"total_actions_executed"`
	TotalEventsReceived  int     `json:"total_events_received"`
	PeakMemoryUsage      uint64  `json:"peak_memory_usage"`
	PeakGoroutineCount   int     `json:"peak_goroutine_count"`
	FinalGoroutineLeak   int     `json:"final_goroutine_leak"`
}

// DynamicScalingTest represents the main test structure
type DynamicScalingTest struct {
	config             *DynamicScalingTestConfig
	brokers            []*broker.ServiceBroker
	transporterType    string
	phases             []ScalingPhase
	metrics            []ScalingMetrics
	actionResults      []UnifiedActionResult
	eventResults       []UnifiedActionResult
	aggregatedEvents   map[string][]interface{}
	aggregatorMap      map[int]string
	eventsMutex        sync.RWMutex
	outputDir          string
	currentBrokerCount int
	currentCycle       int
	actionChainConfig  map[string]UnifiedActionCallConfig
}

// UnifiedActionCallConfig represents the configuration for action calls (reused from unified test)
type UnifiedActionCallConfig struct {
	Actions              []string `json:"actions"`
	ReturnPayloadSize    int      `json:"return_payload_size"`
	ParameterPayloadSize int      `json:"parameter_payload_size"`
	ExpectedResultCount  int      `json:"expected_result_count"`
	ExpectedEventCount   int      `json:"expected_event_count"`
}

// UnifiedActionResult represents the result of an action call (reused from unified test)
type UnifiedActionResult struct {
	ServiceName string        `json:"service_name"`
	ActionName  string        `json:"action_name"`
	Result      interface{}   `json:"result"`
	Error       error         `json:"error,omitempty"`
	Timestamp   time.Time     `json:"timestamp"`
	Duration    time.Duration `json:"duration"`
}

// NewDynamicScalingTest creates a new dynamic scaling test instance
func NewDynamicScalingTest(config *DynamicScalingTestConfig) *DynamicScalingTest {
	return &DynamicScalingTest{
		config:             config,
		brokers:            make([]*broker.ServiceBroker, 0),
		phases:             make([]ScalingPhase, 0),
		metrics:            make([]ScalingMetrics, 0),
		actionResults:      make([]UnifiedActionResult, 0),
		eventResults:       make([]UnifiedActionResult, 0),
		aggregatedEvents:   make(map[string][]interface{}),
		aggregatorMap:      make(map[int]string),
		outputDir:          "test_results",
		currentBrokerCount: config.DynamicScalingConfig.StartingBrokers,
		actionChainConfig:  make(map[string]UnifiedActionCallConfig),
	}
}

// LoadDynamicScalingTestConfig loads a configuration from a JSON file
func LoadDynamicScalingTestConfig(configPath string) (*DynamicScalingTestConfig, error) {
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %v", configPath, err)
	}

	var config DynamicScalingTestConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %v", configPath, err)
	}

	// Set defaults
	if config.TestTimeoutSeconds == 0 {
		config.TestTimeoutSeconds = 600 // 10 minutes default
	}
	if config.LogLevel == "" {
		config.LogLevel = "INFO"
	}

	// Set log level for the test framework
	switch strings.ToUpper(config.LogLevel) {
	case "DEBUG":
		log.SetLevel(log.DebugLevel)
	case "INFO":
		log.SetLevel(log.InfoLevel)
	case "WARN", "WARNING":
		log.SetLevel(log.WarnLevel)
	case "ERROR":
		log.SetLevel(log.ErrorLevel)
	default:
		log.SetLevel(log.InfoLevel)
	}
	if config.DynamicScalingConfig.StartingBrokers == 0 {
		config.DynamicScalingConfig.StartingBrokers = 2
	}
	if config.DynamicScalingConfig.MaxBrokers == 0 {
		config.DynamicScalingConfig.MaxBrokers = 10
	}
	if config.DynamicScalingConfig.GrowthIntervalSeconds == 0 {
		config.DynamicScalingConfig.GrowthIntervalSeconds = 5
	}
	if config.DynamicScalingConfig.ReductionIntervalSeconds == 0 {
		config.DynamicScalingConfig.ReductionIntervalSeconds = 5
	}
	if config.ServiceConfig.ActionsPerService == 0 {
		config.ServiceConfig.ActionsPerService = 1
	}
	if config.ServiceConfig.ActionName == "" {
		config.ServiceConfig.ActionName = "action-0"
	}
	if config.ServiceConfig.ReturnPayloadSize == 0 {
		config.ServiceConfig.ReturnPayloadSize = 1024
	}
	if config.ServiceConfig.ParameterPayloadSize == 0 {
		config.ServiceConfig.ParameterPayloadSize = 512
	}

	return &config, nil
}

// ValidateConfig validates the configuration
func (config *DynamicScalingTestConfig) Validate() error {
	if config.TestName == "" {
		return fmt.Errorf("test_name is required")
	}
	if len(config.TransporterTypes) == 0 {
		return fmt.Errorf("transporter_types is required and cannot be empty")
	}
	if config.DynamicScalingConfig.StartingBrokers <= 0 {
		return fmt.Errorf("starting_brokers must be greater than 0")
	}
	if config.DynamicScalingConfig.MaxBrokers <= config.DynamicScalingConfig.StartingBrokers {
		return fmt.Errorf("max_brokers must be greater than starting_brokers")
	}
	if config.DynamicScalingConfig.GrowthIntervalSeconds <= 0 {
		return fmt.Errorf("growth_interval_seconds must be greater than 0")
	}
	if config.DynamicScalingConfig.ReductionIntervalSeconds <= 0 {
		return fmt.Errorf("reduction_interval_seconds must be greater than 0")
	}

	// Validate transporter types
	validTransporters := map[string]bool{
		"Memory": true,
		"TCP":    true,
		"NATS":   true,
		"Redis":  true,
		"AMQP":   true,
		"Kafka":  true,
	}

	for _, transporterType := range config.TransporterTypes {
		if !validTransporters[transporterType] {
			return fmt.Errorf("invalid transporter type: %s", transporterType)
		}
	}

	return nil
}

// Run executes the dynamic scaling test
func (dst *DynamicScalingTest) Run(transporterType string) (*DynamicScalingTestResult, error) {
	log.Debug("DynamicScalingTest.Run() method called")
	dst.transporterType = transporterType

	// Ensure cleanup always happens, even if test panics
	defer func() {
		if r := recover(); r != nil {
			log.WithField("panic", r).Error("Test panicked, cleaning up resources")
			dst.cleanup()
			panic(r) // Re-panic after cleanup
		}
	}()

	log.WithFields(log.Fields{
		"test_name":          dst.config.TestName,
		"transporter_type":   transporterType,
		"starting_brokers":   dst.config.DynamicScalingConfig.StartingBrokers,
		"max_brokers":        dst.config.DynamicScalingConfig.MaxBrokers,
		"growth_interval":    dst.config.DynamicScalingConfig.GrowthIntervalSeconds,
		"reduction_interval": dst.config.DynamicScalingConfig.ReductionIntervalSeconds,
	}).Info("Starting dynamic scaling test")

	// Validate configuration
	if err := dst.config.Validate(); err != nil {
		log.WithError(err).Error("Configuration validation failed")
		return nil, fmt.Errorf("configuration validation failed: %v", err)
	}

	// Create test result
	result := &DynamicScalingTestResult{
		TestName:        dst.config.TestName,
		TestDescription: dst.config.TestDescription,
		Timestamp:       time.Now(),
		Configuration:   dst.config,
		Success:         false,
	}

	// Run multiple cycles
	cycles := dst.config.DynamicScalingConfig.Cycles
	if cycles <= 0 {
		cycles = 1 // Default to 1 cycle if not specified
	}

	log.WithField("cycles", cycles).Info("Running multiple scaling cycles")

	for cycle := 1; cycle <= cycles; cycle++ {
		log.WithField("cycle", cycle).Info("Starting scaling cycle")

		// Phase 1: Initial Setup (only for first cycle)
		if cycle == 1 {
			log.Debug("About to call runInitialSetupPhase for cycle 1")
			if err := dst.runInitialSetupPhase(); err != nil {
				log.WithError(err).Error("Initial setup phase failed")
				result.Error = err
				return result, err
			}
			log.Debug("runInitialSetupPhase completed for cycle 1")
		} else {
			// For subsequent cycles, reset to starting brokers
			if err := dst.resetToStartingBrokers(); err != nil {
				log.WithError(err).Error("Failed to reset to starting brokers")
				result.Error = err
				dst.cleanup()
				return result, err
			}
		}

		// Set cycle context for phase logging
		dst.currentCycle = cycle

		// Phase 2: Dynamic Growth
		if err := dst.runGrowthPhase(); err != nil {
			log.WithError(err).Error("Growth phase failed")
			result.Error = err
			dst.cleanup()
			return result, err
		}

		// Phase 3: Stabilization (optional)
		if dst.config.DynamicScalingConfig.EnableStabilizationPhase {
			if err := dst.runStabilizationPhase(); err != nil {
				log.WithError(err).Error("Stabilization phase failed")
				result.Error = err
				dst.cleanup()
				return result, err
			}
		}

		// Phase 4: Dynamic Reduction
		if err := dst.runReductionPhase(); err != nil {
			log.WithError(err).Error("Reduction phase failed")
			result.Error = err
			dst.cleanup()
			return result, err
		}

		// Add cycle completion logging
		log.WithFields(log.Fields{
			"cycle":        dst.currentCycle,
			"broker_count": dst.currentBrokerCount,
		}).Info("Cycle completed successfully")

		log.WithField("cycle", cycle).Info("Completed scaling cycle")
	}

	// Generate final results
	log.WithFields(log.Fields{
		"phases_count":   len(dst.phases),
		"metrics_count":  len(dst.metrics),
		"action_results": len(dst.actionResults),
	}).Debug("Generating final results")

	result.Phases = dst.phases
	result.Metrics = dst.metrics
	result.Success = true
	result.TotalDurationMs = dst.calculateTotalDuration()
	result.MaxBrokersReached = dst.config.DynamicScalingConfig.MaxBrokers
	result.TotalActionsExecuted = len(dst.actionResults)
	// Count total events received across all aggregators
	totalEvents := 0
	dst.eventsMutex.RLock()
	for _, events := range dst.aggregatedEvents {
		totalEvents += len(events)
	}
	dst.eventsMutex.RUnlock()
	result.TotalEventsReceived = totalEvents
	result.PeakMemoryUsage = dst.calculatePeakMemoryUsage()
	result.PeakGoroutineCount = dst.calculatePeakGoroutineCount()
	result.FinalGoroutineLeak = dst.calculateFinalGoroutineLeak()

	log.WithFields(log.Fields{
		"result_phases_count":  len(result.Phases),
		"result_metrics_count": len(result.Metrics),
		"total_actions":        result.TotalActionsExecuted,
		"total_events":         result.TotalEventsReceived,
	}).Debug("Final results generated")

	// Save results to JSON file
	if err := dst.saveResultsToJSON(result); err != nil {
		log.WithError(err).Warn("Failed to save results to JSON file")
	}

	// Cleanup resources after results are generated
	dst.cleanup()

	return result, nil
}

// runInitialSetupPhase implements Phase 1: Initial Setup
func (dst *DynamicScalingTest) runInitialSetupPhase() error {
	phase := ScalingPhase{
		PhaseName:         fmt.Sprintf("Initial Setup (Cycle %d)", dst.currentCycle),
		BrokerCount:       dst.config.DynamicScalingConfig.StartingBrokers,
		ActionChainLength: dst.config.DynamicScalingConfig.StartingBrokers,
		StartTime:         time.Now(),
	}

	log.Info("Phase 1: Starting initial setup phase")

	// Create initial brokers
	log.Info("About to create initial brokers")
	if err := dst.createInitialBrokers(); err != nil {
		log.WithField("error", err).Error("Failed to create initial brokers")
		return fmt.Errorf("failed to create initial brokers: %v", err)
	}
	log.Info("Initial brokers created successfully")

	// Initialize action chain configuration
	log.Info("Initializing action chain configuration")
	dst.initializeActionChainConfig()
	log.Info("Action chain configuration initialized")

	// Run initial performance test
	log.Info("About to run initial performance test")
	metrics, err := dst.runPerformanceTest("Initial Setup")
	if err != nil {
		log.WithField("error", err).Error("Failed to run initial performance test")
		return fmt.Errorf("failed to run initial performance test: %v", err)
	}
	log.WithField("metrics", metrics).Info("Initial performance test completed")

	phase.EndTime = time.Now()
	phase.DurationMs = float64(phase.EndTime.Sub(phase.StartTime).Nanoseconds()) / 1e6

	dst.phases = append(dst.phases, phase)
	dst.metrics = append(dst.metrics, *metrics)

	log.WithFields(log.Fields{
		"cycle":            dst.currentCycle,
		"phase":            phase.PhaseName,
		"broker_count":     phase.BrokerCount,
		"duration_ms":      phase.DurationMs,
		"actions_executed": metrics.ActionsExecuted,
	}).Info("Phase 1 completed")

	return nil
}

// resetToStartingBrokers resets the broker count to starting brokers for subsequent cycles
func (dst *DynamicScalingTest) resetToStartingBrokers() error {
	log.Info("Resetting to starting brokers for new cycle")

	// Capture initial goroutine count
	initialGoroutines := runtime.NumGoroutine()
	log.WithField("initial_goroutines", initialGoroutines).Info("Starting cycle reset")

	// Stop all brokers except the starting number with timeout
	startingBrokers := dst.config.DynamicScalingConfig.StartingBrokers
	for i := startingBrokers; i < len(dst.brokers); i++ {
		if dst.brokers[i] != nil {
			log.WithField("broker_index", i).Debug("Stopping broker for cycle reset")

			// Stop broker in a goroutine with timeout
			done := make(chan bool, 1)
			go func(b *broker.ServiceBroker) {
				b.Stop()
				done <- true
			}(dst.brokers[i])

			// Wait for stop with timeout
			select {
			case <-done:
				log.WithField("broker_index", i).Debug("Broker stopped successfully for cycle reset")
			case <-time.After(3 * time.Second):
				log.WithField("broker_index", i).Warn("Broker stop timed out during cycle reset")
			}

			dst.brokers[i] = nil // Clear the reference
		}
	}

	// Remove extra brokers from the slice
	dst.brokers = dst.brokers[:startingBrokers]
	dst.currentBrokerCount = startingBrokers

	// Reset action chain configuration
	dst.initializeActionChainConfig()

	// Clear action results for new cycle
	dst.actionResults = make([]UnifiedActionResult, 0)

	// Clear events for new cycle
	dst.eventsMutex.Lock()
	dst.aggregatedEvents = make(map[string][]interface{})
	dst.eventsMutex.Unlock()

	// Wait for goroutines to clean up
	log.Info("Waiting for goroutines to clean up after cycle reset")
	time.Sleep(2 * time.Second)

	// Force garbage collection multiple times
	for i := 0; i < 3; i++ {
		runtime.GC()
		time.Sleep(500 * time.Millisecond)
	}

	// Final goroutine count
	finalGoroutines := runtime.NumGoroutine()
	leakedGoroutines := finalGoroutines - initialGoroutines

	log.WithFields(log.Fields{
		"broker_count":       dst.currentBrokerCount,
		"initial_goroutines": initialGoroutines,
		"final_goroutines":   finalGoroutines,
		"leaked_goroutines":  leakedGoroutines,
	}).Info("Reset to starting brokers completed")

	return nil
}

// runGrowthPhase implements Phase 2: Dynamic Growth
func (dst *DynamicScalingTest) runGrowthPhase() error {
	log.Info("Phase 2: Starting dynamic growth phase")

	for dst.currentBrokerCount < dst.config.DynamicScalingConfig.MaxBrokers {
		// Add one broker
		if err := dst.addBroker(); err != nil {
			return fmt.Errorf("failed to add broker %d: %v", dst.currentBrokerCount, err)
		}

		// Update action chain configuration
		dst.updateActionChainConfig()

		// Run performance test
		phaseName := fmt.Sprintf("Growth to %d brokers", dst.currentBrokerCount)
		metrics, err := dst.runPerformanceTest(phaseName)
		if err != nil {
			return fmt.Errorf("failed to run performance test for %d brokers: %v", dst.currentBrokerCount, err)
		}

		// Record phase
		phase := ScalingPhase{
			PhaseName:         phaseName,
			BrokerCount:       dst.currentBrokerCount,
			ActionChainLength: dst.currentBrokerCount,
			StartTime:         time.Now().Add(-time.Duration(dst.config.DynamicScalingConfig.GrowthIntervalSeconds) * time.Second),
			EndTime:           time.Now(),
		}
		phase.DurationMs = float64(phase.EndTime.Sub(phase.StartTime).Nanoseconds()) / 1e6

		dst.phases = append(dst.phases, phase)
		dst.metrics = append(dst.metrics, *metrics)

		log.WithFields(log.Fields{
			"phase":            phaseName,
			"broker_count":     dst.currentBrokerCount,
			"duration_ms":      phase.DurationMs,
			"actions_executed": metrics.ActionsExecuted,
		}).Info("Growth phase completed")

		// Wait for next growth interval
		if dst.currentBrokerCount < dst.config.DynamicScalingConfig.MaxBrokers {
			log.WithField("wait_seconds", dst.config.DynamicScalingConfig.GrowthIntervalSeconds).Info("Waiting for next growth interval")
			time.Sleep(time.Duration(dst.config.DynamicScalingConfig.GrowthIntervalSeconds) * time.Second)
		}
	}

	log.Info("Phase 2: Dynamic growth phase completed")
	return nil
}

// runStabilizationPhase implements Phase 3: Stabilization
func (dst *DynamicScalingTest) runStabilizationPhase() error {
	phase := ScalingPhase{
		PhaseName:         "Stabilization",
		BrokerCount:       dst.currentBrokerCount,
		ActionChainLength: dst.currentBrokerCount,
		StartTime:         time.Now(),
	}

	log.Info("Phase 3: Starting stabilization phase")

	// Run extended performance test to check for delayed leaks
	metrics, err := dst.runPerformanceTest("Stabilization")
	if err != nil {
		return fmt.Errorf("failed to run stabilization performance test: %v", err)
	}

	phase.EndTime = time.Now()
	phase.DurationMs = float64(phase.EndTime.Sub(phase.StartTime).Nanoseconds()) / 1e6

	dst.phases = append(dst.phases, phase)
	dst.metrics = append(dst.metrics, *metrics)

	log.WithFields(log.Fields{
		"phase":            phase.PhaseName,
		"broker_count":     phase.BrokerCount,
		"duration_ms":      phase.DurationMs,
		"actions_executed": metrics.ActionsExecuted,
	}).Info("Phase 3 completed")

	return nil
}

// runReductionPhase implements Phase 4: Dynamic Reduction
func (dst *DynamicScalingTest) runReductionPhase() error {
	log.Info("Phase 4: Starting dynamic reduction phase")

	for dst.currentBrokerCount > dst.config.DynamicScalingConfig.StartingBrokers {
		// Remove highest-index broker
		if err := dst.removeBroker(); err != nil {
			return fmt.Errorf("failed to remove broker %d: %v", dst.currentBrokerCount, err)
		}

		// Update action chain configuration
		dst.updateActionChainConfig()

		// Run performance test
		phaseName := fmt.Sprintf("Reduction to %d brokers", dst.currentBrokerCount)
		metrics, err := dst.runPerformanceTest(phaseName)
		if err != nil {
			return fmt.Errorf("failed to run performance test for %d brokers: %v", dst.currentBrokerCount, err)
		}

		// Record phase
		phase := ScalingPhase{
			PhaseName:         phaseName,
			BrokerCount:       dst.currentBrokerCount,
			ActionChainLength: dst.currentBrokerCount,
			StartTime:         time.Now().Add(-time.Duration(dst.config.DynamicScalingConfig.ReductionIntervalSeconds) * time.Second),
			EndTime:           time.Now(),
		}
		phase.DurationMs = float64(phase.EndTime.Sub(phase.StartTime).Nanoseconds()) / 1e6

		dst.phases = append(dst.phases, phase)
		dst.metrics = append(dst.metrics, *metrics)

		log.WithFields(log.Fields{
			"phase":            phaseName,
			"broker_count":     dst.currentBrokerCount,
			"duration_ms":      phase.DurationMs,
			"actions_executed": metrics.ActionsExecuted,
		}).Info("Reduction phase completed")

		// Wait for next reduction interval
		if dst.currentBrokerCount > dst.config.DynamicScalingConfig.StartingBrokers {
			log.WithField("wait_seconds", dst.config.DynamicScalingConfig.ReductionIntervalSeconds).Info("Waiting for next reduction interval")
			time.Sleep(time.Duration(dst.config.DynamicScalingConfig.ReductionIntervalSeconds) * time.Second)
		}
	}

	log.Info("Phase 4: Dynamic reduction phase completed")
	return nil
}

// createInitialBrokers creates the initial set of brokers
func (dst *DynamicScalingTest) createInitialBrokers() error {
	log.Info("Creating initial brokers")

	// Determine aggregator placement
	dst.determineAggregatorPlacement()

	// Create brokers
	for i := 0; i < dst.config.DynamicScalingConfig.StartingBrokers; i++ {
		broker := dst.createBroker(i, dst.transporterType)
		dst.brokers = append(dst.brokers, broker)
	}

	// Start all brokers in separate goroutines to avoid deadlocks
	var wg sync.WaitGroup
	for i, bkr := range dst.brokers {
		wg.Add(1)
		go func(brokerIndex int, broker *broker.ServiceBroker) {
			defer wg.Done()
			log.WithField("broker_index", brokerIndex).Debug("Starting broker in goroutine")
			broker.Start()
			log.WithField("broker_index", brokerIndex).Debug("Broker started successfully in goroutine")
		}(i, bkr)
	}
	wg.Wait()

	log.Info("All brokers started successfully in separate goroutines")

	// Wait for service discovery to complete
	log.Info("Waiting for service discovery to complete across all brokers")
	time.Sleep(1 * time.Second)

	// Log all broker node IDs
	for i, bkr := range dst.brokers {
		log.WithFields(log.Fields{
			"broker_index": i,
			"node_id":      bkr.LocalNode().GetID(),
		}).Debug("Broker node ID")
	}

	// Wait for all nodes to be discovered - this ensures all brokers know about each other
	allNodeIDs := make([]string, 0, len(dst.brokers))
	for i, bkr := range dst.brokers {
		nodeID := bkr.LocalNode().GetID()
		allNodeIDs = append(allNodeIDs, nodeID)
		log.WithFields(log.Fields{
			"broker_index": i,
			"node_id":      nodeID,
		}).Debug("Broker node ID")
	}

	// Use WaitForNodes to ensure all brokers know about each other
	log.Info("Waiting for all brokers to discover each other using WaitForNodes")
	for i, bkr := range dst.brokers {
		// Each broker should wait for all other brokers
		otherNodeIDs := make([]string, 0)
		for j, otherBkr := range dst.brokers {
			if i != j {
				otherNodeIDs = append(otherNodeIDs, otherBkr.LocalNode().GetID())
			}
		}

		if len(otherNodeIDs) > 0 {
			log.WithFields(log.Fields{
				"broker_index": i,
				"waiting_for":  otherNodeIDs,
			}).Debug("Broker waiting for other nodes")

			// Use a timeout for WaitForNodes to prevent hanging
			done := make(chan error, 1)
			go func() {
				done <- bkr.WaitForNodes(otherNodeIDs...)
			}()

			select {
			case err := <-done:
				if err != nil {
					log.WithFields(log.Fields{
						"broker_index": i,
						"error":        err,
					}).Warn("WaitForNodes failed, but continuing")
				} else {
					log.WithFields(log.Fields{
						"broker_index": i,
						"discovered":   otherNodeIDs,
					}).Debug("Broker successfully discovered other nodes")
				}
			case <-time.After(5 * time.Second):
				log.WithFields(log.Fields{
					"broker_index": i,
					"timeout":      "5s",
				}).Warn("WaitForNodes timed out, but continuing")
			}
		}
	}

	log.WithFields(log.Fields{
		"total_brokers": len(dst.brokers),
		"node_ids":      allNodeIDs,
	}).Info("All brokers started and discovered each other")

	// Now publish services to all brokers after they are started
	log.Info("Publishing services to all brokers")
	for i, bkr := range dst.brokers {
		// Add service to this broker
		serviceName := fmt.Sprintf("service-%d", i)
		dst.addServiceToBroker(bkr, serviceName, i)

		// Add event aggregator service if this broker is designated as an aggregator
		if aggregatorName, isAggregator := dst.aggregatorMap[i]; isAggregator {
			dst.addEventAggregatorService(bkr, aggregatorName, i)
		}
	}

	log.Info("All services published to brokers")

	// Wait for all services to be discovered by all brokers
	log.Info("Waiting for all services to be discovered by all brokers")
	time.Sleep(1 * time.Second) // Give services time to be discovered
	log.Info("All services should be discovered by all brokers")

	// Wait for service discovery to complete after publishing services
	log.Info("Waiting for service discovery to complete after publishing services")

	// Wait for all services to be discovered by all brokers
	for i, bkr := range dst.brokers {
		// Wait for all other brokers' services to be discovered
		otherBrokerServices := make([]string, 0)
		for j := 0; j < len(dst.brokers); j++ {
			if i != j {
				otherBrokerServices = append(otherBrokerServices, fmt.Sprintf("service-%d.action-0", j))
			}
		}

		if len(otherBrokerServices) > 0 {
			log.WithFields(log.Fields{
				"broker_index":         i,
				"waiting_for_services": otherBrokerServices,
			}).Debug("Broker waiting for other services to be discovered")

			// Wait for services to be discovered
			for _, serviceName := range otherBrokerServices {
				if err := bkr.WaitForActions(serviceName); err != nil {
					log.WithFields(log.Fields{
						"broker_index": i,
						"service":      serviceName,
						"error":        err,
					}).Warn("Failed to discover service, but continuing")
				} else {
					log.WithFields(log.Fields{
						"broker_index": i,
						"service":      serviceName,
					}).Debug("Service discovered successfully")
				}
			}
		}
	}

	log.Info("Service discovery completed")
	log.Info("Initial brokers created and started")
	return nil
}

// addBroker adds a new broker to the system
func (dst *DynamicScalingTest) addBroker() error {
	brokerIndex := dst.currentBrokerCount
	log.WithField("broker_index", brokerIndex).Info("Adding new broker")

	// Create new broker
	newBroker := dst.createBroker(brokerIndex, dst.transporterType)
	dst.brokers = append(dst.brokers, newBroker)

	// Start the broker in a goroutine and wait for it to start
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.WithField("broker_index", brokerIndex).Debug("Starting new broker in goroutine")
		newBroker.Start()
		log.WithField("broker_index", brokerIndex).Debug("New broker started successfully in goroutine")
	}()

	// Wait for the broker to start
	wg.Wait()
	log.WithField("broker_index", brokerIndex).Info("New broker started, now waiting for discovery")

	// Verify the new broker is discovered by checking node IDs
	newNodeID := newBroker.LocalNode().GetID()
	log.WithFields(log.Fields{
		"broker_index": brokerIndex,
		"node_id":      newNodeID,
	}).Info("New broker started and should be discovered")

	// Use WaitForNodes to ensure the new broker discovers all existing brokers
	existingNodeIDs := make([]string, 0)
	for i, bkr := range dst.brokers {
		if i != brokerIndex { // Don't include the new broker itself
			existingNodeIDs = append(existingNodeIDs, bkr.LocalNode().GetID())
		}
	}

	if len(existingNodeIDs) > 0 {
		log.WithFields(log.Fields{
			"broker_index": brokerIndex,
			"waiting_for":  existingNodeIDs,
		}).Debug("New broker waiting for existing nodes")

		// Use a timeout for WaitForNodes to prevent hanging
		done := make(chan error, 1)
		go func() {
			done <- newBroker.WaitForNodes(existingNodeIDs...)
		}()

		select {
		case err := <-done:
			if err != nil {
				log.WithFields(log.Fields{
					"broker_index": brokerIndex,
					"error":        err,
				}).Warn("WaitForNodes failed for new broker, but continuing")
			} else {
				log.WithFields(log.Fields{
					"broker_index": brokerIndex,
					"discovered":   existingNodeIDs,
				}).Debug("New broker successfully discovered existing nodes")
			}
		case <-time.After(5 * time.Second):
			log.WithFields(log.Fields{
				"broker_index": brokerIndex,
				"timeout":      "5s",
			}).Warn("WaitForNodes timed out for new broker, but continuing")
		}
	}

	// Also ensure existing brokers discover the new broker
	for i, bkr := range dst.brokers {
		if i != brokerIndex { // Don't include the new broker itself
			log.WithFields(log.Fields{
				"existing_broker": i,
				"waiting_for":     newNodeID,
			}).Debug("Existing broker waiting for new node")

			// Use a timeout for WaitForNodes to prevent hanging
			done := make(chan error, 1)
			go func() {
				done <- bkr.WaitForNodes(newNodeID)
			}()

			select {
			case err := <-done:
				if err != nil {
					log.WithFields(log.Fields{
						"existing_broker": i,
						"error":           err,
					}).Warn("WaitForNodes failed for existing broker, but continuing")
				} else {
					log.WithFields(log.Fields{
						"existing_broker": i,
						"discovered":      newNodeID,
					}).Debug("Existing broker successfully discovered new node")
				}
			case <-time.After(5 * time.Second):
				log.WithFields(log.Fields{
					"existing_broker": i,
					"timeout":         "5s",
				}).Warn("WaitForNodes timed out for existing broker, but continuing")
			}
		}
	}

	// Publish services to the new broker after it's started
	log.Info("Publishing services to new broker")
	serviceName := fmt.Sprintf("service-%d", brokerIndex)
	dst.addServiceToBroker(newBroker, serviceName, brokerIndex)

	// Add event aggregator service if this broker is designated as an aggregator
	if aggregatorName, isAggregator := dst.aggregatorMap[brokerIndex]; isAggregator {
		dst.addEventAggregatorService(newBroker, aggregatorName, brokerIndex)
	}

	// Wait for the new service to be discovered by all brokers
	actionName := fmt.Sprintf("%s.action-0", serviceName)
	log.WithField("action", actionName).Info("Waiting for new service to be discovered by all brokers")

	// Give new broker time to discover existing services
	log.WithField("new_broker", brokerIndex).Debug("Giving new broker time to discover existing services")
	time.Sleep(500 * time.Millisecond)

	// Give existing brokers time to discover the new service
	log.WithField("action", actionName).Debug("Giving existing brokers time to discover new service")
	time.Sleep(500 * time.Millisecond)

	dst.currentBrokerCount++
	log.WithField("broker_count", dst.currentBrokerCount).Info("Broker added successfully")
	return nil
}

// removeBroker removes the highest-index broker from the system
func (dst *DynamicScalingTest) removeBroker() error {
	if len(dst.brokers) == 0 {
		return fmt.Errorf("no brokers to remove")
	}

	brokerIndex := len(dst.brokers) - 1
	log.WithField("broker_index", brokerIndex).Info("Removing broker")

	// Stop the broker
	brokerToRemove := dst.brokers[brokerIndex]
	brokerToRemove.Stop()

	// Remove from slice
	dst.brokers = dst.brokers[:brokerIndex]

	dst.currentBrokerCount--
	log.WithField("broker_count", dst.currentBrokerCount).Info("Broker removed successfully")
	return nil
}

// initializeActionChainConfig initializes the action chain configuration for initial brokers
func (dst *DynamicScalingTest) initializeActionChainConfig() {
	log.Info("Initializing action chain configuration")

	// Create linear action chain: service-0.action-0 -> service-1.action-0 -> ... -> service-N.action-0
	for i := 0; i < dst.config.DynamicScalingConfig.StartingBrokers; i++ {
		actionName := fmt.Sprintf("service-%d.action-0", i)

		var nextActions []string
		if i < dst.config.DynamicScalingConfig.StartingBrokers-1 {
			nextActions = []string{fmt.Sprintf("service-%d.action-0", i+1)}
		}

		dst.actionChainConfig[actionName] = UnifiedActionCallConfig{
			Actions:              nextActions,
			ReturnPayloadSize:    dst.config.ServiceConfig.ReturnPayloadSize,
			ParameterPayloadSize: dst.config.ServiceConfig.ParameterPayloadSize,
			ExpectedResultCount:  1,
			ExpectedEventCount:   dst.config.ServiceConfig.ExpectedEventCount,
		}
	}

	log.WithField("action_chain_length", len(dst.actionChainConfig)).Info("Action chain configuration initialized")
}

// updateActionChainConfig updates the action chain configuration when brokers are added/removed
func (dst *DynamicScalingTest) updateActionChainConfig() {
	log.WithField("broker_count", dst.currentBrokerCount).Info("Updating action chain configuration")

	// Clear existing configuration
	dst.actionChainConfig = make(map[string]UnifiedActionCallConfig)

	// Create linear action chain for current broker count
	for i := 0; i < dst.currentBrokerCount; i++ {
		actionName := fmt.Sprintf("service-%d.action-0", i)

		var nextActions []string
		if i < dst.currentBrokerCount-1 {
			nextActions = []string{fmt.Sprintf("service-%d.action-0", i+1)}
		}

		dst.actionChainConfig[actionName] = UnifiedActionCallConfig{
			Actions:              nextActions,
			ReturnPayloadSize:    dst.config.ServiceConfig.ReturnPayloadSize,
			ParameterPayloadSize: dst.config.ServiceConfig.ParameterPayloadSize,
			ExpectedResultCount:  1,
			ExpectedEventCount:   dst.config.ServiceConfig.ExpectedEventCount,
		}
	}

	log.WithField("action_chain_length", len(dst.actionChainConfig)).Info("Action chain configuration updated")
}

// runPerformanceTest runs a performance test for the current broker configuration
func (dst *DynamicScalingTest) runPerformanceTest(phaseName string) (*ScalingMetrics, error) {
	log.WithField("phase", phaseName).Info("Running performance test")

	// Capture initial memory stats
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	initialGoroutines := runtime.NumGoroutine()

	startTime := time.Now()

	// Give actions time to be discovered before executing
	log.Info("Giving actions time to be discovered before executing action chain")
	time.Sleep(500 * time.Millisecond)

	// Execute action chain
	rootAction := "service-0.action-0"
	log.WithFields(log.Fields{
		"phase":        phaseName,
		"root_action":  rootAction,
		"action_chain": dst.actionChainConfig,
		"broker_count": dst.currentBrokerCount,
	}).Info("Executing action chain")

	log.WithFields(log.Fields{
		"phase":               phaseName,
		"action_chain_config": dst.actionChainConfig,
	}).Debug("About to execute action chain")

	result, err := dst.executeActionChain(rootAction)

	log.WithFields(log.Fields{
		"phase":  phaseName,
		"result": result,
		"error":  err,
	}).Debug("Action chain execution completed")
	if err != nil {
		return nil, fmt.Errorf("failed to execute action chain: %v", err)
	}

	log.WithFields(log.Fields{
		"phase":            phaseName,
		"result":           result,
		"actions_executed": len(dst.actionResults),
	}).Info("Action chain execution completed")

	executionTime := time.Since(startTime)

	// Capture final memory stats
	runtime.ReadMemStats(&m)
	finalMemory := m.HeapAlloc
	finalGoroutines := runtime.NumGoroutine()

	// Count total events received across all aggregators
	totalEvents := 0
	dst.eventsMutex.RLock()
	for _, events := range dst.aggregatedEvents {
		totalEvents += len(events)
	}
	dst.eventsMutex.RUnlock()

	// Create metrics
	metrics := &ScalingMetrics{
		PhaseName:         phaseName,
		BrokerCount:       dst.currentBrokerCount,
		ActionChainLength: len(dst.actionChainConfig),
		ExecutionTimeMs:   float64(executionTime.Nanoseconds()) / 1e6,
		MemoryUsageBytes:  finalMemory,
		GoroutineCount:    finalGoroutines,
		GoroutineLeak:     finalGoroutines - initialGoroutines,
		ActionsExecuted:   len(dst.actionResults),
		EventsReceived:    totalEvents,
		ValidationSuccess: result != nil,
	}

	log.WithFields(log.Fields{
		"phase":             phaseName,
		"broker_count":      metrics.BrokerCount,
		"execution_time_ms": metrics.ExecutionTimeMs,
		"memory_usage":      metrics.MemoryUsageBytes,
		"goroutine_leak":    metrics.GoroutineLeak,
		"actions_executed":  metrics.ActionsExecuted,
	}).Info("Performance test completed")

	return metrics, nil
}

// executeActionChain executes the complete action chain
func (dst *DynamicScalingTest) executeActionChain(actionName string) (interface{}, error) {
	log.WithField("action", actionName).Debug("Executing action chain")

	// Get action configuration
	actionConfig, exists := dst.actionChainConfig[actionName]
	if !exists {
		return nil, fmt.Errorf("no configuration found for action %s", actionName)
	}

	// Find broker for this action
	broker := dst.findBrokerForAction(actionName)
	if broker == nil {
		return nil, fmt.Errorf("no broker found for action %s", actionName)
	}

	// Create payload
	actionPayload := map[string]interface{}{
		"config":       dst.actionChainConfig,
		"payload_data": make([]byte, 1024),
	}

	// Create metadata
	metadata := map[string]interface{}{
		"service_name": strings.Split(actionName, ".")[0],
		"action_name":  strings.Split(actionName, ".")[1],
	}

	// Execute action
	log.WithFields(log.Fields{
		"action": actionName,
		"broker": broker.LocalNode().GetID(),
	}).Debug("About to call action")

	result := <-broker.Call(actionName, actionPayload, moleculer.Options{Meta: payload.New(metadata)})

	log.WithFields(log.Fields{
		"action":   actionName,
		"is_error": result.IsError(),
		"error":    result.Error(),
		"value":    result.Value(),
	}).Debug("Action call completed")

	if result.IsError() {
		return nil, fmt.Errorf("action %s failed: %v", actionName, result.Error())
	}

	// Store action result
	dst.actionResults = append(dst.actionResults, UnifiedActionResult{
		ServiceName: strings.Split(actionName, ".")[0],
		ActionName:  strings.Split(actionName, ".")[1],
		Result:      result.Value(),
		Timestamp:   time.Now(),
	})

	log.WithFields(log.Fields{
		"action":       actionName,
		"result_count": len(dst.actionResults),
		"result":       result.Value(),
	}).Debug("Action result stored")

	// If no sub-actions, return result
	if len(actionConfig.Actions) == 0 {
		log.WithFields(log.Fields{
			"action":      actionName,
			"sub_actions": actionConfig.Actions,
		}).Debug("No sub-actions to execute")
		return result.Value(), nil
	}

	// Execute sub-actions
	log.WithFields(log.Fields{
		"action":      actionName,
		"sub_actions": actionConfig.Actions,
	}).Debug("Executing sub-actions")

	subResults := make([]interface{}, 0)
	for _, subAction := range actionConfig.Actions {
		log.WithFields(log.Fields{
			"parent_action": actionName,
			"sub_action":    subAction,
		}).Debug("Calling sub-action")

		subResult, err := dst.executeActionChain(subAction)
		if err != nil {
			log.WithFields(log.Fields{
				"parent_action": actionName,
				"sub_action":    subAction,
				"error":         err,
			}).Error("Sub-action failed")
			return nil, fmt.Errorf("sub-action %s failed: %v", subAction, err)
		}
		subResults = append(subResults, subResult)
	}

	// Combine results
	combinedResult := map[string]interface{}{
		"action_name": actionName,
		"result":      result.Value(),
		"sub_results": subResults,
	}

	return combinedResult, nil
}

// findBrokerForAction finds which broker has the given action
func (dst *DynamicScalingTest) findBrokerForAction(actionName string) *broker.ServiceBroker {
	serviceName := strings.Split(actionName, ".")[0]

	// Extract service number from service name (e.g., "service-5" -> 5)
	var serviceNumber int
	fmt.Sscanf(serviceName, "service-%d", &serviceNumber)

	// Find broker with matching service
	for i, broker := range dst.brokers {
		if i == serviceNumber {
			return broker
		}
	}

	return nil
}

// determineAggregatorPlacement determines which brokers should have event aggregators
func (dst *DynamicScalingTest) determineAggregatorPlacement() {
	dst.aggregatorMap = make(map[int]string)

	if dst.currentBrokerCount < 3 {
		// For small broker counts, use the second broker (index 1)
		if dst.currentBrokerCount >= 2 {
			aggregatorName := fmt.Sprintf("event-aggregator-%d", 1)
			dst.aggregatorMap[1] = aggregatorName
		}
	} else {
		// For larger broker counts, use every 3rd broker starting from index 2
		for i := 2; i < dst.currentBrokerCount; i += 3 {
			aggregatorName := fmt.Sprintf("event-aggregator-%d", i)
			dst.aggregatorMap[i] = aggregatorName
		}
	}
}

// createBroker creates a broker with the specified transporter (reused from unified test)
func (dst *DynamicScalingTest) createBroker(index int, transporterType string) *broker.ServiceBroker {
	log.WithFields(log.Fields{
		"broker_index":     index,
		"transporter_type": transporterType,
	}).Debug("Creating broker")

	// Create broker config
	brokerConfig := &moleculer.Config{
		LogLevel:                   dst.config.LogLevel,
		WaitForDependenciesTimeout: 30 * time.Second,
		RequestTimeout:             60 * time.Second,
	}

	// Get transporter configuration from JSON file
	transporterConfigInterface, exists := dst.config.TransporterConfigs[transporterType]
	if !exists {
		log.Warn(fmt.Sprintf("No configuration found for transporter %s, using default", transporterType))
		brokerConfig.Transporter = transporterType
		return broker.New(brokerConfig)
	}

	// Type assert to map[string]interface{}
	transporterConfig, ok := transporterConfigInterface.(map[string]interface{})
	if !ok {
		log.Warn(fmt.Sprintf("Invalid configuration format for transporter %s, using default", transporterType))
		brokerConfig.Transporter = transporterType
		return broker.New(brokerConfig)
	}

	// Configure transporter based on type
	switch transporterType {
	case "TCP":
		brokerConfig.Transporter = "TCP"
	case "NATS":
		if url, ok := transporterConfig["url"].(string); ok {
			brokerConfig.Transporter = url
		} else {
			brokerConfig.Transporter = "nats://localhost:4222"
		}
	case "Redis":
		brokerConfig.TransporterFactory = func() interface{} {
			redisConfig := &redis.RedisConfig{
				Host:     getStringFromMap(transporterConfig, "host", "localhost"),
				Port:     getIntFromMap(transporterConfig, "port", 6379),
				Password: getStringFromMap(transporterConfig, "password", ""),
				DB:       getIntFromMap(transporterConfig, "db", 2),
				Prefix:   getStringFromMap(transporterConfig, "prefix", "test-moleculer"),
			}
			return redis.NewRedisTransporter(redisConfig)
		}
	case "AMQP":
		brokerConfig.TransporterFactory = func() interface{} {
			url := getStringFromMap(transporterConfig, "url", "amqp://localhost:5672")
			amqpConfig := amqp.AmqpOptions{
				Url: []string{url},
				Logger: log.WithFields(log.Fields{
					"Unit Test": true,
					"transport": "amqp",
				}),
			}
			return amqp.CreateAmqpTransporter(amqpConfig)
		}
	case "Kafka":
		if brokers, ok := transporterConfig["brokers"].([]interface{}); ok && len(brokers) > 0 {
			if broker, ok := brokers[0].(string); ok {
				brokerConfig.Transporter = fmt.Sprintf("kafka://%s", broker)
			} else {
				brokerConfig.Transporter = "kafka://localhost:9092"
			}
		} else {
			brokerConfig.Transporter = "kafka://localhost:9092"
		}
	default:
		brokerConfig.Transporter = dst.transporterType
	}

	// Create broker
	bkr := broker.New(brokerConfig)

	// Store broker index for later service publishing
	// Services will be published after broker is started
	return bkr
}

// addServiceToBroker adds a service to a broker (reused from unified test)
func (dst *DynamicScalingTest) addServiceToBroker(bkr *broker.ServiceBroker, serviceName string, brokerIndex int) {
	log.WithFields(log.Fields{
		"broker_index":  brokerIndex,
		"service_name":  serviceName,
		"actions_count": dst.config.ServiceConfig.ActionsPerService,
	}).Trace("Creating service")

	// Create actions for this service
	actions := make([]moleculer.Action, 0)
	for i := 0; i < dst.config.ServiceConfig.ActionsPerService; i++ {
		actionName := fmt.Sprintf("action-%d", i)
		actions = append(actions, moleculer.Action{
			Name: actionName,
			Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
				return dst.genericAction(ctx, params)
			},
		})
	}

	// Create service schema
	serviceSchema := moleculer.ServiceSchema{
		Name:    serviceName,
		Actions: actions,
		Events:  []moleculer.Event{},
	}

	// Publish service
	bkr.Publish(serviceSchema)
}

// addEventAggregatorService adds an event aggregator service to a broker (reused from unified test)
func (dst *DynamicScalingTest) addEventAggregatorService(bkr *broker.ServiceBroker, serviceName string, brokerIndex int) {
	log.WithFields(log.Fields{
		"broker_index":    brokerIndex,
		"aggregator_name": serviceName,
	}).Debug("Adding event aggregator service")

	// Get events this aggregator should listen to
	eventsToListen := dst.getEventsToListenTo(brokerIndex)

	log.WithFields(log.Fields{
		"aggregator_name":  serviceName,
		"events_to_listen": eventsToListen,
	}).Debug("Setting up event aggregator with automatic event detection")

	// Initialize the events list for this aggregator
	dst.aggregatedEvents[serviceName] = make([]interface{}, 0)

	// Create event handlers for each event this aggregator should listen to
	eventHandlers := make([]moleculer.Event, 0)
	log.WithFields(log.Fields{
		"aggregator":       serviceName,
		"events_to_listen": eventsToListen,
		"event_count":      len(eventsToListen),
	}).Debug("🔧 Creating event handlers for aggregator")

	for _, eventName := range eventsToListen {
		// Create a closure to capture the serviceName and eventName
		eventHandler := func(aggregatorName, eventName string) func(moleculer.Context, moleculer.Payload) {
			return func(eventCtx moleculer.Context, eventParams moleculer.Payload) {
				eventData := map[string]interface{}{
					"event_name":    eventName,
					"aggregator":    aggregatorName,
					"timestamp":     time.Now().UnixNano(),
					"payload":       eventParams.RawMap(),
					"received_from": "event-source",
				}

				// Store the event in the aggregator's collection (thread-safe)
				dst.eventsMutex.Lock()
				dst.aggregatedEvents[aggregatorName] = append(dst.aggregatedEvents[aggregatorName], eventData)
				dst.eventsMutex.Unlock()

				log.WithFields(log.Fields{
					"aggregator":   aggregatorName,
					"event_name":   eventName,
					"total_events": len(dst.aggregatedEvents[aggregatorName]),
				}).Trace("Event received and stored")
			}
		}(serviceName, eventName)

		eventHandlers = append(eventHandlers, moleculer.Event{
			Name:    eventName,
			Handler: eventHandler,
		})
	}

	// Create event aggregator service
	serviceSchema := moleculer.ServiceSchema{
		Name: serviceName,
		Actions: []moleculer.Action{
			{
				Name: "get-aggregated-events",
				Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
					return dst.handleGetAggregatedEvents(ctx, params, serviceName)
				},
			},
		},
		Events: eventHandlers,
	}

	// Publish service
	bkr.Publish(serviceSchema)
}

// getEventsToListenTo automatically determines which events an aggregator should listen to
// Each aggregator listens to all events from services that are NOT on its own broker
func (dst *DynamicScalingTest) getEventsToListenTo(aggregatorBrokerIndex int) []string {
	eventsToListen := make([]string, 0)

	// For dynamic scaling, we listen to all service events from other brokers
	// Since we have services 0 to currentBrokerCount-1, we listen to all of them
	for i := 0; i < dst.currentBrokerCount; i++ {
		if i != aggregatorBrokerIndex {
			// Each service has one action that emits events
			eventName := fmt.Sprintf("service-%d.action-0.called", i)
			eventsToListen = append(eventsToListen, eventName)
		}
	}

	log.WithFields(log.Fields{
		"aggregator_broker": aggregatorBrokerIndex,
		"events_to_listen":  eventsToListen,
		"total_events":      len(eventsToListen),
	}).Debug("Determined events for aggregator to listen to")

	return eventsToListen
}

// genericAction is the generic action function (reused from unified test)
func (dst *DynamicScalingTest) genericAction(context moleculer.Context, params moleculer.Payload) interface{} {
	// Get service/action identity from metadata
	meta := context.Meta()
	serviceName := meta.Get("service_name").String()
	actionName := meta.Get("action_name").String()
	actionKey := fmt.Sprintf("%s.%s", serviceName, actionName)

	log.WithFields(log.Fields{
		"service_name": serviceName,
		"action_name":  actionName,
		"action_key":   actionKey,
		"params":       params.String(),
	}).Error("🚨 GENERIC ACTION CALLED - This should only happen during performance test!")

	// Get configuration for this action from params
	config := params.Get("config").RawMap()
	actionConfig, exists := config[actionKey]
	if !exists {
		log.Error(fmt.Sprintf("No configuration found for action %s", actionKey))
		return []interface{}{}
	}

	// Convert actionConfig to UnifiedActionCallConfig
	var configStruct UnifiedActionCallConfig
	if configMap, ok := actionConfig.(map[string]interface{}); ok {
		if actions, ok := configMap["actions"].([]interface{}); ok {
			configStruct.Actions = make([]string, len(actions))
			for i, action := range actions {
				if actionStr, ok := action.(string); ok {
					configStruct.Actions[i] = actionStr
				}
			}
		}
		if returnPayloadSize, ok := configMap["return_payload_size"].(float64); ok {
			configStruct.ReturnPayloadSize = int(returnPayloadSize)
		}
		if parameterPayloadSize, ok := configMap["parameter_payload_size"].(float64); ok {
			configStruct.ParameterPayloadSize = int(parameterPayloadSize)
		}
		if expectedResultCount, ok := configMap["expected_result_count"].(float64); ok {
			configStruct.ExpectedResultCount = int(expectedResultCount)
		}
		if expectedEventCount, ok := configMap["expected_event_count"].(float64); ok {
			configStruct.ExpectedEventCount = int(expectedEventCount)
		}
	} else if config, ok := actionConfig.(UnifiedActionCallConfig); ok {
		configStruct = config
	} else {
		log.Error(fmt.Sprintf("Unexpected type for action %s: %T", actionKey, actionConfig))
		return []interface{}{}
	}

	// Create return payload
	returnPayloadSize := configStruct.ReturnPayloadSize
	returnPayload := make([]byte, returnPayloadSize)
	for i := range returnPayload {
		returnPayload[i] = byte(time.Now().UnixNano() % 256)
	}

	// Emit event
	eventName := fmt.Sprintf("%s.%s.called", serviceName, actionName)
	eventData := map[string]interface{}{
		"action_name":  actionKey,
		"random_value": time.Now().UnixNano(),
		"payload_size": returnPayloadSize,
	}
	context.Emit(eventName, eventData)

	// Create result for this action
	actionResult := map[string]interface{}{
		"action_name":  actionKey,
		"random_value": time.Now().UnixNano(),
		"payload_size": returnPayloadSize,
		"payload":      returnPayload,
	}

	// Check if this action needs to call other actions
	subActions := configStruct.Actions
	if len(subActions) == 0 {
		return []interface{}{actionResult}
	}

	// Call all sub-actions and collect results
	var allResults []interface{}
	allResults = append(allResults, actionResult)

	for _, subActionStr := range subActions {
		// Create payload for sub-action
		parameterPayloadSize := configStruct.ParameterPayloadSize
		subPayload := map[string]interface{}{
			"config":       config,
			"payload_data": make([]byte, parameterPayloadSize),
		}

		// Create metadata for sub-action
		subMeta := map[string]interface{}{
			"service_name": strings.Split(subActionStr, ".")[0],
			"action_name":  strings.Split(subActionStr, ".")[1],
		}

		// Call sub-action with metadata
		subResult := <-context.Call(subActionStr, subPayload, moleculer.Options{Meta: payload.New(subMeta)})
		if subResult.IsError() {
			log.Error(fmt.Sprintf("Sub-action %s call failed: %v", subActionStr, subResult.Error()))
			continue
		}

		// Add sub-action results to our results
		if subResults, ok := subResult.Value().([]interface{}); ok {
			allResults = append(allResults, subResults...)
		}
	}

	return allResults
}

// handleGetAggregatedEvents handles the get-aggregated-events action (simplified version)
func (dst *DynamicScalingTest) handleGetAggregatedEvents(ctx moleculer.Context, params moleculer.Payload, serviceName string) interface{} {
	// Get the collected events for this aggregator (thread-safe)
	dst.eventsMutex.RLock()
	events, exists := dst.aggregatedEvents[serviceName]
	if !exists {
		events = []interface{}{}
	}
	// Create a copy to avoid holding the lock
	eventsCopy := make([]interface{}, len(events))
	copy(eventsCopy, events)
	dst.eventsMutex.RUnlock()

	return map[string]interface{}{
		"status":       "success",
		"events":       eventsCopy,
		"events_count": len(eventsCopy),
		"aggregator":   serviceName,
	}
}

// Helper methods for calculating summary metrics
func (dst *DynamicScalingTest) calculateTotalDuration() float64 {
	if len(dst.phases) == 0 {
		return 0
	}

	firstPhase := dst.phases[0]
	lastPhase := dst.phases[len(dst.phases)-1]

	return float64(lastPhase.EndTime.Sub(firstPhase.StartTime).Nanoseconds()) / 1e6
}

func (dst *DynamicScalingTest) calculatePeakMemoryUsage() uint64 {
	var peak uint64
	for _, metric := range dst.metrics {
		if metric.MemoryUsageBytes > peak {
			peak = metric.MemoryUsageBytes
		}
	}
	return peak
}

func (dst *DynamicScalingTest) calculatePeakGoroutineCount() int {
	var peak int
	for _, metric := range dst.metrics {
		if metric.GoroutineCount > peak {
			peak = metric.GoroutineCount
		}
	}
	return peak
}

func (dst *DynamicScalingTest) calculateFinalGoroutineLeak() int {
	if len(dst.metrics) == 0 {
		return 0
	}
	return dst.metrics[len(dst.metrics)-1].GoroutineLeak
}

// cleanup stops all brokers and cleans up resources
func (dst *DynamicScalingTest) cleanup() {
	log.Info("Starting aggressive cleanup phase - stopping all brokers")

	// Capture initial goroutine count
	initialGoroutines := runtime.NumGoroutine()
	log.WithField("initial_goroutines", initialGoroutines).Info("Starting cleanup")

	// Stop all brokers with timeout
	for i, bkr := range dst.brokers {
		if bkr != nil {
			log.WithField("broker_index", i).Debug("Stopping broker")

			// Stop broker in a goroutine with timeout
			done := make(chan bool, 1)
			go func(b *broker.ServiceBroker) {
				b.Stop()
				done <- true
			}(bkr)

			// Wait for stop with timeout
			select {
			case <-done:
				log.WithField("broker_index", i).Debug("Broker stopped successfully")
			case <-time.After(5 * time.Second):
				log.WithField("broker_index", i).Warn("Broker stop timed out")
			}
		}
	}

	// Clear brokers slice
	dst.brokers = nil

	// Clear other resources
	dst.actionResults = nil
	dst.eventResults = nil
	dst.phases = nil
	dst.metrics = nil

	// Clear events
	dst.eventsMutex.Lock()
	dst.aggregatedEvents = nil
	dst.eventsMutex.Unlock()

	// Wait for goroutines to clean up
	log.Info("Waiting for goroutines to clean up")
	time.Sleep(2 * time.Second)

	// Force garbage collection multiple times with longer delays
	for i := 0; i < 5; i++ {
		runtime.GC()
		time.Sleep(500 * time.Millisecond)
	}

	// Final goroutine count
	finalGoroutines := runtime.NumGoroutine()
	leakedGoroutines := finalGoroutines - initialGoroutines

	log.WithFields(log.Fields{
		"initial_goroutines": initialGoroutines,
		"final_goroutines":   finalGoroutines,
		"leaked_goroutines":  leakedGoroutines,
	}).Info("Cleanup phase completed - all brokers stopped and resources cleared")
}

// saveResultsToJSON saves the test results to a JSON file
func (dst *DynamicScalingTest) saveResultsToJSON(result *DynamicScalingTestResult) error {
	// Create output directory if it doesn't exist
	if err := os.MkdirAll(dst.outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory %s: %v", dst.outputDir, err)
	}

	// Generate filename with timestamp and transporter type
	timestamp := result.Timestamp.Format("20060102_150405")
	filename := fmt.Sprintf("dynamic_scaling_test_%s_%s_%s.json",
		strings.ReplaceAll(result.TestName, " ", "_"),
		dst.transporterType,
		timestamp)

	filepath := filepath.Join(dst.outputDir, filename)

	// Create the file
	file, err := os.Create(filepath)
	if err != nil {
		return fmt.Errorf("failed to create JSON file %s: %v", filepath, err)
	}
	defer file.Close()

	// Create JSON encoder with indentation
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")

	// Encode the result
	if err := encoder.Encode(result); err != nil {
		return fmt.Errorf("failed to encode JSON to file %s: %v", filepath, err)
	}

	log.WithFields(log.Fields{
		"filepath":         filepath,
		"transporter_type": dst.transporterType,
		"test_name":        result.TestName,
		"success":          result.Success,
		"total_duration":   result.TotalDurationMs,
		"max_brokers":      result.MaxBrokersReached,
	}).Info("Dynamic scaling test results saved to JSON file")

	return nil
}
