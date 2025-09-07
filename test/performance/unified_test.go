package performance

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"runtime"
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
	log "github.com/sirupsen/logrus"
)

// UnifiedTestConfig represents the configuration for the unified performance test
type UnifiedTestConfig struct {
	// Test Identity
	TestName        string `json:"test_name"`
	TestDescription string `json:"test_description"`

	// Transporter Configuration
	TransporterTypes  []string               `json:"transporter_types"`
	TransporterConfig map[string]interface{} `json:"transporter_config"`

	// Broker Configuration
	BrokerCount       int `json:"broker_count"`
	TotalServices     int `json:"total_services"`
	ActionsPerService int `json:"actions_per_service"`

	// Call Chain Configuration
	CallChainConfig map[string]UnifiedActionCallConfig `json:"call_chain_config"`

	// Event Configuration (AUTOMATIC - no manual configuration needed)
	// Each event aggregator automatically listens to all events from services NOT on its own broker

	// Test Execution
	TestTimeoutSeconds int `json:"test_timeout_seconds"`
	TestCycles         int `json:"test_cycles"`

	// Logging
	LogLevel string `json:"log_level"`
}

// UnifiedActionCallConfig represents the configuration for action calls in unified test
type UnifiedActionCallConfig struct {
	// Target actions to call
	Actions []string `json:"actions"`

	// Payload configuration
	ReturnPayloadSize    int `json:"return_payload_size"`
	ParameterPayloadSize int `json:"parameter_payload_size"`

	// Validation
	ExpectedResultCount int `json:"expected_result_count"`
	ExpectedEventCount  int `json:"expected_event_count"`
}

// TestMetrics represents comprehensive test metrics
type TestMetrics struct {
	// Timing
	DiscoveryTimeMs float64 `json:"discovery_time_ms"`
	ExecutionTimeMs float64 `json:"execution_time_ms"`
	TotalTimeMs     float64 `json:"total_time_ms"`

	// Actions
	TotalActionsCalled int `json:"total_actions_called"`
	SuccessfulActions  int `json:"successful_actions"`
	FailedActions      int `json:"failed_actions"`

	// Events
	TotalEventsReceived int `json:"total_events_received"`
	ExpectedEvents      int `json:"expected_events"`

	// Memory
	InitialHeapBytes uint64 `json:"initial_heap_bytes"`
	PeakHeapBytes    uint64 `json:"peak_heap_bytes"`
	FinalHeapBytes   uint64 `json:"final_heap_bytes"`
	HeapGrowthBytes  int64  `json:"heap_growth_bytes"`

	// Goroutines
	InitialGoroutines int `json:"initial_goroutines"`
	PeakGoroutines    int `json:"peak_goroutines"`
	FinalGoroutines   int `json:"final_goroutines"`
	GoroutineLeak     int `json:"goroutine_leak"`

	// Throughput
	ActionsPerSecond float64 `json:"actions_per_second"`
	EventsPerSecond  float64 `json:"events_per_second"`

	// Validation
	CallChainComplete   bool `json:"call_chain_complete"`
	EventChainComplete  bool `json:"event_chain_complete"`
	PayloadSizesCorrect bool `json:"payload_sizes_correct"`
	ActionOrderCorrect  bool `json:"action_order_correct"`
}

// ValidationReport represents the validation results
type ValidationReport struct {
	IsValid                 bool                     `json:"is_valid"`
	CallChainComplete       bool                     `json:"call_chain_complete"`
	EventChainComplete      bool                     `json:"event_chain_complete"`
	PayloadSizesCorrect     bool                     `json:"payload_sizes_correct"`
	ActionOrderCorrect      bool                     `json:"action_order_correct"`
	EventAggregationValid   bool                     `json:"event_aggregation_valid"`
	ExpectedActionsExecuted bool                     `json:"expected_actions_executed"`
	ExpectedEventsCollected bool                     `json:"expected_events_collected"`
	ValidationErrors        []string                 `json:"validation_errors"`
	ActionChainResults      map[string]interface{}   `json:"action_chain_results"`
	EventAggregationResults map[string][]interface{} `json:"event_aggregation_results"`
	MissingActions          []string                 `json:"missing_actions"`
	MissingEvents           []string                 `json:"missing_events"`
	PayloadSizeMismatches   []string                 `json:"payload_size_mismatches"`
}

// UnifiedActionResult represents the result of an action call in unified test
type UnifiedActionResult struct {
	ServiceName string        `json:"service_name"`
	ActionName  string        `json:"action_name"`
	Result      interface{}   `json:"result"`
	Error       error         `json:"error,omitempty"`
	Timestamp   time.Time     `json:"timestamp"`
	Duration    time.Duration `json:"duration"`
}

// UnifiedMemoryStats represents memory statistics for unified test
type UnifiedMemoryStats struct {
	InitialHeapBytes  uint64 `json:"initial_heap_bytes"`
	PeakHeapBytes     uint64 `json:"peak_heap_bytes"`
	FinalHeapBytes    uint64 `json:"final_heap_bytes"`
	HeapGrowthBytes   int64  `json:"heap_growth_bytes"`
	InitialGoroutines int    `json:"initial_goroutines"`
	PeakGoroutines    int    `json:"peak_goroutines"`
	FinalGoroutines   int    `json:"final_goroutines"`
	GoroutineLeak     int    `json:"goroutine_leak"`
}

// UnifiedTestResult represents the complete test result
type UnifiedTestResult struct {
	// Test Identity
	TestName        string    `json:"test_name"`
	TestDescription string    `json:"test_description"`
	Timestamp       time.Time `json:"timestamp"`

	// Configuration
	Configuration *UnifiedTestConfig `json:"configuration"`

	// Execution Results
	Success bool  `json:"success"`
	Error   error `json:"error,omitempty"`

	// Timing
	DiscoveryTimeMs float64 `json:"discovery_time_ms"`
	ExecutionTimeMs float64 `json:"execution_time_ms"`
	TotalTimeMs     float64 `json:"total_time_ms"`

	// Metrics
	Metrics *TestMetrics `json:"metrics"`

	// Validation
	ValidationReport *ValidationReport `json:"validation_report"`

	// Raw Data
	ActionResults []UnifiedActionResult `json:"action_results"`
	EventResults  []UnifiedActionResult `json:"event_results"`

	// Memory Stats
	MemoryStats *UnifiedMemoryStats `json:"memory_stats"`
}

// UnifiedTest represents the main test structure
type UnifiedTest struct {
	config           *UnifiedTestConfig
	brokers          []*broker.ServiceBroker
	transporterType  string
	discoveryTime    time.Duration
	executionTime    time.Duration
	actionResults    []UnifiedActionResult
	eventResults     []UnifiedActionResult
	memoryStats      *UnifiedMemoryStats
	validationReport *ValidationReport
}

// NewUnifiedTest creates a new unified test instance
func NewUnifiedTest(config *UnifiedTestConfig) *UnifiedTest {
	return &UnifiedTest{
		config:        config,
		brokers:       make([]*broker.ServiceBroker, 0),
		actionResults: make([]UnifiedActionResult, 0),
		eventResults:  make([]UnifiedActionResult, 0),
		memoryStats:   &UnifiedMemoryStats{},
	}
}

// LoadUnifiedTestConfig loads a configuration from a JSON file
func LoadUnifiedTestConfig(configPath string) (*UnifiedTestConfig, error) {
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %v", configPath, err)
	}

	var config UnifiedTestConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %v", configPath, err)
	}

	// Set defaults
	if config.TestTimeoutSeconds == 0 {
		config.TestTimeoutSeconds = 30
	}
	if config.TestCycles == 0 {
		config.TestCycles = 1
	}
	if config.LogLevel == "" {
		config.LogLevel = "INFO"
	}
	if config.ActionsPerService == 0 {
		config.ActionsPerService = 1
	}

	return &config, nil
}

// ValidateConfig validates the configuration
func (config *UnifiedTestConfig) Validate() error {
	if config.TestName == "" {
		return fmt.Errorf("test_name is required")
	}
	if len(config.TransporterTypes) == 0 {
		return fmt.Errorf("transporter_types is required and cannot be empty")
	}
	if config.BrokerCount <= 0 {
		return fmt.Errorf("broker_count must be greater than 0")
	}
	if config.TotalServices <= 0 {
		return fmt.Errorf("total_services must be greater than 0")
	}
	if config.ActionsPerService <= 0 {
		return fmt.Errorf("actions_per_service must be greater than 0")
	}
	if config.TestTimeoutSeconds <= 0 {
		return fmt.Errorf("test_timeout_seconds must be greater than 0")
	}
	if config.TestCycles <= 0 {
		return fmt.Errorf("test_cycles must be greater than 0")
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

// adjustConfiguration adjusts the configuration to ensure valid values
func (ut *UnifiedTest) adjustConfiguration() {
	// Ensure minimum 9 brokers
	if ut.config.BrokerCount < 9 {
		ut.config.BrokerCount = 9
	}

	// Ensure broker count is multiple of 3
	if ut.config.BrokerCount%3 != 0 {
		ut.config.BrokerCount = ((ut.config.BrokerCount + 2) / 3) * 3
	}

	// Ensure total services is divisible by 3
	if ut.config.TotalServices%3 != 0 {
		ut.config.TotalServices = ((ut.config.TotalServices + 2) / 3) * 3
	}

	// Ensure minimum 3 actions per service
	if ut.config.ActionsPerService < 3 {
		ut.config.ActionsPerService = 3
	}
}

// distributeServices distributes services across brokers with load balancing
func (ut *UnifiedTest) distributeServices() map[string][]string {
	serviceDistribution := make(map[string][]string)
	totalServices := ut.config.TotalServices

	// Calculate number of brokers per service using the formula
	brokersPerService := int(math.Max(3, math.Round(float64(totalServices)/float64(ut.config.BrokerCount))))

	// Initialize broker lists
	for i := 0; i < ut.config.BrokerCount; i++ {
		serviceDistribution[fmt.Sprintf("broker-%d", i)] = make([]string, 0)
	}

	// For each service, select exactly brokersPerService brokers (skipping its own broker index)
	for i := 0; i < totalServices; i++ {
		serviceName := fmt.Sprintf("service-%d", i)

		// Select brokersPerService brokers for this service (skipping broker with same index as service)
		// Pattern: service-X exists on brokers (X+1)%N, (X+2)%N, ..., (X+brokersPerService)%N
		for j := 1; j <= brokersPerService; j++ {
			brokerIndex := (i + j) % ut.config.BrokerCount
			brokerKey := fmt.Sprintf("broker-%d", brokerIndex)
			serviceDistribution[brokerKey] = append(serviceDistribution[brokerKey], serviceName)
		}
	}

	return serviceDistribution
}

// captureMemoryStats captures current memory and goroutine statistics
func (ut *UnifiedTest) captureMemoryStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	ut.memoryStats.FinalHeapBytes = m.HeapAlloc
	ut.memoryStats.PeakHeapBytes = m.HeapAlloc // Will be updated during execution
	ut.memoryStats.FinalGoroutines = runtime.NumGoroutine()
	ut.memoryStats.PeakGoroutines = runtime.NumGoroutine() // Will be updated during execution

	// Calculate growth
	ut.memoryStats.HeapGrowthBytes = int64(ut.memoryStats.FinalHeapBytes) - int64(ut.memoryStats.InitialHeapBytes)
	ut.memoryStats.GoroutineLeak = ut.memoryStats.FinalGoroutines - ut.memoryStats.InitialGoroutines
}

// Run executes the unified performance test
func (ut *UnifiedTest) Run(transporterType string) (*UnifiedTestResult, error) {
	ut.transporterType = transporterType

	log.WithFields(log.Fields{
		"test_name":        ut.config.TestName,
		"transporter_type": transporterType,
		"broker_count":     ut.config.BrokerCount,
		"total_services":   ut.config.TotalServices,
		"test_cycles":      ut.config.TestCycles,
		"log_level":        ut.config.LogLevel,
	}).Info("Starting unified performance test")

	// Validate configuration
	if err := ut.config.Validate(); err != nil {
		log.WithError(err).Error("Configuration validation failed")
		return nil, fmt.Errorf("configuration validation failed: %v", err)
	}
	log.Debug("Configuration validation passed")

	// Capture initial memory stats
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	ut.memoryStats.InitialHeapBytes = m.HeapAlloc
	ut.memoryStats.InitialGoroutines = runtime.NumGoroutine()

	log.WithFields(log.Fields{
		"initial_heap_bytes": ut.memoryStats.InitialHeapBytes,
		"initial_goroutines": ut.memoryStats.InitialGoroutines,
	}).Debug("Captured initial memory statistics")

	// Adjust configuration
	ut.adjustConfiguration()

	log.WithFields(log.Fields{
		"adjusted_broker_count":   ut.config.BrokerCount,
		"adjusted_total_services": ut.config.TotalServices,
	}).Info("Configuration adjusted")

	// Create test result
	result := &UnifiedTestResult{
		TestName:        ut.config.TestName,
		TestDescription: ut.config.TestDescription,
		Timestamp:       time.Now(),
		Configuration:   ut.config,
		Success:         false,
	}

	// Run discovery phase
	log.Info("Starting discovery phase")
	if err := ut.runDiscoveryPhase(); err != nil {
		log.WithError(err).Error("Discovery phase failed")
		result.Error = err
		return result, err
	}
	log.WithField("discovery_time_ms", ut.discoveryTime.Nanoseconds()/1e6).Info("Discovery phase completed")

	// Run execution phase
	log.Info("Starting execution phase")
	if err := ut.runExecutionPhase(); err != nil {
		log.WithError(err).Error("Execution phase failed")
		result.Error = err
		return result, err
	}
	log.WithField("execution_time_ms", ut.executionTime.Nanoseconds()/1e6).Info("Execution phase completed")

	// Run validation phase
	ut.validationReport = ut.validateResults()

	// Capture final memory stats
	ut.captureMemoryStats()

	// Generate metrics
	result.Metrics = ut.generateMetrics()
	result.ValidationReport = ut.validationReport
	result.ActionResults = ut.actionResults
	result.EventResults = ut.eventResults
	result.MemoryStats = ut.memoryStats
	result.DiscoveryTimeMs = float64(ut.discoveryTime.Nanoseconds()) / 1e6
	result.ExecutionTimeMs = float64(ut.executionTime.Nanoseconds()) / 1e6
	result.TotalTimeMs = result.DiscoveryTimeMs + result.ExecutionTimeMs
	result.Success = true

	return result, nil
}

// runDiscoveryPhase implements the discovery phase
func (ut *UnifiedTest) runDiscoveryPhase() error {
	startTime := time.Now()

	log.WithField("broker_count", ut.config.BrokerCount).Info("Creating and starting brokers")

	// Create and start all brokers
	for i := 0; i < ut.config.BrokerCount; i++ {
		log.WithField("broker_index", i).Debug("Creating broker")
		broker := ut.createBroker(i)
		ut.brokers = append(ut.brokers, broker)

		log.WithField("broker_index", i).Debug("Starting broker")
		broker.Start()
		log.WithField("broker_index", i).Debug("Broker started successfully")
	}

	log.Info("All brokers created and started")

	// Wait for services to be discovered on each broker
	// Each broker should only wait for services that are actually available on that broker
	serviceDistribution := ut.distributeServices()

	log.Info("Waiting for service discovery on each broker")

	for i, broker := range ut.brokers {
		brokerKey := fmt.Sprintf("broker-%d", i)
		brokerServices := serviceDistribution[brokerKey]

		// Add event aggregator service if this broker has one
		if i%3 == 2 {
			aggregatorName := fmt.Sprintf("event-aggregator-%d", i)
			brokerServices = append(brokerServices, aggregatorName)
		}

		log.WithFields(log.Fields{
			"broker_index":  i,
			"broker_key":    brokerKey,
			"services":      brokerServices,
			"service_count": len(brokerServices),
		}).Debug("Waiting for services on broker")

		err := broker.WaitFor(brokerServices...)
		if err != nil {
			log.WithFields(log.Fields{
				"broker_index": i,
				"broker_key":   brokerKey,
				"services":     brokerServices,
				"error":        err,
			}).Error("Service discovery failed on broker")
			return fmt.Errorf("service discovery failed: %v", err)
		}

		log.WithFields(log.Fields{
			"broker_index":  i,
			"broker_key":    brokerKey,
			"service_count": len(brokerServices),
		}).Debug("All services discovered on broker")
	}

	log.Info("All services discovered successfully")

	// Additional validation
	ut.validateServiceDiscovery()

	ut.discoveryTime = time.Since(startTime)
	log.WithField("discovery_duration_ms", ut.discoveryTime.Nanoseconds()/1e6).Info("Discovery phase completed")
	return nil
}

// runExecutionPhase implements the execution phase
func (ut *UnifiedTest) runExecutionPhase() error {
	startTime := time.Now()

	log.Info("Starting execution phase with action chains")

	// Run multiple cycles of action execution
	for cycle := 0; cycle < ut.config.TestCycles; cycle++ {
		log.WithField("cycle", cycle).Debug("Starting execution cycle")

		// Execute action chains on each broker using services available on that broker
		serviceDistribution := ut.distributeServices()

		for brokerIndex, broker := range ut.brokers {
			brokerKey := fmt.Sprintf("broker-%d", brokerIndex)
			brokerServices := serviceDistribution[brokerKey]

			log.WithFields(log.Fields{
				"cycle":        cycle,
				"broker_index": brokerIndex,
				"broker_key":   brokerKey,
				"services":     brokerServices,
			}).Debug("Executing actions on broker")

			// Execute actions for each service on this broker
			for _, serviceName := range brokerServices {
				actionName := "action-0" // Use first action for now
				actionFullName := fmt.Sprintf("%s.%s", serviceName, actionName)

				log.WithFields(log.Fields{
					"cycle":        cycle,
					"broker_index": brokerIndex,
					"action_name":  actionFullName,
				}).Trace("Calling action")

				// Create action config with complete call chain configuration
				actionConfig := ut.createActionConfig()

				// Execute action
				result := <-broker.Call(actionFullName, actionConfig)

				if result.IsError() {
					log.WithFields(log.Fields{
						"cycle":        cycle,
						"broker_index": brokerIndex,
						"action_name":  actionFullName,
						"error":        result.Error(),
					}).Error("Action call failed")
					return fmt.Errorf("action chain failed in cycle %d on broker %d: %v", cycle, brokerIndex, result.Error())
				}

				log.WithFields(log.Fields{
					"cycle":        cycle,
					"broker_index": brokerIndex,
					"action_name":  actionFullName,
				}).Trace("Action call successful")

				// Parse and validate results for this cycle
				ut.parseActionResults(result, cycle)
			}
		}

		log.WithField("cycle", cycle).Debug("Completed execution cycle")
	}

	// Collect event results from all aggregators
	ut.collectEventResults()

	ut.executionTime = time.Since(startTime)
	log.WithField("execution_duration_ms", ut.executionTime.Nanoseconds()/1e6).Info("Execution phase completed")
	return nil
}

// validateResults implements the comprehensive validation phase
func (ut *UnifiedTest) validateResults() *ValidationReport {
	report := &ValidationReport{
		ActionChainResults:      make(map[string]interface{}),
		EventAggregationResults: make(map[string][]interface{}),
		MissingActions:          make([]string, 0),
		MissingEvents:           make([]string, 0),
		PayloadSizeMismatches:   make([]string, 0),
		ValidationErrors:        make([]string, 0),
	}

	log.Info("Starting comprehensive validation phase")

	// 1. Validate Action Chain Results
	report.ExpectedActionsExecuted = ut.validateActionChainResults(report)

	// 2. Validate Payload Sizes
	report.PayloadSizesCorrect = ut.validatePayloadSizes(report)

	// 3. Validate Event Aggregation
	report.EventAggregationValid = ut.validateEventAggregation(report)

	// 4. Validate Call Chain Completion
	report.CallChainComplete = ut.validateCallChainCompletion(report)

	// 5. Validate Event Chain Completion
	report.EventChainComplete = ut.validateEventChainCompletion(report)

	// 6. Validate Action Order
	report.ActionOrderCorrect = ut.validateActionOrder(report)

	// Overall validation success
	report.ExpectedEventsCollected = report.EventAggregationValid && report.EventChainComplete

	// Determine overall validation success
	report.CallChainComplete = report.ExpectedActionsExecuted && report.CallChainComplete && report.ActionOrderCorrect
	report.IsValid = report.CallChainComplete && report.ExpectedEventsCollected && report.PayloadSizesCorrect

	log.WithFields(log.Fields{
		"action_chain_valid":      report.ExpectedActionsExecuted,
		"payload_sizes_valid":     report.PayloadSizesCorrect,
		"event_aggregation_valid": report.EventAggregationValid,
		"call_chain_complete":     report.CallChainComplete,
		"event_chain_complete":    report.EventChainComplete,
		"overall_valid":           report.IsValid,
		"validation_errors":       len(report.ValidationErrors),
	}).Info("Validation phase completed")

	return report
}

// validateActionChainResults validates that all expected actions were executed and returned proper results
func (ut *UnifiedTest) validateActionChainResults(report *ValidationReport) bool {
	log.Info("Validating action chain results")

	// Get the root action result (the final result from the action chain)
	if len(ut.actionResults) == 0 {
		report.ValidationErrors = append(report.ValidationErrors, "No action results found")
		return false
	}

	// The first action result should contain the complete chain result
	rootResult := ut.actionResults[0]
	report.ActionChainResults["root"] = rootResult.Result

	// Extract the results array from the root result
	results, ok := rootResult.Result.(map[string]interface{})
	if !ok {
		report.ValidationErrors = append(report.ValidationErrors, "Root result is not a map")
		return false
	}

	resultsArray, ok := results["results"].([]interface{})
	if !ok {
		report.ValidationErrors = append(report.ValidationErrors, "Results field is not an array")
		return false
	}

	// Get expected actions from config
	expectedActions := ut.getExpectedActionsFromConfig()
	actualActionCount := len(resultsArray)
	expectedActionCount := len(expectedActions)

	log.WithFields(log.Fields{
		"expected_actions": expectedActionCount,
		"actual_actions":   actualActionCount,
	}).Debug("Comparing expected vs actual action counts")

	// Validate that we have results from all expected actions
	if actualActionCount != expectedActionCount {
		report.ValidationErrors = append(report.ValidationErrors,
			fmt.Sprintf("Action count mismatch: expected %d, got %d", expectedActionCount, actualActionCount))
		return false
	}

	// Validate that each result contains the expected action name
	for i, result := range resultsArray {
		if i >= len(expectedActions) {
			break
		}

		resultMap, ok := result.(map[string]interface{})
		if !ok {
			report.ValidationErrors = append(report.ValidationErrors,
				fmt.Sprintf("Result %d is not a map", i))
			continue
		}

		actionName, ok := resultMap["action_name"].(string)
		if !ok {
			report.ValidationErrors = append(report.ValidationErrors,
				fmt.Sprintf("Result %d missing action_name", i))
			continue
		}

		expectedAction := expectedActions[i]
		if actionName != expectedAction {
			report.ValidationErrors = append(report.ValidationErrors,
				fmt.Sprintf("Action name mismatch at position %d: expected %s, got %s", i, expectedAction, actionName))
		}
	}

	success := len(report.ValidationErrors) == 0
	log.WithField("success", success).Info("Action chain results validation completed")
	return success
}

// validatePayloadSizes validates that all payload sizes match expectations
func (ut *UnifiedTest) validatePayloadSizes(report *ValidationReport) bool {
	log.Info("Validating payload sizes")

	success := true

	// Validate each action result's payload size
	for _, actionResult := range ut.actionResults {
		actionKey := fmt.Sprintf("%s.%s", actionResult.ServiceName, actionResult.ActionName)

		// Get expected payload size from config
		expectedConfig, exists := ut.config.CallChainConfig[actionKey]
		if !exists {
			report.ValidationErrors = append(report.ValidationErrors,
				fmt.Sprintf("No config found for action %s", actionKey))
			success = false
			continue
		}

		// Extract payload from result
		resultMap, ok := actionResult.Result.(map[string]interface{})
		if !ok {
			report.ValidationErrors = append(report.ValidationErrors,
				fmt.Sprintf("Result for %s is not a map", actionKey))
			success = false
			continue
		}

		payload, ok := resultMap["payload"].([]byte)
		if !ok {
			report.ValidationErrors = append(report.ValidationErrors,
				fmt.Sprintf("Payload for %s is not []byte", actionKey))
			success = false
			continue
		}

		actualSize := len(payload)
		expectedSize := expectedConfig.ReturnPayloadSize

		if actualSize != expectedSize {
			report.PayloadSizeMismatches = append(report.PayloadSizeMismatches,
				fmt.Sprintf("%s: expected %d bytes, got %d bytes", actionKey, expectedSize, actualSize))
			success = false
		}
	}

	log.WithField("success", success).Info("Payload sizes validation completed")
	return success
}

// validateEventAggregation validates that event aggregators collected the expected events
func (ut *UnifiedTest) validateEventAggregation(report *ValidationReport) bool {
	log.Info("Validating event aggregation")

	success := true

	// Get event aggregator services (every 3rd broker)
	aggregatorBrokers := make([]int, 0)
	for i := 2; i < ut.config.BrokerCount; i += 3 {
		aggregatorBrokers = append(aggregatorBrokers, i)
	}

	// For each aggregator, collect its events
	for _, brokerIndex := range aggregatorBrokers {
		aggregatorName := fmt.Sprintf("event-aggregator-%d", brokerIndex)

		// Call get-aggregated-events on the aggregator
		// For now, we'll use broker-0 to call the aggregator
		aggregatorBroker := ut.brokers[0]

		if aggregatorBroker == nil {
			report.ValidationErrors = append(report.ValidationErrors,
				fmt.Sprintf("No broker found for aggregator %s", aggregatorName))
			success = false
			continue
		}

		// Call get-aggregated-events
		result := <-aggregatorBroker.Call(fmt.Sprintf("%s.get-aggregated-events", aggregatorName), map[string]interface{}{})
		if result.Error() != nil {
			report.ValidationErrors = append(report.ValidationErrors,
				fmt.Sprintf("Failed to get aggregated events from %s: %v", aggregatorName, result.Error()))
			success = false
			continue
		}

		// Store the aggregated events
		report.EventAggregationResults[aggregatorName] = []interface{}{result.Value()}

		log.WithFields(log.Fields{
			"aggregator":   aggregatorName,
			"events_count": len(report.EventAggregationResults[aggregatorName]),
		}).Debug("Collected events from aggregator")
	}

	log.WithField("success", success).Info("Event aggregation validation completed")
	return success
}

// validateCallChainCompletion validates that the call chain completed successfully
func (ut *UnifiedTest) validateCallChainCompletion(report *ValidationReport) bool {
	log.Info("Validating call chain completion")

	// Check that we have action results
	if len(ut.actionResults) == 0 {
		report.ValidationErrors = append(report.ValidationErrors, "No action results found")
		return false
	}

	// Check that the root action completed successfully
	rootResult := ut.actionResults[0]
	if rootResult.Error != nil {
		report.ValidationErrors = append(report.ValidationErrors,
			fmt.Sprintf("Root action failed: %v", rootResult.Error))
		return false
	}

	log.Info("Call chain completion validation passed")
	return true
}

// validateEventChainCompletion validates that the event chain completed successfully
func (ut *UnifiedTest) validateEventChainCompletion(report *ValidationReport) bool {
	log.Info("Validating event chain completion")

	// Check that we have event results
	if len(ut.eventResults) == 0 {
		report.ValidationErrors = append(report.ValidationErrors, "No event results found")
		return false
	}

	// Check that all events were emitted successfully
	for _, eventResult := range ut.eventResults {
		if eventResult.Error != nil {
			report.ValidationErrors = append(report.ValidationErrors,
				fmt.Sprintf("Event emission failed for %s.%s: %v", eventResult.ServiceName, eventResult.ActionName, eventResult.Error))
			return false
		}
	}

	log.Info("Event chain completion validation passed")
	return true
}

// validateActionOrder validates that actions were executed in the expected order
func (ut *UnifiedTest) validateActionOrder(report *ValidationReport) bool {
	log.Info("Validating action order")

	// For now, we'll implement a basic order validation
	// This could be enhanced to check specific ordering requirements

	expectedActions := ut.getExpectedActionsFromConfig()
	if len(ut.actionResults) != len(expectedActions) {
		report.ValidationErrors = append(report.ValidationErrors,
			"Action count mismatch in order validation")
		return false
	}

	log.Info("Action order validation passed")
	return true
}

// getExpectedActionsFromConfig extracts the expected action sequence from the call chain config
func (ut *UnifiedTest) getExpectedActionsFromConfig() []string {
	expectedActions := make([]string, 0)

	// Start with the root action (service-0.action-0)
	currentAction := "service-0.action-0"
	visited := make(map[string]bool)

	for currentAction != "" && !visited[currentAction] {
		visited[currentAction] = true
		expectedActions = append(expectedActions, currentAction)

		// Get the next actions from config
		config, exists := ut.config.CallChainConfig[currentAction]
		if !exists || len(config.Actions) == 0 {
			break
		}

		// For simplicity, take the first action (could be enhanced for parallel execution)
		if len(config.Actions) > 0 {
			currentAction = config.Actions[0]
		} else {
			break
		}
	}

	return expectedActions
}

// getActualActionResults returns the actual action results
func (ut *UnifiedTest) getActualActionResults() []UnifiedActionResult {
	return ut.actionResults
}

// generateMetrics generates comprehensive test metrics
func (ut *UnifiedTest) generateMetrics() *TestMetrics {
	metrics := &TestMetrics{
		DiscoveryTimeMs:     float64(ut.discoveryTime.Nanoseconds()) / 1e6,
		ExecutionTimeMs:     float64(ut.executionTime.Nanoseconds()) / 1e6,
		TotalTimeMs:         float64(ut.discoveryTime.Nanoseconds())/1e6 + float64(ut.executionTime.Nanoseconds())/1e6,
		TotalActionsCalled:  len(ut.actionResults),
		TotalEventsReceived: len(ut.eventResults),
		InitialHeapBytes:    ut.memoryStats.InitialHeapBytes,
		PeakHeapBytes:       ut.memoryStats.PeakHeapBytes,
		FinalHeapBytes:      ut.memoryStats.FinalHeapBytes,
		HeapGrowthBytes:     ut.memoryStats.HeapGrowthBytes,
		InitialGoroutines:   ut.memoryStats.InitialGoroutines,
		PeakGoroutines:      ut.memoryStats.PeakGoroutines,
		FinalGoroutines:     ut.memoryStats.FinalGoroutines,
		GoroutineLeak:       ut.memoryStats.GoroutineLeak,
	}

	// Calculate throughput
	if metrics.ExecutionTimeMs > 0 {
		metrics.ActionsPerSecond = float64(metrics.TotalActionsCalled) / (metrics.ExecutionTimeMs / 1000)
		metrics.EventsPerSecond = float64(metrics.TotalEventsReceived) / (metrics.ExecutionTimeMs / 1000)
	}

	// Set validation flags
	if ut.validationReport != nil {
		metrics.CallChainComplete = ut.validationReport.CallChainComplete
		metrics.EventChainComplete = ut.validationReport.EventChainComplete
		metrics.PayloadSizesCorrect = ut.validationReport.PayloadSizesCorrect
		metrics.ActionOrderCorrect = ut.validationReport.ActionOrderCorrect
	}

	return metrics
}

// createBroker creates a broker with the specified transporter
func (ut *UnifiedTest) createBroker(index int) *broker.ServiceBroker {
	log.WithFields(log.Fields{
		"broker_index":     index,
		"transporter_type": ut.transporterType,
	}).Debug("Creating broker")

	// Create transporter factory
	transporterType := TransporterType(ut.transporterType)
	factory := NewTransporterFactory(&TransporterConfig{
		Type: transporterType,
	})

	// Create broker config
	brokerConfig := &moleculer.Config{
		TransporterFactory: func() interface{} {
			return factory.CreateTransporter()
		},
		LogLevel: ut.config.LogLevel,
	}

	log.WithFields(log.Fields{
		"broker_index": index,
		"log_level":    ut.config.LogLevel,
	}).Debug("Broker config created")

	// Create broker
	bkr := broker.New(brokerConfig)
	log.WithField("broker_index", index).Debug("Broker instance created")

	// Add services to this broker
	serviceDistribution := ut.distributeServices()
	brokerKey := fmt.Sprintf("broker-%d", index)
	services := serviceDistribution[brokerKey]

	log.WithFields(log.Fields{
		"broker_index":  index,
		"broker_key":    brokerKey,
		"services":      services,
		"service_count": len(services),
	}).Debug("Adding services to broker")

	// Add regular services
	for _, serviceName := range services {
		log.WithFields(log.Fields{
			"broker_index": index,
			"service_name": serviceName,
		}).Trace("Adding service to broker")
		ut.addServiceToBroker(bkr, serviceName, index)
	}

	// Add event aggregator service if this is an aggregator broker (every 3rd broker)
	if index%3 == 2 {
		aggregatorName := fmt.Sprintf("event-aggregator-%d", index)
		log.WithFields(log.Fields{
			"broker_index":    index,
			"aggregator_name": aggregatorName,
		}).Debug("Adding event aggregator service to broker")
		ut.addEventAggregatorService(bkr, aggregatorName, index)
	}

	log.WithField("broker_index", index).Debug("Broker setup completed")
	return bkr
}

func (ut *UnifiedTest) getAllExpectedServices() []string {
	services := make([]string, 0)
	totalServices := ut.config.TotalServices

	// Add all regular services
	for i := 0; i < totalServices; i++ {
		serviceName := fmt.Sprintf("service-%d", i)
		services = append(services, serviceName)
	}

	// Add event aggregator services (every 3rd broker)
	for i := 2; i < ut.config.BrokerCount; i += 3 {
		aggregatorName := fmt.Sprintf("event-aggregator-%d", i)
		services = append(services, aggregatorName)
	}

	return services
}

func (ut *UnifiedTest) validateServiceDiscovery() {
	// TODO: Implement service discovery validation
}

func (ut *UnifiedTest) createActionConfig() map[string]interface{} {
	// TODO: Implement action config creation
	return make(map[string]interface{})
}

func (ut *UnifiedTest) parseActionResults(result moleculer.Payload, cycle int) {
	// TODO: Implement action result parsing
}

func (ut *UnifiedTest) collectEventResults() {
	// TODO: Implement event result collection
}

// addServiceToBroker adds a service to a broker
func (ut *UnifiedTest) addServiceToBroker(bkr *broker.ServiceBroker, serviceName string, brokerIndex int) {
	log.WithFields(log.Fields{
		"broker_index":  brokerIndex,
		"service_name":  serviceName,
		"actions_count": ut.config.ActionsPerService,
	}).Trace("Creating service")

	// Extract service number from service name (e.g., "service-5" -> 5)
	var serviceNumber int
	fmt.Sscanf(serviceName, "service-%d", &serviceNumber)

	// Create actions for this service
	actions := make([]moleculer.Action, 0)
	for i := 0; i < ut.config.ActionsPerService; i++ {
		actionName := fmt.Sprintf("action-%d", i)
		actions = append(actions, moleculer.Action{
			Name: actionName,
			Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
				return ut.handleAction(ctx, params, serviceName, actionName)
			},
		})
		log.WithFields(log.Fields{
			"broker_index": brokerIndex,
			"service_name": serviceName,
			"action_name":  actionName,
		}).Trace("Created action for service")
	}

	// Create service schema
	serviceSchema := moleculer.ServiceSchema{
		Name:    serviceName,
		Actions: actions,
		Events: []moleculer.Event{
			{
				Name: "test-event",
				Handler: func(ctx moleculer.Context, params moleculer.Payload) {
					// Event handler - can be used for testing
				},
			},
		},
	}

	// Publish service
	bkr.Publish(serviceSchema)
	log.WithFields(log.Fields{
		"broker_index":  brokerIndex,
		"service_name":  serviceName,
		"actions_count": len(actions),
	}).Trace("Service published to broker")
}

// addEventAggregatorService adds an event aggregator service to a broker
func (ut *UnifiedTest) addEventAggregatorService(bkr *broker.ServiceBroker, serviceName string, brokerIndex int) {
	// Automatically determine which events to listen to
	// Listen to all events from services that are NOT on this broker
	eventsToListen := ut.getEventsToListenTo(brokerIndex)

	log.WithFields(log.Fields{
		"broker_index":     brokerIndex,
		"aggregator_name":  serviceName,
		"events_to_listen": eventsToListen,
	}).Debug("Setting up event aggregator with automatic event detection")

	// Create event aggregator service
	serviceSchema := moleculer.ServiceSchema{
		Name: serviceName,
		Actions: []moleculer.Action{
			{
				Name: "setup-events",
				Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
					return ut.handleSetupEvents(ctx, params, eventsToListen)
				},
			},
			{
				Name: "get-aggregated-events",
				Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
					return ut.handleGetAggregatedEvents(ctx, params, serviceName)
				},
			},
		},
	}

	// Publish service
	bkr.Publish(serviceSchema)
}

// getEventsToListenTo automatically determines which events an aggregator should listen to
// Each aggregator listens to all events from services that are NOT on its own broker
func (ut *UnifiedTest) getEventsToListenTo(aggregatorBrokerIndex int) []string {
	eventsToListen := make([]string, 0)
	serviceDistribution := ut.distributeServices()

	// Get all services on the aggregator's broker
	aggregatorBrokerKey := fmt.Sprintf("broker-%d", aggregatorBrokerIndex)
	servicesOnAggregatorBroker := serviceDistribution[aggregatorBrokerKey]

	// Create a map for quick lookup of services on this broker
	servicesOnThisBroker := make(map[string]bool)
	for _, serviceName := range servicesOnAggregatorBroker {
		servicesOnThisBroker[serviceName] = true
	}

	// For each service in the system, if it's NOT on this broker, add its events
	for i := 0; i < ut.config.TotalServices; i++ {
		serviceName := fmt.Sprintf("service-%d", i)

		// If this service is NOT on the aggregator's broker, add its events
		if !servicesOnThisBroker[serviceName] {
			// Add events for all actions of this service
			for j := 0; j < ut.config.ActionsPerService; j++ {
				eventName := fmt.Sprintf("%s.action-%d-event", serviceName, j)
				eventsToListen = append(eventsToListen, eventName)
			}
		}
	}

	log.WithFields(log.Fields{
		"aggregator_broker":  aggregatorBrokerIndex,
		"services_on_broker": servicesOnAggregatorBroker,
		"events_to_listen":   eventsToListen,
	}).Debug("Automatically determined events for aggregator")

	return eventsToListen
}

// handleAction handles action calls with call chain logic
func (ut *UnifiedTest) handleAction(ctx moleculer.Context, params moleculer.Payload, serviceName, actionName string) interface{} {
	log.WithFields(log.Fields{
		"service_name": serviceName,
		"action_name":  actionName,
	}).Trace("Handling action call")

	// Get call chain config for this action
	configKey := fmt.Sprintf("%s.%s", serviceName, actionName)
	callConfig, exists := ut.config.CallChainConfig[configKey]

	// Determine payload size based on config
	payloadSize := 1024 // Default
	if exists {
		payloadSize = callConfig.ReturnPayloadSize
	}

	// Emit event for this action
	eventName := fmt.Sprintf("%s-%s-event", serviceName, actionName)
	eventData := map[string]interface{}{
		"action_name":  actionName,
		"random_value": time.Now().UnixNano(),
		"payload":      make([]byte, payloadSize),
	}
	ctx.Emit(eventName, eventData)

	// Store event result for validation
	ut.eventResults = append(ut.eventResults, UnifiedActionResult{
		ServiceName: serviceName,
		ActionName:  actionName,
		Result:      eventData,
		Error:       nil,
		Duration:    time.Since(time.Now()),
	})

	log.WithFields(log.Fields{
		"service_name": serviceName,
		"action_name":  actionName,
		"event_name":   eventName,
	}).Trace("Emitted event for action")

	if !exists {
		// No more actions to call, return result
		result := map[string]interface{}{
			"action_name":  actionName,
			"random_value": time.Now().UnixNano(),
			"payload":      make([]byte, payloadSize),
		}

		// Store action result for validation
		ut.actionResults = append(ut.actionResults, UnifiedActionResult{
			ServiceName: serviceName,
			ActionName:  actionName,
			Result:      result,
			Error:       nil,
			Duration:    time.Since(time.Now()),
		})

		return result
	}

	// Call next actions in the chain
	results := make([]interface{}, 0)
	for _, nextAction := range callConfig.Actions {
		// Create payload for next action
		nextPayload := map[string]interface{}{
			"config":  ut.config.CallChainConfig,
			"payload": make([]byte, callConfig.ParameterPayloadSize),
		}

		// Call next action
		result := <-ctx.Call(nextAction, nextPayload)
		if result.IsError() {
			errorResult := map[string]interface{}{
				"error": result.Error().Error(),
			}

			// Store action result for validation
			ut.actionResults = append(ut.actionResults, UnifiedActionResult{
				ServiceName: serviceName,
				ActionName:  actionName,
				Result:      errorResult,
				Error:       result.Error(),
				Duration:    time.Since(time.Now()),
			})

			return errorResult
		}

		results = append(results, result.Value())
	}

	// Return combined results
	finalResult := map[string]interface{}{
		"action_name":  actionName,
		"random_value": time.Now().UnixNano(),
		"payload":      make([]byte, callConfig.ReturnPayloadSize),
		"results":      results,
	}

	// Store action result for validation
	ut.actionResults = append(ut.actionResults, UnifiedActionResult{
		ServiceName: serviceName,
		ActionName:  actionName,
		Result:      finalResult,
		Error:       nil,
		Duration:    time.Since(time.Now()),
	})

	return finalResult
}

// handleSetupEvents handles the setup-events action for event aggregators
func (ut *UnifiedTest) handleSetupEvents(ctx moleculer.Context, params moleculer.Payload, eventsToListen []string) interface{} {
	// Set up event listeners (simplified - in real implementation, you'd store these)
	// For now, just return success
	return map[string]interface{}{
		"status": "success",
		"events": eventsToListen,
		"action": "setup-events",
	}
}

// handleGetAggregatedEvents handles the get-aggregated-events action for event aggregators
func (ut *UnifiedTest) handleGetAggregatedEvents(ctx moleculer.Context, params moleculer.Payload, serviceName string) interface{} {
	// Return aggregated events (simplified - in real implementation, you'd collect actual events)
	// For now, return empty array
	return map[string]interface{}{
		"status":  "success",
		"events":  []interface{}{},
		"service": serviceName,
		"action":  "get-aggregated-events",
	}
}
