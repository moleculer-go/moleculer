# Unified Performance Test Design

## Overview

This document describes the design for a single, unified performance test that replaces all existing performance tests (`performance_benchmark_test.go`, `multi_transporter_test.go`, `multi_broker_test.go`, `enhanced_test.go`). The test is completely config-driven and can test any scenario by changing the configuration.

## Core Principles

1. **Single Test**: One test file that handles all scenarios
2. **Config-Driven**: All behavior controlled by configuration
3. **Transporter Agnostic**: Same test runs on any transporter
4. **Multi-Broker Support**: Configurable number of brokers
5. **Call Chain Validation**: Validates complete action chains
6. **Comprehensive Metrics**: Measures everything (memory, goroutines, timing, etc.)
7. **Discovery Timing**: Properly measures service discovery time
8. **Result Validation**: Ensures all actions were called and returned properly

## Test Architecture

### 1. Configuration Structure

```go
type UnifiedTestConfig struct {
    // Test Identity
    TestName        string
    TestDescription string
    
    // Transporter Configuration
    TransporterTypes  []string // ["Memory", "TCP", "NATS", "Redis", "AMQP", "Kafka"]
    TransporterConfig map[string]interface{}
    
    // Broker Configuration
    BrokerCount       int
    TotalServices     int  // Total number of services (service-0 through service-(TotalServices-1))
    ActionsPerService int  // Minimum 3 actions per service
    
    // Call Chain Configuration
    CallChainConfig map[string]ActionCallConfig
    
    // Event Configuration (AUTOMATIC - no manual configuration needed)
    // Each event aggregator automatically listens to all events from services NOT on its own broker
    
    // Test Execution
    TestTimeoutSeconds int
    TestCycles         int  // How many times to run the action execution
    
    // Logging
    LogLevel string
}
```

### 2. Action Call Configuration

```go
type ActionCallConfig struct {
    // Target actions to call
    Actions []string
    
    // Payload configuration
    ReturnPayloadSize    int
    ParameterPayloadSize int
    
    // Validation
    ExpectedResultCount int
    ExpectedEventCount  int
}
```

### 3. Test Execution Flow

```
1. CONFIGURATION PHASE
   ├── Parse and validate configuration
   ├── Create transporter factory
   └── Initialize test metrics

2. DISCOVERY PHASE (Discovery Time)
   ├── Create brokers with services
   ├── Start all brokers
   ├── Wait for service discovery (WaitFor)
   ├── Validate all services are available
   └── Capture discovery time

3. EXECUTION PHASE (Execution Time)
   ├── Start action chain from first service
   ├── Execute complete call chain
   ├── Collect all action results
   ├── Collect all event results
   └── Capture execution time

4. VALIDATION PHASE
   ├── Validate call chain completion
   ├── Validate event chain completion
   ├── Validate payload sizes
   ├── Validate action order
   └── Generate validation report

5. METRICS COLLECTION
   ├── Memory statistics
   ├── Goroutine count
   ├── Timing metrics
   ├── Throughput metrics
   └── Error statistics

6. RESULT GENERATION
   ├── Create comprehensive test result
   ├── Include all configuration
   ├── Include all metrics
   └── Save to test results
```

## Detailed Design

### 1. Service Architecture

#### Service Distribution
- **Minimum 9 brokers** (for 3 event aggregators)
- **Broker count must be multiple of 3** (9, 12, 15, 18, 21, 24, 27, 30, etc.)
- **Total services must be divisible by 3** (9, 12, 15, 18, 21, 24, 27, 30, 33, 36, etc.)
- **Auto-adjustment**: If config is invalid, adjust to closest valid numbers
- **Load balancing**: Each service exists on multiple brokers (calculated dynamically)
- **Service distribution formula**: Number of brokers per service = max(3, round(Total Services / Broker Count))
- **Dynamic calculation**: For 60 services and 9 brokers = max(3, round(60/9)) = max(3, 7) = 7 brokers per service
- **Load balancing ratio**: Each service exists on max(3, round(Total Services / Broker Count)) brokers (skipping its own broker index)
- **Examples**: 
  - 9 services, 9 brokers → max(3, round(9/9)) = max(3, 1) = 3 brokers per service
  - 18 services, 9 brokers → max(3, round(18/9)) = max(3, 2) = 3 brokers per service
  - 60 services, 9 brokers → max(3, round(60/9)) = max(3, 7) = 7 brokers per service
- **Every 3rd broker** (brokers 2, 5, 8, 11, 14, 17, etc.) becomes event aggregator

#### Action Chain
- **Config-based chain**: Each action looks up its configuration using `"service-X.action-Y"` as key
- **Flexible patterns**: Can be linear, branching, or complex graphs based on config
- **Test coordinator** passes the complete call chain config to the first action
- **Each action** determines what to call next from its config
- **Chain continues** until an action has no more actions to call
- **Validation** ensures all expected actions in the config were executed

#### Event System
- **Every action fires an event** with format: `{serviceName}-{actionName}-event`
- **Event data includes**: action_name, random_value, payload
- **Every 3rd broker** (brokers 2, 5, 8, 11, 14, 17, etc.) has an event aggregator service with two actions:
  - `setup-events`: Automatically configures which events to listen to (all events from services NOT on this broker)
  - `get-aggregated-events`: Returns all collected events
- **Event aggregation algorithm**: Each aggregator automatically listens to all events from services that are NOT on its own broker
- **Event aggregation** happens automatically during action execution

#### Auto-Adjustment Logic
```go
func (ut *UnifiedTest) adjustConfiguration() {
    // Ensure minimum 9 brokers
    if ut.config.BrokerCount < 9 {
        ut.config.BrokerCount = 9
    }
    
    // Ensure broker count is multiple of 3
    if ut.config.BrokerCount % 3 != 0 {
        ut.config.BrokerCount = ((ut.config.BrokerCount + 2) / 3) * 3
    }
    
    // Ensure total services is divisible by 3
    if ut.config.TotalServices % 3 != 0 {
        ut.config.TotalServices = ((ut.config.TotalServices + 2) / 3) * 3
    }
    
    // Ensure minimum 3 actions per service
    if ut.config.ActionsPerService < 3 {
        ut.config.ActionsPerService = 3
    }
}

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
```

#### Example Distributions

**Example 1: Config = 8 brokers, 18 total services**
- **Adjusted**: 9 brokers, 18 total services
- **Event Aggregators**: Brokers 2, 5, 8 (3 aggregators)
- **Brokers per service**: max(3, round(18/9)) = max(3, 2) = 3 brokers per service
- **Service Distribution with Load Balancing**: Each service exists on exactly 3 brokers
- **Pattern**: service-X exists on brokers (X+1)%9, (X+2)%9, (X+3)%9

**Verification - Each service should exist on exactly 3 brokers:**
- service-0: exists on brokers 1, 2, 3 ✅
- service-1: exists on brokers 2, 3, 4 ✅
- service-2: exists on brokers 3, 4, 5 ✅
- service-3: exists on brokers 4, 5, 6 ✅
- service-4: exists on brokers 5, 6, 7 ✅
- service-5: exists on brokers 6, 7, 8 ✅
- service-6: exists on brokers 7, 8, 0 ✅
- service-7: exists on brokers 8, 0, 1 ✅
- service-8: exists on brokers 0, 1, 2 ✅
- service-9: exists on brokers 1, 2, 3 ✅
- service-10: exists on brokers 2, 3, 4 ✅
- service-11: exists on brokers 3, 4, 5 ✅
- service-12: exists on brokers 4, 5, 6 ✅
- service-13: exists on brokers 5, 6, 7 ✅
- service-14: exists on brokers 6, 7, 8 ✅
- service-15: exists on brokers 7, 8, 0 ✅
- service-16: exists on brokers 8, 0, 1 ✅
- service-17: exists on brokers 0, 1, 2 ✅

**Wait! There's an error in the logic above. Let me recalculate:**

Using the pattern `(i + j) % 9` where `j = 1, 2, 3`:
- service-8: exists on brokers `(8+1)%9=0`, `(8+2)%9=1`, `(8+3)%9=2` ✅ (brokers 0, 1, 2)
- service-9: exists on brokers `(9+1)%9=1`, `(9+2)%9=2`, `(9+3)%9=3` ✅ (brokers 1, 2, 3)

**The logic is actually correct!** Each service exists on exactly 3 brokers.

**Example 2: Config = 10 brokers, 36 total services**
- **Adjusted**: 12 brokers, 36 total services
- **Event Aggregators**: Brokers 2, 5, 8, 11 (4 aggregators)
- **Brokers per service**: max(3, round(36/12)) = max(3, 3) = 3 brokers per service
- **Service Distribution with Load Balancing**: Each service exists on exactly 3 brokers
- **Pattern**: service-X exists on brokers (X+1)%12, (X+2)%12, (X+3)%12
- **Verification**: 
  - service-0: exists on brokers 1, 2, 3 ✅
  - service-1: exists on brokers 2, 3, 4 ✅
  - service-35: exists on brokers 0, 1, 2 ✅

**Example 3: Config = 27 brokers, 27 total services**
- **Adjusted**: 27 brokers, 27 total services
- **Event Aggregators**: Brokers 2, 5, 8, 11, 14, 17, 20, 23, 26 (9 aggregators)
- **Brokers per service**: max(3, round(27/27)) = max(3, 1) = 3 brokers per service
- **Service Distribution with Load Balancing**: Each service exists on exactly 3 brokers
- **Pattern**: service-X exists on brokers (X+1)%27, (X+2)%27, (X+3)%27
- **Verification**: 
  - service-0: exists on brokers 1, 2, 3 ✅
  - service-1: exists on brokers 2, 3, 4 ✅
  - service-26: exists on brokers 0, 1, 2 ✅

**Example 4: Config = 9 brokers, 63 total services**
- **Adjusted**: 9 brokers, 63 total services
- **Event Aggregators**: Brokers 2, 5, 8 (3 aggregators)
- **Brokers per service**: max(3, round(63/9)) = max(3, 7) = 7 brokers per service
- **Service Distribution with Load Balancing**: Each service exists on exactly 7 brokers
- **Pattern**: service-X exists on brokers (X+1)%9, (X+2)%9, ..., (X+7)%9
- **Verification**: 
  - service-0: exists on brokers 1, 2, 3, 4, 5, 6, 7 ✅
  - service-1: exists on brokers 2, 3, 4, 5, 6, 7, 8 ✅
  - service-8: exists on brokers 0, 1, 2, 3, 4, 5, 6 ✅

### 2. Discovery Phase

```go
func (ut *UnifiedTest) runDiscoveryPhase() error {
    startTime := time.Now()
    
    // Create and start all brokers
    for i := 0; i < ut.config.BrokerCount; i++ {
        broker := ut.createBroker(i)
        ut.brokers = append(ut.brokers, broker)
        broker.Start()
    }
    
    // Wait for all services to be discovered
    expectedServices := ut.getAllExpectedServices()
    for _, broker := range ut.brokers {
        err := broker.WaitFor(expectedServices...)
        if err != nil {
            return fmt.Errorf("service discovery failed: %v", err)
        }
    }
    
    // Additional validation
    ut.validateServiceDiscovery()
    
    ut.discoveryTime = time.Since(startTime)
    return nil
}
```

### 3. Execution Phase

```go
func (ut *UnifiedTest) runExecutionPhase() error {
    startTime := time.Now()
    
    // Run multiple cycles of action execution
    for cycle := 0; cycle < ut.config.TestCycles; cycle++ {
        // Start action chain from first service
        firstService := "service-0"
        firstAction := "action-0"
        
        // Create action config with complete call chain configuration
        actionConfig := ut.createActionConfig()
        
        // Execute call chain - pass complete config to first action
        result := <-ut.brokers[0].Call(fmt.Sprintf("%s.%s", firstService, firstAction), actionConfig)
        
        if result.IsError() {
            return fmt.Errorf("action chain failed in cycle %d: %v", cycle, result.Error())
        }
        
        // Parse and validate results for this cycle
        ut.parseActionResults(result, cycle)
    }
    
    // Collect event results from all aggregators
    ut.collectEventResults()
    
    ut.executionTime = time.Since(startTime)
    return nil
}
```

### 4. Validation Phase

```go
func (ut *UnifiedTest) validateResults() *ValidationReport {
    report := &ValidationReport{}
    
    // ALWAYS validate - no optional validation
    expectedActions := ut.getExpectedActionsFromConfig()
    actualActions := ut.getActualActionResults()
    
    report.CallChainComplete = ut.validateCallChain(expectedActions, actualActions)
    report.EventChainComplete = ut.validateEventChain()
    report.PayloadSizesCorrect = ut.validatePayloadSizes()
    report.ActionOrderCorrect = ut.validateActionOrder()
    
    return report
}
```

### 5. Metrics Collection

```go
type TestMetrics struct {
    // Timing
    DiscoveryTimeMs   float64
    ExecutionTimeMs   float64
    TotalTimeMs       float64
    
    // Actions
    TotalActionsCalled    int
    SuccessfulActions     int
    FailedActions         int
    
    // Events
    TotalEventsReceived   int
    ExpectedEvents        int
    
    // Memory
    InitialHeapBytes      uint64
    PeakHeapBytes         uint64
    FinalHeapBytes        uint64
    HeapGrowthBytes       int64
    
    // Goroutines
    InitialGoroutines     int
    PeakGoroutines        int
    FinalGoroutines       int
    GoroutineLeak         int
    
    // Throughput
    ActionsPerSecond      float64
    EventsPerSecond       float64
    
    // Validation
    CallChainComplete     bool
    EventChainComplete    bool
    PayloadSizesCorrect   bool
    ActionOrderCorrect    bool
}
```

### 6. Test Result Structure

```go
type UnifiedTestResult struct {
    // Test Identity
    TestName        string
    TestDescription string
    Timestamp       time.Time
    
    // Configuration
    Configuration   *UnifiedTestConfig
    
    // Execution Results
    Success         bool
    Error           error
    
    // Timing
    DiscoveryTimeMs   float64
    ExecutionTimeMs   float64
    TotalTimeMs       float64
    
    // Metrics
    Metrics          *TestMetrics
    
    // Validation
    ValidationReport *ValidationReport
    
    // Raw Data
    ActionResults    []ActionResult
    EventResults     []ActionResult
    
    // Memory Stats
    MemoryStats      *MemoryStats
}
```

## Configuration Examples

### 1. Basic Test (All Transporters)
```json
{
    "test_name": "Basic Test",
    "transporter_types": ["Memory", "TCP", "NATS", "Redis", "AMQP", "Kafka"],
    "broker_count": 9,
    "total_services": 9,
    "actions_per_service": 3,
    "test_timeout_seconds": 30,
    "test_cycles": 1,
    "log_level": "INFO"
}
```

### 2. Medium Test (All Transporters)
```json
{
    "test_name": "Medium Test",
    "transporter_types": ["Memory", "TCP", "NATS", "Redis", "AMQP", "Kafka"],
    "broker_count": 12,
    "total_services": 18,
    "actions_per_service": 3,
    "test_timeout_seconds": 60,
    "test_cycles": 3,
    "log_level": "INFO"
}
```

### 3. Large Test (All Transporters)
```json
{
    "test_name": "Large Test",
    "transporter_types": ["Memory", "TCP", "NATS", "Redis", "AMQP", "Kafka"],
    "broker_count": 18,
    "total_services": 27,
    "actions_per_service": 3,
    "test_timeout_seconds": 120,
    "test_cycles": 5,
    "log_level": "DEBUG"
}
```

### 4. Debug Test (Single Transporter)
```json
{
    "test_name": "Debug Test",
    "transporter_types": ["Memory"],
    "broker_count": 9,
    "total_services": 9,
    "actions_per_service": 3,
    "test_timeout_seconds": 30,
    "test_cycles": 1,
    "log_level": "DEBUG"
}
```

## Implementation Plan

### Phase 1: Core Structure
1. Create `UnifiedTestConfig` struct
2. Create `UnifiedTest` struct
3. Implement configuration parsing
4. Implement basic test execution flow

### Phase 2: Discovery Phase
1. Implement broker creation and startup
2. Implement service discovery with WaitFor
3. Implement discovery time measurement
4. Add service validation

### Phase 3: Execution Phase
1. Implement action chain execution
2. Implement event collection
3. Implement execution time measurement
4. Add error handling

### Phase 4: Validation Phase
1. Implement call chain validation
2. Implement event chain validation
3. Implement payload size validation
4. Implement action order validation

### Phase 5: Metrics Collection
1. Implement memory statistics
2. Implement goroutine tracking
3. Implement throughput calculation
4. Implement comprehensive metrics

### Phase 6: Result Generation
1. Implement test result structure
2. Implement result serialization
3. Implement test result saving
4. Add comprehensive logging

## Benefits of This Design

1. **Single Test**: One test file handles all scenarios
2. **Config-Driven**: Easy to test different scenarios with flexible call chains
3. **All Transporters**: Same test runs on all transporters for fair comparison
4. **Comprehensive**: Measures everything in one place (no limits, just measurement)
5. **Maintainable**: Clear structure and separation of concerns
6. **Debuggable**: Clear phases and logging
7. **Extensible**: Easy to add new metrics or validations
8. **Comparable**: Same test across all transporters for fair comparison
9. **Flexible Call Chains**: Supports any call pattern (linear, branching, complex graphs)
10. **Proper Discovery Timing**: Measures actual service discovery time with WaitFor()
11. **Auto-Adjustment**: Automatically corrects invalid configurations
12. **Scalable**: Supports 9+ brokers with proper event aggregation
13. **Consistent**: All transporters run identical scenarios
14. **Separate Outputs**: Each transporter gets its own result file

## Migration Strategy

1. **Phase 1**: Create new unified test alongside existing tests
2. **Phase 2**: Validate new test produces same results as existing tests
3. **Phase 3**: Gradually replace existing tests with unified test
4. **Phase 4**: Remove old test files
5. **Phase 5**: Add new features and validations

This design provides a clean, maintainable, and comprehensive solution for all performance testing needs while being completely config-driven and transporter-agnostic. The test focuses on **measurement and data collection** rather than imposing limits, allowing for comprehensive performance analysis across different transporters and scenarios.
