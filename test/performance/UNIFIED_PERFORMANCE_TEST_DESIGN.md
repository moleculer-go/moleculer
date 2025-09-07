# Unified Performance Test Design

## Overview

This document describes the design for a single, unified performance test that replaces all existing performance tests. The test is completely config-driven and can test any scenario by changing the configuration.

## Core Architecture

1. **Always start from broker-0** - no need to find which broker has the action
2. **Pass complete config + payload + metadata** to first action
3. **One generic action function** deployed across all services
4. **Action code handles everything organically** - no test-level orchestration
5. **Result aggregation happens in action code** - returns flat list of all results
6. **Validation uses final aggregated result** from root action

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
1. DISCOVERY PHASE
   ├── Create brokers with services
   ├── Start all brokers
   ├── Wait for service discovery (WaitFor)
   └── Capture discovery time

2. EXECUTION PHASE
   ├── Always start from broker-0
   ├── Call root action with complete config + payload + metadata
   ├── Action code handles all sub-calls organically
   ├── Collect final aggregated result from root action
   └── Capture execution time

3. VALIDATION PHASE
   ├── Extract actions from final result
   ├── Compare with expected actions from config
   └── Generate validation report
```

## Detailed Design

### 1. Service Architecture

#### Service Distribution
- **All services deployed on all brokers** - ensures maximum discoverability
- **Simple approach**: Each service exists on every broker
- **No complex load balancing** - focus on call chain execution

#### Action Chain
- **Config-based chain**: Each action looks up its configuration using `"service-X.action-Y"` as key
- **Flexible patterns**: Can be linear, branching, or complex graphs based on config
- **Test coordinator** always starts from broker-0 and calls the first action
- **Single payload object**: Action receives one payload with multiple parameters (config, payload_data, etc.)
- **Metadata passing**: Service name and action name passed in metadata via options
- **Action identity**: Action gets its identity from `context.Meta()` which contains service/action info
- **Organic execution**: Each action determines what to call next from its config and handles all sub-calls
- **Result aggregation**: Each action creates a flat list of all results (its own + sub-action results)
- **Chain continues** until an action has no more actions to call
- **Validation** ensures all expected actions in the config were executed using final aggregated result

#### Event System
- **Every action fires an event** with format: `{serviceName}-{actionName}-event`
- **Event emission**: Each action emits event using `defer context.Emit("service-0.action-0.called", eventData)`
- **Event data includes**: action_name, random_value, payload_size (for load simulation)
- **Every 3rd broker** (brokers 2, 5, 8, 11, 14, 17, etc.) has an event aggregator service with two actions:
  - `setup-events`: Automatically configures which events to listen to (all events from services NOT on this broker)
  - `get-aggregated-events`: Returns all collected events
- **Event aggregation algorithm**: Each aggregator automatically listens to all events from services that are NOT on its own broker
- **Event aggregation** happens automatically during action execution

#### Service Distribution Logic
```go
func (ut *UnifiedTest) distributeServices() map[string][]string {
    serviceDistribution := make(map[string][]string)
    
    // Initialize broker lists
    for i := 0; i < ut.config.BrokerCount; i++ {
        serviceDistribution[fmt.Sprintf("broker-%d", i)] = make([]string, 0)
    }
    
    // Deploy all services on all brokers
    for i := 0; i < ut.config.TotalServices; i++ {
        serviceName := fmt.Sprintf("service-%d", i)
        for j := 0; j < ut.config.BrokerCount; j++ {
            brokerKey := fmt.Sprintf("broker-%d", j)
            serviceDistribution[brokerKey] = append(serviceDistribution[brokerKey], serviceName)
        }
    }
    
    return serviceDistribution
}
```

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
        // Always start from broker-0
        rootAction := ut.findRootAction()
        
        // Create payload with config and load simulation data
        payload := map[string]interface{}{
            "config": ut.config.CallChainConfig,
            "payload_data": make([]byte, 1024), // Load simulation
        }
        
        // Create metadata with service/action info
        metadata := map[string]interface{}{
            "service_name": strings.Split(rootAction, ".")[0],
            "action_name": strings.Split(rootAction, ".")[1],
        }
        
        // Execute call chain - pass complete config + payload + metadata to first action
        result := <-ut.brokers[0].Call(rootAction, payload, moleculer.Meta(metadata))
        
        if result.IsError() {
            return fmt.Errorf("action chain failed in cycle %d: %v", cycle, result.Error())
        }
        
        // Store the final aggregated result from root action
        ut.finalResult = result.Value()
    }
    
    // Collect event results from all aggregators
    ut.collectEventResults()
    
    ut.executionTime = time.Since(startTime)
    return nil
}
```

### 4. Action Implementation

```go
// Generic action function that gets deployed across all services
func genericAction(context moleculer.Context, params moleculer.Payload) interface{} {
    // Get service/action identity from metadata
    meta := context.Meta()
    serviceName := meta.Get("service_name").String()
    actionName := meta.Get("action_name").String()
    actionKey := fmt.Sprintf("%s.%s", serviceName, actionName)
    
    // Get configuration for this action
    config := params.Get("config").Map()
    actionConfig := config[actionKey].(map[string]interface{})
    
    // Create return payload of specified size
    returnPayloadSize := actionConfig["return_payload_size"].(int)
    returnPayload := make([]byte, returnPayloadSize)
    
    // Emit event
    defer context.Emit(fmt.Sprintf("%s.%s.called", serviceName, actionName), map[string]interface{}{
        "action_name": actionKey,
        "random_value": time.Now().UnixNano(),
        "payload_size": returnPayloadSize,
    })
    
    // Create result for this action
    actionResult := map[string]interface{}{
        "action_name": actionKey,
        "random_value": time.Now().UnixNano(),
        "payload_size": returnPayloadSize,
        "payload": returnPayload,
    }
    
    // Check if this action needs to call other actions
    subActions := actionConfig["actions"].([]interface{})
    if len(subActions) == 0 {
        // No sub-actions, return just this action's result
        return []interface{}{actionResult}
    }
    
    // Call all sub-actions and collect results
    var allResults []interface{}
    allResults = append(allResults, actionResult)
    
    for _, subActionName := range subActions {
        // Create payload for sub-action
        parameterPayloadSize := actionConfig["parameter_payload_size"].(int)
        subPayload := map[string]interface{}{
            "config": config, // Pass same config to all actions
            "payload_data": make([]byte, parameterPayloadSize),
        }
        
        // Create metadata for sub-action
        subMeta := map[string]interface{}{
            "service_name": strings.Split(subActionName.(string), ".")[0],
            "action_name": strings.Split(subActionName.(string), ".")[1],
        }
        
        // Call sub-action
        subResult := <-context.Call(subActionName.(string), subPayload, moleculer.Meta(subMeta))
        if subResult.IsError() {
            // Handle error appropriately
            continue
        }
        
        // Add sub-action results to our results
        if subResults, ok := subResult.Value().([]interface{}); ok {
            allResults = append(allResults, subResults...)
        }
    }
    
    // Return flat list of all results (this action + all sub-action results)
    return allResults
}
```

### 5. Validation Phase

```go
func (ut *UnifiedTest) validateResults() *ValidationReport {
    report := &ValidationReport{}
    
    // ALWAYS validate - no optional validation
    expectedActions := ut.getExpectedActionsFromConfig()
    actualActions := ut.extractActionsFromFinalResult(ut.finalResult)
    
    report.CallChainComplete = ut.validateCallChain(expectedActions, actualActions)
    report.EventChainComplete = ut.validateEventChain()
    report.PayloadSizesCorrect = ut.validatePayloadSizes()
    report.ActionOrderCorrect = ut.validateActionOrder()
    
    return report
}
```

### 6. Test Result Structure

```go
type UnifiedTestResult struct {
    // Test Identity
    TestName        string
    TestDescription string
    
    // Execution Results
    Success         bool
    Error           error
    
    // Timing
    DiscoveryTimeMs   float64
    ExecutionTimeMs   float64
    TotalTimeMs       float64
    
    // Validation
    ValidationReport *ValidationReport
    
    // Final Result
    FinalResult      interface{}
}
```

## Configuration Example

### Minimal Test
```json
{
    "test_name": "Minimal Debug Test",
    "test_description": "Minimal test with 2 brokers and 2 services for easy debugging",
    "transporter_types": ["TCP"],
    "transporter_config": {
        "TCP": {
            "port": 0
        }
    },
    "broker_count": 2,
    "total_services": 2,
    "actions_per_service": 1,
    "call_chain_config": {
        "service-0.action-0": {
            "actions": ["service-1.action-0"],
            "return_payload_size": 100,
            "parameter_payload_size": 50,
            "expected_result_count": 1,
            "expected_event_count": 2
        },
        "service-1.action-0": {
            "actions": [],
            "return_payload_size": 100,
            "parameter_payload_size": 50,
            "expected_result_count": 0,
            "expected_event_count": 1
        }
    },
    "test_timeout_seconds": 10,
    "test_cycles": 1,
    "log_level": "TRACE"
}
```

## Implementation Steps

1. **Create generic action function** that gets deployed across all services
2. **Implement test execution** that always starts from broker-0
3. **Pass complete config + payload + metadata** to first action
4. **Let action code handle all sub-calls organically**
5. **Collect final aggregated result** from root action
6. **Validate using final result** to ensure all actions were called

## Benefits

1. **Simple Architecture**: Always start from broker-0, no complex broker finding
2. **Organic Execution**: Action code handles all sub-calls naturally
3. **Easy Debugging**: Clear flow with trace logging
4. **Config-Driven**: Easy to test different call chain patterns
5. **Framework Compliant**: Uses moleculer-go patterns correctly
