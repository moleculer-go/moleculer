# Dynamic Scaling Test Specification

## Overview
A new performance test that dynamically scales brokers and action chains over time to identify resource leaks and performance degradation patterns based on broker count.

## Test Name
`DynamicScalingTest`

## Test Purpose
- Identify resource leaks that scale with broker count
- Test performance degradation as the system grows
- Validate dynamic action chain updates
- Measure memory and goroutine growth over time
- Test transporter performance under dynamic scaling

## Test Configuration

### Initial Setup
- **Starting Brokers**: 2
- **Starting Services**: 2 (service-0, service-1)
- **Starting Action Chain**: service-0.action-0 → service-1.action-0
- **Transporter Types**: All supported (TCP, NATS, Redis, AMQP, Kafka)

### Dynamic Growth Pattern
- **Growth Interval**: Configurable via JSON config (default: 5 seconds)
- **Reduction Interval**: Configurable via JSON config (default: 5 seconds)
- **Max Brokers**: Configurable via JSON config (default: 10)
- **Growth Rate**: 1 broker per interval
- **Reduction Rate**: 1 broker per interval
- **Service Naming**: service-{broker_index}
- **Action Naming**: action-0 (all services have same action name)

### Action Chain Evolution
```
Initial (2 brokers):
service-0.action-0 → service-1.action-0

After adding broker-2 (3 brokers):
service-0.action-0 → service-1.action-0 → service-2.action-0

After adding broker-3 (4 brokers):
service-0.action-0 → service-1.action-0 → service-2.action-0 → service-3.action-0

At max brokers (10 brokers):
service-0.action-0 → service-1.action-0 → ... → service-9.action-0

After removing broker-9 (9 brokers):
service-0.action-0 → service-1.action-0 → ... → service-8.action-0

After removing broker-8 (8 brokers):
service-0.action-0 → service-1.action-0 → ... → service-7.action-0

Back to initial (2 brokers):
service-0.action-0 → service-1.action-0
```

## Test Phases

### Phase 1: Initial Setup (0-Xs)
- Start with 2 brokers
- Deploy service-0 and service-1
- Configure initial action chain
- Run baseline performance test
- Capture initial metrics

### Phase 2: Dynamic Growth (Xs to Ys)
- Every configurable interval:
  1. Add new broker
  2. Deploy new service
  3. Update action chain configuration
  4. Run performance test with new chain
  5. Capture metrics
  6. Check for resource leaks
- Continue until max brokers reached

### Phase 3: Stabilization (Ys to Zs)
- After reaching max brokers
- Run extended test to check for delayed leaks
- Monitor resource usage over time
- Optional phase (configurable duration)

### Phase 4: Dynamic Reduction (Zs to end)
- Every configurable interval:
  1. Stop highest-index broker
  2. Remove service from action chain
  3. Update action chain configuration
  4. Run performance test with reduced chain
  5. Capture metrics
  6. Check for resource cleanup
- Continue until back to 2 brokers

## Metrics Collection

### Per-Phase Metrics
- **Broker Count**: Current number of active brokers
- **Action Chain Length**: Number of actions in current chain
- **Execution Time**: Time to complete full action chain
- **Memory Usage**: Heap allocation and growth
- **Goroutine Count**: Total and leaked goroutines
- **Discovery Time**: Time for new broker to join cluster
- **Validation Success**: Action chain execution success rate

### Cumulative Metrics
- **Total Memory Growth**: Memory increase from start to current phase
- **Total Goroutine Growth**: Goroutine increase from start to current phase
- **Performance Degradation**: Execution time increase over time
- **Resource Leak Rate**: Rate of resource accumulation per broker

## Test Configuration File

### JSON Structure
```json
{
  "test_name": "Dynamic Scaling Test",
  "test_description": "Test that grows and reduces brokers and action chains over time",
  "transporter_types": ["TCP", "NATS", "Redis", "AMQP", "Kafka"],
  "transporter_config": {
    "TCP": {
      "type": "TCP",
      "port": 0
    },
    "NATS": {
      "type": "NATS",
      "url": "nats://localhost:4222"
    },
    "Redis": {
      "type": "Redis",
      "host": "localhost",
      "port": 6379,
      "password": "",
      "db": 2,
      "prefix": "test-moleculer"
    },
    "AMQP": {
      "type": "AMQP",
      "url": "amqp://localhost:5672"
    },
    "Kafka": {
      "type": "Kafka",
      "brokers": ["localhost:9092"]
    }
  },
  "dynamic_scaling_config": {
    "starting_brokers": 2,
    "max_brokers": 10,
    "growth_interval_seconds": 30,
    "reduction_interval_seconds": 30,
    "stabilization_phase_seconds": 60,
    "enable_stabilization_phase": true
  },
  "service_config": {
    "actions_per_service": 1,
    "action_name": "action-0",
    "return_payload_size": 1024,
    "parameter_payload_size": 512,
    "expected_event_count": 1
  },
  "test_timeout_seconds": 600,
  "log_level": "INFO"
}
```

## Expected Outcomes

### What the Test Measures
- **Resource Leak Patterns**: How memory and goroutines scale with broker count
- **Performance Scaling**: How execution time changes as brokers are added/removed
- **Cleanup Effectiveness**: How well resources are cleaned up when brokers are removed
- **Transporter Behavior**: How different transporters handle dynamic scaling
- **Action Chain Integrity**: Whether action chains work correctly during growth/reduction
- **Discovery Performance**: How long it takes for new brokers to join the cluster
- **Event Aggregation**: Whether events are properly collected during scaling phases

### Data Collection Focus
- **Growth Phase**: Measure resource accumulation patterns
- **Stabilization Phase**: Measure delayed resource leaks
- **Reduction Phase**: Measure resource cleanup effectiveness
- **Comparison**: Compare growth vs reduction patterns for each transporter

## Implementation Considerations

### Dynamic Action Chain Updates
- Update `call_chain_config` in real-time
- Ensure atomic updates to prevent race conditions
- Validate chain integrity after each update

### Broker Management
- Graceful broker startup and shutdown
- Proper service registration and discovery
- Clean resource cleanup when brokers are removed

### Metrics Collection
- Real-time monitoring during growth phases
- Efficient data storage for large datasets
- Clear visualization of growth patterns

### Error Handling
- Robust error recovery during dynamic updates
- Fallback mechanisms for failed broker additions
- Comprehensive logging for debugging

## Test Variations

### Variation 1: Slow Growth
- Growth interval: 60 seconds
- Max brokers: 5
- Focus on detailed resource monitoring

### Variation 2: Fast Growth
- Growth interval: 10 seconds
- Max brokers: 15
- Stress test for rapid scaling

### Variation 3: Mixed Transporters
- Different transporters for different brokers
- Test cross-transporter communication
- Validate transporter-specific resource usage

## Test Implementation Details

### Separate Test Structure
- **Independent Test**: Completely separate from existing unified test
- **Own Configuration**: Uses its own JSON config files
- **Same Techniques**: Reuses action chain, event collection, and metrics techniques
- **New Test File**: `dynamic_scaling_test.go`
- **New Config Files**: `configs/dynamic_scaling_*.json`

### Dynamic Configuration Management
- **Real-time Updates**: Action chain config updated as brokers are added/removed
- **Atomic Operations**: Ensure thread-safe configuration updates
- **Validation**: Verify action chain integrity after each update
- **Rollback**: Handle failed broker additions gracefully

### Broker Lifecycle Management
- **Graceful Startup**: Proper broker initialization and service registration
- **Graceful Shutdown**: Clean broker termination and resource cleanup
- **Service Discovery**: Ensure new brokers are discovered by existing ones
- **Event Cleanup**: Proper event handler cleanup when brokers are removed

## Deliverables

1. **Test Implementation**: Complete Go test with dynamic scaling
2. **Configuration Files**: JSON configs for different test variations
3. **Results Analysis**: Automated analysis of resource leak patterns
4. **Performance Reports**: Detailed reports for each transporter
5. **Visualization**: Charts showing growth patterns and resource usage
6. **Documentation**: Complete test documentation and usage guide

## Timeline

- **Phase 1**: Test specification and design (1 day)
- **Phase 2**: Core implementation (2-3 days)
- **Phase 3**: Testing and validation (1-2 days)
- **Phase 4**: Documentation and analysis (1 day)

## Dependencies

- Existing unified test framework
- Dynamic configuration management
- Real-time metrics collection
- Broker lifecycle management
- Action chain update mechanisms

## Risks and Mitigations

### Risk: Resource Exhaustion
- **Mitigation**: Configurable max brokers and timeout limits

### Risk: Race Conditions
- **Mitigation**: Atomic updates and proper synchronization

### Risk: Test Complexity
- **Mitigation**: Modular design and comprehensive error handling

### Risk: Performance Impact
- **Mitigation**: Efficient metrics collection and optional detailed logging
