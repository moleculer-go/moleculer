# Broker Wait Loops Optimization - Design Document

## **Problem Statement**
The current broker wait methods use busy waiting with `time.Sleep(time.Microsecond)` in tight loops, causing:
- High CPU usage during waiting periods
- Inefficient resource utilization
- Poor scalability under load

## **Solution: Channel-Based Synchronization with Context Cancellation**

### **Core Concept**
Replace busy waiting loops with event-driven channel synchronization using the LocalBus for notifications and context cancellation for clean shutdown.

## **Design Overview**

### **1. Context-Based Cleanup Management**
- Add `waitContext` and `waitCancel` to `ServiceBroker` struct
- Initialize context in `broker.init()`
- Cancel context in `broker.Stop()` to clean up all waiting goroutines

### **2. New Event Constants**
Define new events in `moleculer/registry/registry.go`:
```go
const (
    EventServiceAvailable = "service.available"
    EventActionAvailable  = "action.available"
    EventNodeAvailable    = "node.available"
)
```

### **3. Async Wait Methods**
Create new async methods in `moleculer/broker/broker.go`:
- `WaitForServiceAsync(serviceName string, timeout time.Duration) <-chan error`
- `WaitForActionAsync(actionName string, timeout time.Duration) <-chan error`
- `WaitForNodeAsync(nodeID string, timeout time.Duration) <-chan error`
- `WaitForDependenciesAsync(deps []string, timeout time.Duration) <-chan error`

### **4. Event Emission**
Modify `moleculer/registry/registry.go` to emit availability events:
- `EventServiceAvailable` when services are added/connected
- `EventActionAvailable` when actions are added/connected
- `EventNodeAvailable` when nodes are connected/updated

## **Implementation Details**

### **ServiceBroker Struct Changes**
```go
type ServiceBroker struct {
    // ... existing fields ...
    waitContext context.Context
    waitCancel  context.CancelFunc
}
```

### **Context Initialization**
```go
func (broker *ServiceBroker) init() {
    // ... existing init code ...
    broker.waitContext, broker.waitCancel = context.WithCancel(context.Background())
}
```

### **Context Cleanup**
```go
func (broker *ServiceBroker) Stop() {
    // ... existing stop code ...
    
    // Cancel all waiting operations
    if broker.waitCancel != nil {
        broker.waitCancel()
    }
}
```

### **Async Wait Method Pattern**
```go
func (broker *ServiceBroker) WaitForServiceAsync(serviceName string, timeout time.Duration) <-chan error {
    resultChan := make(chan error, 1)
    
    // Check if already available
    if broker.registry.KnowService(serviceName) {
        resultChan <- nil
        return resultChan
    }
    
    // Use a cleanup flag to prevent double-sending
    var cleanup sync.Once
    sendResult := func(err error) {
        cleanup.Do(func() {
            select {
            case resultChan <- err:
            case <-broker.waitContext.Done():
                // Broker is stopping, don't send result
            }
        })
    }
    
    // Subscribe to service available event
    broker.LocalBus().Once(EventServiceAvailable, func(args ...interface{}) {
        if len(args) > 0 {
            if availableServiceName, ok := args[0].(string); ok && availableServiceName == serviceName {
                sendResult(nil)
            }
        }
    })
    
    // Handle timeout with context cancellation
    go func() {
        select {
        case <-time.After(timeout):
            sendResult(errors.New("timeout waiting for service: " + serviceName))
        case <-broker.waitContext.Done():
            // Broker is stopping, don't send timeout error
            return
        }
    }()
    
    return resultChan
}
```

### **Blocking Wait Method Updates**
```go
func (broker *ServiceBroker) waitForService(serviceName string, timeout time.Duration) error {
    resultChan := broker.WaitForServiceAsync(serviceName, timeout)
    select {
    case err := <-resultChan:
        return err
    case <-broker.waitContext.Done():
        return errors.New("broker is stopping")
    }
}
```

## **Event Emission Points**

### **Service Available Events**
- `AddLocalService()` - emit for service name and full name
- `RemoteNodeInfoReceived()` - emit for new remote services

### **Action Available Events**
- `AddLocalService()` - emit for each action's full name
- `RemoteNodeInfoReceived()` - emit for new remote actions

### **Node Available Events**
- `RemoteNodeInfoReceived()` - emit for connected/updated nodes

## **Benefits**

### **Performance Improvements**
- **Eliminates Busy Waiting**: No more `time.Sleep(time.Microsecond)` loops
- **Event-Driven**: Immediate notification when conditions are met
- **Reduced CPU Usage**: No constant polling
- **Better Scalability**: Efficient under high load

### **Resource Management**
- **Goroutine Cleanup**: Context cancellation prevents leaks
- **Memory Efficiency**: No accumulation of waiting goroutines
- **Clean Shutdown**: All operations cancelled on broker stop

### **Maintainability**
- **Standard Go Patterns**: Uses `context.Context` idiomatically
- **Simple Implementation**: Easy to understand and maintain
- **Consistent API**: All wait methods follow same pattern

## **Testing Strategy**

### **Unit Tests**
- Test async methods return immediately when already available
- Test timeout handling
- Test context cancellation on broker stop
- Test event subscription and notification

### **Integration Tests**
- Test with actual service registration
- Test with multiple concurrent waits
- Test broker stop during waiting operations

### **Performance Tests**
- Benchmark before/after CPU usage
- Test with high concurrency
- Measure memory usage during waiting

## **Migration Strategy**

### **Phase 1: Add New Methods**
- Implement async methods alongside existing ones
- Add event emission to registry
- Add context management to broker

### **Phase 2: Update Existing Methods**
- Modify existing wait methods to use async versions
- Maintain backward compatibility

### **Phase 3: Testing & Validation**
- Run comprehensive tests
- Performance benchmarking
- Memory leak testing

## **Files to Modify**

### **Core Files**
1. **`moleculer/broker/broker.go`** - Add context fields, async methods, update existing methods
2. **`moleculer/registry/registry.go`** - Add event constants and emission

### **Test Files**
3. **`moleculer/broker/broker_test.go`** - Add tests for new functionality
4. **`moleculer/registry/registry_test.go`** - Add tests for event emission

## **Success Criteria**

### **Functional Requirements**
- ✅ All existing wait methods work identically
- ✅ New async methods provide same functionality
- ✅ Context cancellation works on broker stop
- ✅ No goroutine leaks

### **Performance Requirements**
- ✅ Eliminate busy waiting loops
- ✅ Reduce CPU usage during waiting
- ✅ Improve scalability under load
- ✅ Maintain response time for notifications

### **Quality Requirements**
- ✅ All tests pass
- ✅ No regressions in existing functionality
- ✅ Clean code with proper error handling
- ✅ Comprehensive test coverage

