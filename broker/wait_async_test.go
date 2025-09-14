package broker

import (
	"testing"
	"time"

	"github.com/moleculer-go/moleculer"
)

func TestWaitForServiceAsync(t *testing.T) {
	broker := New()
	broker.Start()
	defer broker.Stop()

	// Test 1: Service already available
	broker.Publish(moleculer.ServiceSchema{
		Name: "test-service",
		Actions: []moleculer.Action{
			{
				Name: "test",
				Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
					return params
				},
			},
		},
	})

	resultChan := broker.WaitForServiceAsync("test-service", 100*time.Millisecond)
	select {
	case err := <-resultChan:
		if err != nil {
			t.Errorf("Expected no error for already available service, got: %v", err)
		}
	case <-time.After(50 * time.Millisecond):
		t.Error("Expected immediate result for already available service")
	}

	// Test 2: Service not available - timeout
	resultChan = broker.WaitForServiceAsync("nonexistent-service", 50*time.Millisecond)
	select {
	case err := <-resultChan:
		if err == nil {
			t.Error("Expected timeout error for nonexistent service")
		}
		if err.Error() != "timeout waiting for service: nonexistent-service" {
			t.Errorf("Expected timeout error message, got: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Expected timeout within 50ms")
	}

	// Test 3: Service becomes available after waiting
	resultChan = broker.WaitForServiceAsync("delayed-service", 200*time.Millisecond)

	// Start a goroutine to add the service after a short delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		broker.Publish(moleculer.ServiceSchema{
			Name: "delayed-service",
			Actions: []moleculer.Action{
				{
					Name: "test",
					Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
						return params
					},
				},
			},
		})
	}()

	select {
	case err := <-resultChan:
		if err != nil {
			t.Errorf("Expected no error when service becomes available, got: %v", err)
		}
	case <-time.After(150 * time.Millisecond):
		t.Error("Expected service to become available within 150ms")
	}
}

func TestWaitForActionAsync(t *testing.T) {
	broker := New()
	broker.Start()
	defer broker.Stop()

	// Test 1: Action already available
	broker.Publish(moleculer.ServiceSchema{
		Name: "test-service",
		Actions: []moleculer.Action{
			{
				Name: "test-action",
				Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
					return params
				},
			},
		},
	})

	resultChan := broker.WaitForActionAsync("test-service.test-action", 100*time.Millisecond)
	select {
	case err := <-resultChan:
		if err != nil {
			t.Errorf("Expected no error for already available action, got: %v", err)
		}
	case <-time.After(50 * time.Millisecond):
		t.Error("Expected immediate result for already available action")
	}

	// Test 2: Action not available - timeout
	resultChan = broker.WaitForActionAsync("nonexistent.action", 50*time.Millisecond)
	select {
	case err := <-resultChan:
		if err == nil {
			t.Error("Expected timeout error for nonexistent action")
		}
		if err.Error() != "timeout waiting for action: nonexistent.action" {
			t.Errorf("Expected timeout error message, got: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Expected timeout within 50ms")
	}

	// Test 3: Action becomes available after waiting
	resultChan = broker.WaitForActionAsync("delayed-service.delayed-action", 200*time.Millisecond)

	// Start a goroutine to add the action after a short delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		broker.Publish(moleculer.ServiceSchema{
			Name: "delayed-service",
			Actions: []moleculer.Action{
				{
					Name: "delayed-action",
					Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
						return params
					},
				},
			},
		})
	}()

	select {
	case err := <-resultChan:
		if err != nil {
			t.Errorf("Expected no error when action becomes available, got: %v", err)
		}
	case <-time.After(150 * time.Millisecond):
		t.Error("Expected action to become available within 150ms")
	}
}

func TestWaitForNodeAsync(t *testing.T) {
	broker := New()
	broker.Start()
	defer broker.Stop()

	// Test 1: Node already available (local node)
	localNodeID := broker.LocalNode().GetID()
	resultChan := broker.WaitForNodeAsync(localNodeID, 100*time.Millisecond)
	select {
	case err := <-resultChan:
		if err != nil {
			t.Errorf("Expected no error for already available node, got: %v", err)
		}
	case <-time.After(50 * time.Millisecond):
		t.Error("Expected immediate result for already available node")
	}

	// Test 2: Node not available - timeout
	resultChan = broker.WaitForNodeAsync("nonexistent-node", 50*time.Millisecond)
	select {
	case err := <-resultChan:
		if err == nil {
			t.Error("Expected timeout error for nonexistent node")
		}
		if err.Error() != "timeout waiting for node: nonexistent-node" {
			t.Errorf("Expected timeout error message, got: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Expected timeout within 50ms")
	}
}

func TestWaitForDependenciesAsync(t *testing.T) {
	broker := New()
	broker.Start()
	defer broker.Stop()

	// Test 1: All dependencies already available
	broker.Publish(moleculer.ServiceSchema{
		Name: "dep1",
		Actions: []moleculer.Action{
			{
				Name: "test",
				Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
					return params
				},
			},
		},
	})
	broker.Publish(moleculer.ServiceSchema{
		Name: "dep2",
		Actions: []moleculer.Action{
			{
				Name: "test",
				Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
					return params
				},
			},
		},
	})

	resultChan := broker.WaitForDependenciesAsync([]string{"dep1", "dep2"}, 100*time.Millisecond)
	select {
	case err := <-resultChan:
		if err != nil {
			t.Errorf("Expected no error for already available dependencies, got: %v", err)
		}
	case <-time.After(50 * time.Millisecond):
		t.Error("Expected immediate result for already available dependencies")
	}

	// Test 2: Some dependencies not available - timeout
	resultChan = broker.WaitForDependenciesAsync([]string{"dep1", "nonexistent-dep"}, 50*time.Millisecond)
	select {
	case err := <-resultChan:
		if err == nil {
			t.Error("Expected timeout error for missing dependencies")
		}
		if err.Error() != "timeout waiting for dependencies: dep1, nonexistent-dep" {
			t.Errorf("Expected timeout error message, got: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Expected timeout within 50ms")
	}

	// Test 3: Dependencies become available after waiting
	resultChan = broker.WaitForDependenciesAsync([]string{"delayed-dep1", "delayed-dep2"}, 200*time.Millisecond)

	// Start a goroutine to add the dependencies after a short delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		broker.Publish(moleculer.ServiceSchema{
			Name: "delayed-dep1",
			Actions: []moleculer.Action{
				{
					Name: "test",
					Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
						return params
					},
				},
			},
		})
		time.Sleep(10 * time.Millisecond)
		broker.Publish(moleculer.ServiceSchema{
			Name: "delayed-dep2",
			Actions: []moleculer.Action{
				{
					Name: "test",
					Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
						return params
					},
				},
			},
		})
	}()

	select {
	case err := <-resultChan:
		if err != nil {
			t.Errorf("Expected no error when dependencies become available, got: %v", err)
		}
	case <-time.After(150 * time.Millisecond):
		t.Error("Expected dependencies to become available within 150ms")
	}
}

func TestWaitAsyncBrokerStop(t *testing.T) {
	broker := New()
	broker.Start()

	// Start waiting for a service that will never come
	resultChan := broker.WaitForServiceAsync("nonexistent-service", 5*time.Second)

	// Stop broker immediately
	broker.Stop()

	// Wait a bit to ensure goroutines have time to clean up
	time.Sleep(100 * time.Millisecond)

	// Check that no result was sent (goroutine was cancelled)
	select {
	case <-resultChan:
		t.Error("Expected no result due to broker stop")
	default:
		// Good - no result sent
	}
}

func TestWaitAsyncContextCancellation(t *testing.T) {
	broker := New()
	broker.Start()
	defer broker.Stop()

	// Test that context cancellation works
	resultChan := broker.WaitForServiceAsync("nonexistent-service", 5*time.Second)

	// Cancel the wait context directly
	broker.waitCancel()

	// Wait a bit to ensure goroutines have time to clean up
	time.Sleep(100 * time.Millisecond)

	// Check that no result was sent (goroutine was cancelled)
	select {
	case <-resultChan:
		t.Error("Expected no result due to context cancellation")
	default:
		// Good - no result sent
	}
}

func TestWaitAsyncConcurrent(t *testing.T) {
	broker := New()
	broker.Start()
	defer broker.Stop()

	// Test multiple concurrent waits
	const numWaits = 5
	resultChans := make([]<-chan error, numWaits)

	// Start multiple concurrent waits for different services
	for i := 0; i < numWaits; i++ {
		resultChans[i] = broker.WaitForServiceAsync("concurrent-service", 200*time.Millisecond)
	}

	// Add the service after a short delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		broker.Publish(moleculer.ServiceSchema{
			Name: "concurrent-service",
			Actions: []moleculer.Action{
				{
					Name: "test",
					Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
						return params
					},
				},
			},
		})
	}()

	// Check that all waits complete successfully
	for i, resultChan := range resultChans {
		select {
		case err := <-resultChan:
			if err != nil {
				t.Errorf("Wait %d failed: %v", i, err)
			}
		case <-time.After(150 * time.Millisecond):
			t.Errorf("Wait %d timed out", i)
		}
	}
}
