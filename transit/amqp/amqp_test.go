package amqp

import (
	"sync"
	"testing"
	"time"

	"github.com/moleculer-go/moleculer/broker"
)

// Test configuration
var (
	queues = []string{
		"MOL.REQ.hello",
		"MOL.RES.hello",
		"MOL.REQ.test.hello",
		"MOL.RES.test.hello",
		"MOL.EVENT.hello.world",
		"MOL.EVENT.hello.world2",
		"MOL.REQ.test-rpc",
		"MOL.RES.test-rpc",
		"MOL.REQ.client",
		"MOL.RES.client",
		"MOL.REQ.worker1",
		"MOL.RES.worker1",
		"MOL.REQ.worker2",
		"MOL.RES.worker2",
		"MOL.REQ.worker3",
		"MOL.RES.worker3",
		"MOL.REQ.pub",
		"MOL.RES.pub",
		"MOL.REQ.sub1",
		"MOL.RES.sub1",
		"MOL.REQ.sub2",
		"MOL.RES.sub2",
		"MOL.REQ.sub3",
		"MOL.RES.sub3",
	}

	exchanges = []string{
		"MOL.REQ",
		"MOL.RES",
		"MOL.EVENT",
		"MOL.HEARTBEAT",
		"MOL.INFO",
		"MOL.PING",
	}
)

// Test functions
func TestAMQPTransporter_RPC_OnlyOneNodeReceivesRequest(t *testing.T) {
	// Clear queues before test
	purge(queues, exchanges, true)

	// Setup RPC test
	var logs []map[string]interface{}
	client := createNode("test-rpc", "client", nil)
	worker1 := createActionWorker(1, &logs)
	worker2 := createActionWorker(2, &logs)
	worker3 := createActionWorker(3, &logs)
	brokers := []*broker.ServiceBroker{client, worker1, worker2, worker3}

	// Start all brokers
	for _, bkr := range brokers {
		bkr.Start()
	}

	// Wait for all workers to be registered and ready
	maxRetries := 10
	for retry := 0; retry < maxRetries; retry++ {
		successCount := 0
		for i := 0; i < 3; i++ {
			testResult := <-client.Call("test.hello", map[string]interface{}{"delay": 10})
			if testResult.Error() == nil {
				successCount++
			}
		}
		if successCount >= 3 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	logs = nil

	// Test logic
	client = brokers[0] // client is first broker
	result := <-client.Call("test.hello", map[string]interface{}{"delay": 20})
	if result.Error() != nil {
		t.Errorf("Expected no error, got %v", result.Error())
	}
	if len(logs) != 2 {
		t.Errorf("Expected 2 logs, got %d", len(logs))
	}
	receiveLogs := filter(&logs, "receive")
	if len(receiveLogs) != 1 {
		t.Errorf("Expected 1 receive log, got %d", len(receiveLogs))
	}
	respondLogs := filter(&logs, "respond")
	if len(respondLogs) != 1 {
		t.Errorf("Expected 1 respond log, got %d", len(respondLogs))
	}

	// Cleanup
	for _, bkr := range brokers {
		bkr.Stop()
	}
	time.Sleep(time.Second)
	purge(queues, exchanges, true)
}

func TestAMQPTransporter_RPC_LoadBalanceRequests(t *testing.T) {
	// Clear queues before test
	purge(queues, exchanges, true)

	// Setup RPC test
	var logs []map[string]interface{}
	client := createNode("test-rpc", "client", nil)
	worker1 := createActionWorker(1, &logs)
	worker2 := createActionWorker(2, &logs)
	worker3 := createActionWorker(3, &logs)
	brokers := []*broker.ServiceBroker{client, worker1, worker2, worker3}

	// Start all brokers
	for _, bkr := range brokers {
		bkr.Start()
	}

	// Wait for all workers to be registered and ready
	maxRetries := 10
	for retry := 0; retry < maxRetries; retry++ {
		successCount := 0
		for i := 0; i < 3; i++ {
			testResult := <-client.Call("test.hello", map[string]interface{}{"delay": 10})
			if testResult.Error() == nil {
				successCount++
			}
		}
		if successCount >= 3 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	logs = nil

	// Test logic - ensure that messages are evenly distributed
	client = brokers[0] // client is first broker
	wg := sync.WaitGroup{}
	res := make([]int, 12)
	errors := make([]error, 12)

	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			payload := <-client.Call("test.hello", map[string]interface{}{"delay": 20})
			if payload.Error() != nil {
				errors[index] = payload.Error()
				return
			}
			res[index] = payload.Get("worker").Int()
		}(i)
	}

	wg.Wait()

	// Check for any errors first
	for i, err := range errors {
		if err != nil {
			t.Errorf("Request %d failed: %v", i, err)
		}
	}

	if len(res) != 12 {
		t.Errorf("Expected 12 results, got %d", len(res))
	}

	// Count occurrences of each worker
	workerCounts := make(map[int]int)
	for _, worker := range res {
		workerCounts[worker]++
	}

	// Verify all workers received requests
	if _, ok := workerCounts[1]; !ok {
		t.Error("Worker 1 should have received requests")
	}
	if _, ok := workerCounts[2]; !ok {
		t.Error("Worker 2 should have received requests")
	}
	if _, ok := workerCounts[3]; !ok {
		t.Error("Worker 3 should have received requests")
	}

	// Verify load balancing is reasonably even (each worker gets 3-5 requests)
	for worker, count := range workerCounts {
		if count < 3 {
			t.Errorf("Worker %d should receive at least 3 requests, got %d", worker, count)
		}
		if count > 5 {
			t.Errorf("Worker %d should receive at most 5 requests, got %d", worker, count)
		}
	}

	// Cleanup
	for _, bkr := range brokers {
		bkr.Stop()
	}
	time.Sleep(time.Second)
	purge(queues, exchanges, true)
}

func TestAMQPTransporter_RPC_OneRequestAtATime(t *testing.T) {
	// Clear queues before test
	purge(queues, exchanges, true)

	// Setup RPC test
	var logs []map[string]interface{}
	client := createNode("test-rpc", "client", nil)
	worker1 := createActionWorker(1, &logs)
	worker2 := createActionWorker(2, &logs)
	worker3 := createActionWorker(3, &logs)
	brokers := []*broker.ServiceBroker{client, worker1, worker2, worker3}

	// Start all brokers
	for _, bkr := range brokers {
		bkr.Start()
	}

	// Wait for all workers to be registered and ready
	maxRetries := 10
	for retry := 0; retry < maxRetries; retry++ {
		successCount := 0
		for i := 0; i < 3; i++ {
			testResult := <-client.Call("test.hello", map[string]interface{}{"delay": 10})
			if testResult.Error() == nil {
				successCount++
			}
		}
		if successCount >= 3 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	logs = nil

	// Test logic - ensure that prefetch is working
	client = brokers[0]  // client is first broker
	worker2 = brokers[2] // worker2 is third broker
	worker3 = brokers[3] // worker3 is fourth broker

	worker2.Stop()
	worker3.Stop()

	wg := sync.WaitGroup{}
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(index int) {
			<-client.Call("test.hello", map[string]interface{}{"delay": 20})
			wg.Done()
		}(i)
	}
	wg.Wait()

	for idx, cur := range logs {
		// All requests should be handled by single node
		if cur["worker"] != 1 {
			t.Errorf("Expected worker 1, got %v", cur["worker"])
		}

		// Order should go from old -> new
		if idx+1 < len(logs) {
			curTime := cur["timestamp"].(time.Time)
			nextTime := logs[idx+1]["timestamp"].(time.Time)
			if curTime.After(nextTime) {
				t.Errorf("Logs should be in chronological order")
			}
		}

		// If receive and respond don't alternate requests are concurrent
		if idx%2 == 0 {
			if cur["type"] != "receive" {
				t.Errorf("Expected receive, got %v", cur["type"])
			}
		} else {
			if cur["type"] != "respond" {
				t.Errorf("Expected respond, got %v", cur["type"])
			}
		}
	}

	// Cleanup
	for _, bkr := range brokers {
		bkr.Stop()
	}
	time.Sleep(time.Second)
	purge(queues, exchanges, true)
}

func TestAMQPTransporter_Emit_OnlyOneService(t *testing.T) {
	// Clear queues before test
	purge(queues, exchanges, true)

	// Setup emit test
	var emitLogs []string
	pub := createEmitWorker("pub", "emit-handler", &emitLogs)
	sub1 := createEmitWorker("sub1", "emit-handler", &emitLogs)
	sub2 := createEmitWorker("sub2", "emit-handler", &emitLogs)
	sub3 := createEmitWorker("sub3", "other-handler", &emitLogs)

	// Start all workers
	pub.Start()
	sub1.Start()
	sub2.Start()
	sub3.Start()

	time.Sleep(time.Second)

	// Test logic
	for i := 0; i < 6; i++ {
		pub.Emit("hello.world2", map[string]interface{}{"testing": true})
	}

	time.Sleep(2 * time.Second)

	if len(emitLogs) != 12 {
		t.Errorf("Expected 12 logs, got %d", len(emitLogs))
	}

	// Check that pub and sub3 are in logs
	hasPub := false
	hasSub3 := false
	for _, log := range emitLogs {
		if log == "pub" {
			hasPub = true
		}
		if log == "sub3" {
			hasSub3 = true
		}
	}
	if !hasPub {
		t.Error("Expected 'pub' in logs")
	}
	if !hasSub3 {
		t.Error("Expected 'sub3' in logs")
	}

	// Count sub3 occurrences
	sub3Count := 0
	for _, log := range emitLogs {
		if log == "sub3" {
			sub3Count++
		}
	}
	if sub3Count != 6 {
		t.Errorf("Expected 6 'sub3' logs, got %d", sub3Count)
	}

	// Cleanup
	pub.Stop()
	sub1.Stop()
	sub2.Stop()
	sub3.Stop()
	time.Sleep(time.Second)
	purge(queues, exchanges, true)
}

func TestAMQPTransporter_Broadcast_AllSubscribedNodes(t *testing.T) {
	// Clear queues before test
	purge(queues, exchanges, true)

	// Setup broadcast test
	var broadcastLogs []string
	pub := createBroadcastWorker("pub", &broadcastLogs)
	sub1 := createBroadcastWorker("sub1", &broadcastLogs)
	sub2 := createBroadcastWorker("sub2", &broadcastLogs)
	sub3 := createBroadcastWorker("sub3", &broadcastLogs)

	// Start all workers first
	pub.Start()
	sub1.Start()
	sub2.Start()
	sub3.Start()

	// Wait longer for all AMQP connections to be established
	time.Sleep(3 * time.Second)

	// Test logic
	pub.Broadcast("hello.world", map[string]interface{}{"testing": true})

	time.Sleep(3 * time.Second)

	if len(broadcastLogs) != 4 {
		t.Errorf("Expected 4 logs, got %d: %v", len(broadcastLogs), broadcastLogs)
	}

	// Check that all expected nodes are in logs
	expectedNodes := []string{"pub", "sub1", "sub2", "sub3"}
	for _, expected := range expectedNodes {
		found := false
		for _, log := range broadcastLogs {
			if log == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected '%s' in logs, got %v", expected, broadcastLogs)
		}
	}

	// Cleanup
	pub.Stop()
	sub1.Stop()
	sub2.Stop()
	sub3.Stop()
	time.Sleep(time.Second)
	purge(queues, exchanges, true)
}
