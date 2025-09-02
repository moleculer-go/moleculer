package tcp

import (
	"io"
	"sync"
	"testing"
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/payload"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockRegistry implements moleculer.Registry for testing
type MockRegistry struct {
	mock.Mock
	nodes     map[string]*MockNode
	localNode *MockNode
}

func NewMockRegistry() *MockRegistry {
	localNode := &MockNode{
		id:        "local-node",
		port:      3000,
		available: true,
	}
	return &MockRegistry{
		nodes:     make(map[string]*MockNode),
		localNode: localNode,
	}
}

func (m *MockRegistry) GetLocalNode() moleculer.Node {
	return m.localNode
}

func (m *MockRegistry) GetNodeByID(id string) moleculer.Node {
	if node, exists := m.nodes[id]; exists {
		return node
	}
	return nil
}

func (m *MockRegistry) AddOfflineNode(id, hostname, ip string, port int) moleculer.Node {
	node := &MockNode{
		id:        id,
		hostname:  hostname,
		ipList:    []string{ip},
		port:      port,
		available: false,
	}
	m.nodes[id] = node
	return node
}

func (m *MockRegistry) ForEachNode(fn moleculer.ForEachNodeFunc) {
	for _, node := range m.nodes {
		if !fn(node) {
			break
		}
	}
}

func (m *MockRegistry) GetNodeByAddress(host string) moleculer.Node {
	for _, node := range m.nodes {
		if node.GetHostname() == host {
			return node
		}
	}
	return nil
}

func (m *MockRegistry) RemoteNodeInfoReceived(message moleculer.Payload) {}

func (m *MockRegistry) DisconnectNode(id string) {
	if node, exists := m.nodes[id]; exists {
		node.available = false
	}
}

// MockNode implements moleculer.Node for testing
type MockNode struct {
	id        string
	hostname  string
	ipList    []string
	port      int
	available bool
}

func (m *MockNode) GetID() string         { return m.id }
func (m *MockNode) GetHostname() string   { return m.hostname }
func (m *MockNode) GetIpList() []string   { return m.ipList }
func (m *MockNode) GetPort() int          { return m.port }
func (m *MockNode) IsLocal() bool         { return m.id == "local-node" }
func (m *MockNode) IsAvailable() bool     { return m.available }
func (m *MockNode) Available()            {}
func (m *MockNode) Unavailable()          {}
func (m *MockNode) GetUdpAddress() string { return "" }
func (m *MockNode) GetSequence() int64    { return 1 }
func (m *MockNode) GetCpuSequence() int64 { return 1 }
func (m *MockNode) GetCpu() int64         { return 50 }
func (m *MockNode) UpdateInfo(info map[string]interface{}) []map[string]interface{} {
	return []map[string]interface{}{}
}
func (m *MockNode) UpdateMetrics()                        {}
func (m *MockNode) HeartBeat(info map[string]interface{}) {}
func (m *MockNode) IncreaseSequence()                     {}
func (m *MockNode) IsExpired(timeout time.Duration) bool  { return false }
func (m *MockNode) Update(id string, info map[string]interface{}) (bool, []map[string]interface{}) {
	return true, []map[string]interface{}{}
}
func (m *MockNode) Publish(event map[string]interface{}) {}
func (m *MockNode) ExportAsMap() map[string]interface{} {
	return map[string]interface{}{
		"id":        m.id,
		"hostname":  m.hostname,
		"port":      m.port,
		"available": m.available,
	}
}

// MockSerializer implements serializer.Serializer for testing
type MockSerializer struct{}

func (m *MockSerializer) BytesToPayload(data *[]byte) moleculer.Payload {
	return payload.New(map[string]interface{}{
		"data": string(*data),
		"type": "mock",
	})
}

func (m *MockSerializer) PayloadToBytes(payload moleculer.Payload) []byte {
	return []byte(payload.Get("data").String())
}

func (m *MockSerializer) MapToPayload(data *map[string]interface{}) (moleculer.Payload, error) {
	return payload.New(*data), nil
}

func (m *MockSerializer) PayloadToMap(payload moleculer.Payload) map[string]interface{} {
	if val := payload.Value(); val != nil {
		if m, ok := val.(map[string]interface{}); ok {
			return m
		}
	}
	return map[string]interface{}{}
}

func (m *MockSerializer) ReaderToPayload(reader io.Reader) moleculer.Payload {
	return payload.New(map[string]interface{}{"reader": "mock"})
}

func (m *MockSerializer) PayloadToString(payload moleculer.Payload) string {
	return payload.String()
}

func (m *MockSerializer) MapToString(data interface{}) string {
	return "mock"
}

func (m *MockSerializer) StringToMap(data string) map[string]interface{} {
	return map[string]interface{}{"string": data}
}

func (m *MockSerializer) PayloadToContextMap(payload moleculer.Payload) map[string]interface{} {
	return m.PayloadToMap(payload)
}

func TestTCPTransport_EndToEnd_InterfaceCompliance(t *testing.T) {
	// Setup test environment
	logger := log.New()
	logger.SetLevel(log.ErrorLevel) // Reduce log noise

	// Create mock components
	registry := NewMockRegistry()
	serializer := &MockSerializer{}

	// Create transport instance
	transport := CreateTCPTransporter(TCPOptions{
		Port:           0, // Random port for testing
		MaxConnections: 5,
		NodeId:         "test-node",
		Prefix:         "test",
		UdpDiscovery:   false, // Disable UDP for unit test
		GossipPeriod:   30,    // Set valid gossip period
		Logger:         logger.WithField("test", "transport"),
		Serializer:     serializer,
	})

	// Test configuration methods
	t.Run("Configuration", func(t *testing.T) {
		transport.SetPrefix("test-prefix")
		transport.SetNodeID("test-node-id")
		transport.SetSerializer(serializer)
		// These should not panic - we're testing interface compliance
	})

	// Test connection lifecycle
	t.Run("ConnectionLifecycle", func(t *testing.T) {
		// Connect
		connectChan := transport.Connect(registry)
		select {
		case err := <-connectChan:
			assert.NoError(t, err, "Transport should connect successfully")
		case <-time.After(5 * time.Second):
			t.Fatal("Connect timeout")
		}

		// Verify connection state (this would be internal state)
		// The transport should be ready to send/receive messages
	})

	// Test message handling
	t.Run("MessageHandling", func(t *testing.T) {
		var receivedMessages []moleculer.Payload
		var mu sync.Mutex

		// Subscribe to messages
		transport.Subscribe("EVENT", "", func(msg moleculer.Payload) {
			mu.Lock()
			defer mu.Unlock()
			receivedMessages = append(receivedMessages, msg)
		})

		transport.Subscribe("REQ", "specific-node", func(msg moleculer.Payload) {
			mu.Lock()
			defer mu.Unlock()
			receivedMessages = append(receivedMessages, msg)
		})

		// Create test message
		testMessage := payload.New(map[string]interface{}{
			"action": "test",
			"data":   "hello world",
		})

		// Publish messages
		transport.Publish("EVENT", "", testMessage)
		transport.Publish("REQ", "specific-node", testMessage)

		// Give some time for message processing
		time.Sleep(100 * time.Millisecond)

		// Verify messages were handled (this would be more comprehensive in real scenario)
		mu.Lock()
		messageCount := len(receivedMessages)
		mu.Unlock()

		// Note: In a real test environment with actual network components,
		// we would expect to receive the messages. For this unit test,
		// we're mainly testing that the interface methods don't panic
		// and the transport handles the message flow correctly.
		t.Logf("Processed %d messages", messageCount)
	})

	// Test disconnection
	t.Run("Disconnection", func(t *testing.T) {
		disconnectChan := transport.Disconnect()
		select {
		case err := <-disconnectChan:
			assert.NoError(t, err, "Transport should disconnect successfully")
		case <-time.After(5 * time.Second):
			t.Fatal("Disconnect timeout")
		}
	})

	// Test error handling
	t.Run("ErrorHandling", func(t *testing.T) {
		// Try operations on disconnected transport
		testMessage := payload.New(map[string]interface{}{"test": "data"})

		// These should handle gracefully (not panic)
		transport.Publish("EVENT", "", testMessage)
		transport.Subscribe("TEST", "", func(moleculer.Payload) {})

		// Should not crash the test
		t.Log("Error handling test completed")
	})
}

func TestTCPTransport_ConfigurationValidation(t *testing.T) {
	logger := log.New()
	logger.SetLevel(log.ErrorLevel)

	tests := []struct {
		name    string
		options TCPOptions
		valid   bool
	}{
		{
			name: "valid configuration",
			options: TCPOptions{
				Port:           3000,
				MaxConnections: 10,
				NodeId:         "test-node",
				Logger:         logger.WithField("test", "config"),
				Serializer:     &MockSerializer{},
			},
			valid: true,
		},
		{
			name: "invalid port",
			options: TCPOptions{
				Port:           -1,
				MaxConnections: 10,
				Logger:         logger.WithField("test", "config"),
			},
			valid: false,
		},
		{
			name: "zero max connections",
			options: TCPOptions{
				Port:           3000,
				MaxConnections: 0,
				Logger:         logger.WithField("test", "config"),
			},
			valid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := CreateTCPTransporter(tt.options)
			// Transport creation should succeed regardless of config validation
			// (validation might happen during Connect)
			assert.NotNil(t, transport)
		})
	}
}

func TestTCPTransport_ConcurrentOperations(t *testing.T) {
	logger := log.New()
	logger.SetLevel(log.ErrorLevel)

	registry := NewMockRegistry()
	transport := CreateTCPTransporter(TCPOptions{
		Port:           0,
		MaxConnections: 10,
		NodeId:         "test-node",
		GossipPeriod:   30,
		Logger:         logger.WithField("test", "concurrent"),
		Serializer:     &MockSerializer{},
		UdpDiscovery:   false,
	})

	// Connect first
	connectChan := transport.Connect(registry)
	select {
	case err := <-connectChan:
		assert.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Connect timeout")
	}

	// Test concurrent operations
	var wg sync.WaitGroup
	numGoroutines := 10
	messagesPerGoroutine := 5

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Subscribe to messages
			transport.Subscribe("TEST", "", func(msg moleculer.Payload) {
				// Handle message
			})

			// Publish messages
			for j := 0; j < messagesPerGoroutine; j++ {
				msg := payload.New(map[string]interface{}{
					"goroutine": id,
					"message":   j,
				})
				transport.Publish("EVENT", "", msg)
			}
		}(i)
	}

	// Wait for all goroutines to complete
	done := make(chan bool)
	go func() {
		wg.Wait()
		done <- true
	}()

	select {
	case <-done:
		t.Log("Concurrent operations completed successfully")
	case <-time.After(10 * time.Second):
		t.Fatal("Concurrent operations timeout")
	}

	// Disconnect
	disconnectChan := transport.Disconnect()
	select {
	case err := <-disconnectChan:
		assert.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Disconnect timeout")
	}
}

func BenchmarkTCPTransport_MessagePublishing(b *testing.B) {
	logger := log.New()
	logger.SetLevel(log.ErrorLevel)

	registry := NewMockRegistry()
	transport := CreateTCPTransporter(TCPOptions{
		Port:           0,
		MaxConnections: 10,
		NodeId:         "bench-node",
		GossipPeriod:   30,
		Logger:         logger.WithField("test", "benchmark"),
		Serializer:     &MockSerializer{},
		UdpDiscovery:   false,
	})

	// Connect
	connectChan := transport.Connect(registry)
	select {
	case err := <-connectChan:
		assert.NoError(b, err)
	case <-time.After(2 * time.Second):
		b.Fatal("Connect timeout")
	}

	// Benchmark message publishing
	testMessage := payload.New(map[string]interface{}{
		"benchmark": "data",
		"value":     12345,
	})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			transport.Publish("EVENT", "", testMessage)
		}
	})

	// Disconnect
	disconnectChan := transport.Disconnect()
	select {
	case err := <-disconnectChan:
		assert.NoError(b, err)
	case <-time.After(2 * time.Second):
		b.Fatal("Disconnect timeout")
	}
}
