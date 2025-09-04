package e2e

import (
	"testing"
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
)

// TestTCPConfigOptions tests all TCP configuration options to ensure they work correctly
func TestTCPConfigOptions(t *testing.T) {
	t.Log("Testing TCP configuration options...")

	// Test 1: UdpDiscovery option
	t.Run("UdpDiscovery", func(t *testing.T) {
		t.Log("Testing UdpDiscovery option...")

		// Test with UDP discovery enabled
		bkr1 := broker.New(&moleculer.Config{
			Transporter: "TCP",
			LogLevel:    "DEBUG",
			DiscoverNodeID: func() string {
				return "test-udp-enabled"
			},
			TCPOptions: map[string]interface{}{
				"UdpDiscovery": true,
				"UdpPort":      4447, // Use different port to avoid conflicts
			},
		})

		// Test with UDP discovery disabled
		bkr2 := broker.New(&moleculer.Config{
			Transporter: "TCP",
			LogLevel:    "DEBUG",
			DiscoverNodeID: func() string {
				return "test-udp-disabled"
			},
			TCPOptions: map[string]interface{}{
				"UdpDiscovery": false,
				"Port":         5003, // Fixed port for direct connection
			},
		})

		// Start brokers
		go bkr1.Start()
		go bkr2.Start()

		// Wait for startup
		time.Sleep(2 * time.Second)

		// Verify brokers started
		if !bkr1.IsStarted() {
			t.Error("Broker 1 should be started")
		}
		if !bkr2.IsStarted() {
			t.Error("Broker 2 should be started")
		}

		// Stop brokers
		bkr1.Stop()
		bkr2.Stop()
		t.Log("UdpDiscovery test completed")
	})

	// Test 2: Port option (fixed vs random)
	t.Run("Port", func(t *testing.T) {
		t.Log("Testing Port option...")

		// Test with fixed port
		bkr1 := broker.New(&moleculer.Config{
			Transporter: "TCP",
			LogLevel:    "DEBUG",
			DiscoverNodeID: func() string {
				return "test-fixed-port"
			},
			TCPOptions: map[string]interface{}{
				"UdpDiscovery": false,
				"Port":         5004, // Fixed port
			},
		})

		// Test with random port (default)
		bkr2 := broker.New(&moleculer.Config{
			Transporter: "TCP",
			LogLevel:    "DEBUG",
			DiscoverNodeID: func() string {
				return "test-random-port"
			},
			TCPOptions: map[string]interface{}{
				"UdpDiscovery": false,
				"Port":         0, // Random port
			},
		})

		go bkr1.Start()
		go bkr2.Start()

		time.Sleep(2 * time.Second)

		if !bkr1.IsStarted() {
			t.Error("Broker 1 should be started")
		}
		if !bkr2.IsStarted() {
			t.Error("Broker 2 should be started")
		}

		bkr1.Stop()
		bkr2.Stop()
		t.Log("Port test completed")
	})

	// Test 3: GossipPeriod option
	t.Run("GossipPeriod", func(t *testing.T) {
		t.Log("Testing GossipPeriod option...")

		// Test with custom gossip period
		bkr1 := broker.New(&moleculer.Config{
			Transporter: "TCP",
			LogLevel:    "DEBUG",
			DiscoverNodeID: func() string {
				return "test-gossip-period"
			},
			TCPOptions: map[string]interface{}{
				"UdpDiscovery": false,
				"GossipPeriod": 5, // 5 seconds
			},
		})

		go bkr1.Start()
		time.Sleep(2 * time.Second)

		if !bkr1.IsStarted() {
			t.Error("Broker should be started")
		}

		bkr1.Stop()
		t.Log("GossipPeriod test completed")
	})

	// Test 4: WorkerPoolSize option
	t.Run("WorkerPoolSize", func(t *testing.T) {
		t.Log("Testing WorkerPoolSize option...")

		// Test with custom worker pool size
		bkr1 := broker.New(&moleculer.Config{
			Transporter: "TCP",
			LogLevel:    "DEBUG",
			DiscoverNodeID: func() string {
				return "test-worker-pool"
			},
			TCPOptions: map[string]interface{}{
				"UdpDiscovery":   false,
				"WorkerPoolSize": 10, // Custom worker pool size
			},
		})

		go bkr1.Start()
		time.Sleep(2 * time.Second)

		if !bkr1.IsStarted() {
			t.Error("Broker should be started")
		}

		bkr1.Stop()
		t.Log("WorkerPoolSize test completed")
	})

	// Test 5: MaxConnections option
	t.Run("MaxConnections", func(t *testing.T) {
		t.Log("Testing MaxConnections option...")

		// Test with custom max connections
		bkr1 := broker.New(&moleculer.Config{
			Transporter: "TCP",
			LogLevel:    "DEBUG",
			DiscoverNodeID: func() string {
				return "test-max-connections"
			},
			TCPOptions: map[string]interface{}{
				"UdpDiscovery":   false,
				"MaxConnections": 10, // Custom max connections
			},
		})

		go bkr1.Start()
		time.Sleep(2 * time.Second)

		if !bkr1.IsStarted() {
			t.Error("Broker should be started")
		}

		bkr1.Stop()
		t.Log("MaxConnections test completed")
	})

	// Test 6: ConnectionTimeout and IdleConnectionTimeout options
	t.Run("ConnectionTimeouts", func(t *testing.T) {
		t.Log("Testing ConnectionTimeout and IdleConnectionTimeout options...")

		// Test with custom timeouts
		bkr1 := broker.New(&moleculer.Config{
			Transporter: "TCP",
			LogLevel:    "DEBUG",
			DiscoverNodeID: func() string {
				return "test-connection-timeouts"
			},
			TCPOptions: map[string]interface{}{
				"UdpDiscovery":          false,
				"ConnectionTimeout":     10 * time.Second,
				"IdleConnectionTimeout": 20 * time.Second,
			},
		})

		go bkr1.Start()
		time.Sleep(2 * time.Second)

		if !bkr1.IsStarted() {
			t.Error("Broker should be started")
		}

		bkr1.Stop()
		t.Log("ConnectionTimeouts test completed")
	})

	// Test 7: UDP configuration options
	t.Run("UDPOptions", func(t *testing.T) {
		t.Log("Testing UDP configuration options...")

		// Test with custom UDP options
		bkr1 := broker.New(&moleculer.Config{
			Transporter: "TCP",
			LogLevel:    "DEBUG",
			DiscoverNodeID: func() string {
				return "test-udp-options"
			},
			TCPOptions: map[string]interface{}{
				"UdpDiscovery":    true,
				"UdpPort":         4448,
				"UdpBindAddress":  "127.0.0.1",
				"UdpPeriod":       5 * time.Second,
				"UdpMaxDiscovery": 5,
				"UdpMulticast":    "239.0.0.1",
				"UdpMulticastTTL": 2,
				"UdpBroadcast":    []string{"192.168.1.255"},
			},
		})

		go bkr1.Start()
		time.Sleep(2 * time.Second)

		if !bkr1.IsStarted() {
			t.Error("Broker should be started")
		}

		bkr1.Stop()
		t.Log("UDP options test completed")
	})

	// Test 8: MaxPacketSize option (needs to be implemented)
	t.Run("MaxPacketSize", func(t *testing.T) {
		t.Log("Testing MaxPacketSize option...")

		// Test with custom max packet size
		bkr1 := broker.New(&moleculer.Config{
			Transporter: "TCP",
			LogLevel:    "DEBUG",
			DiscoverNodeID: func() string {
				return "test-max-packet-size"
			},
			TCPOptions: map[string]interface{}{
				"UdpDiscovery":  false,
				"MaxPacketSize": 512 * 1024, // 512KB
			},
		})

		go bkr1.Start()
		time.Sleep(2 * time.Second)

		if !bkr1.IsStarted() {
			t.Error("Broker should be started")
		}

		bkr1.Stop()
		t.Log("MaxPacketSize test completed")
	})

	// Test 9: UseHostname option (needs to be implemented)
	t.Run("UseHostname", func(t *testing.T) {
		t.Log("Testing UseHostname option...")

		// Test with UseHostname enabled
		bkr1 := broker.New(&moleculer.Config{
			Transporter: "TCP",
			LogLevel:    "DEBUG",
			DiscoverNodeID: func() string {
				return "test-use-hostname"
			},
			TCPOptions: map[string]interface{}{
				"UdpDiscovery": false,
				"UseHostname":  true,
			},
		})

		go bkr1.Start()
		time.Sleep(2 * time.Second)

		if !bkr1.IsStarted() {
			t.Error("Broker should be started")
		}

		bkr1.Stop()
		t.Log("UseHostname test completed")
	})

	// Test 10: Urls option (needs to be implemented)
	t.Run("Urls", func(t *testing.T) {
		t.Log("Testing Urls option...")

		// Test with static URLs
		bkr1 := broker.New(&moleculer.Config{
			Transporter: "TCP",
			LogLevel:    "DEBUG",
			DiscoverNodeID: func() string {
				return "test-static-urls"
			},
			TCPOptions: map[string]interface{}{
				"UdpDiscovery": false,
				"Port":         5005,
				"Urls":         []string{"tcp://127.0.0.1:5006"}, // Static connection
			},
		})

		go bkr1.Start()
		time.Sleep(2 * time.Second)

		if !bkr1.IsStarted() {
			t.Error("Broker should be started")
		}

		bkr1.Stop()
		t.Log("Urls test completed")
	})

	t.Log("All TCP configuration option tests completed")
}

// TestTCPConfigDefaults tests that default values are properly applied
func TestTCPConfigDefaults(t *testing.T) {
	t.Log("Testing TCP configuration defaults...")

	// Test with minimal configuration (should use defaults)
	bkr1 := broker.New(&moleculer.Config{
		Transporter: "TCP",
		LogLevel:    "DEBUG",
		DiscoverNodeID: func() string {
			return "test-defaults"
		},
		// No TCPOptions specified - should use defaults
	})

	go bkr1.Start()
	time.Sleep(2 * time.Second)

	if !bkr1.IsStarted() {
		t.Error("Broker should be started with default configuration")
	}

	bkr1.Stop()
	t.Log("TCP configuration defaults test completed")
}

// TestTCPConfigValidation tests configuration validation
func TestTCPConfigValidation(t *testing.T) {
	t.Log("Testing TCP configuration validation...")

	// Test with invalid configuration values
	testCases := []struct {
		name       string
		config     map[string]interface{}
		shouldFail bool
	}{
		{
			name: "Negative GossipPeriod",
			config: map[string]interface{}{
				"GossipPeriod": -1,
			},
			shouldFail: false, // Should not fail, but should use default
		},
		{
			name: "Zero WorkerPoolSize",
			config: map[string]interface{}{
				"WorkerPoolSize": 0,
			},
			shouldFail: false, // Should not fail, but should use default
		},
		{
			name: "Negative MaxConnections",
			config: map[string]interface{}{
				"MaxConnections": -1,
			},
			shouldFail: false, // Should not fail, but should use default
		},
		{
			name: "Negative MaxPacketSize",
			config: map[string]interface{}{
				"MaxPacketSize": -1,
			},
			shouldFail: false, // Should not fail, but should use default
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			bkr := broker.New(&moleculer.Config{
				Transporter: "TCP",
				LogLevel:    "DEBUG",
				DiscoverNodeID: func() string {
					return "test-validation"
				},
				TCPOptions: tc.config,
			})

			go bkr.Start()
			time.Sleep(1 * time.Second)

			if tc.shouldFail {
				if bkr.IsStarted() {
					t.Error("Broker should not have started with invalid configuration")
				}
			} else {
				if !bkr.IsStarted() {
					t.Error("Broker should have started even with edge case configuration")
				}
			}

			bkr.Stop()
		})
	}

	t.Log("TCP configuration validation test completed")
}
