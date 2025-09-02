package tcp

import (
	"net"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestConnection_NewConnection(t *testing.T) {
	// Create a mock connection (we'll use a pipe for testing)
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	conn := NewConnection("test-node", "127.0.0.1", 3000, client)

	assert.Equal(t, "test-node", conn.nodeID)
	assert.Equal(t, "127.0.0.1", conn.address)
	assert.Equal(t, 3000, conn.port)
	assert.True(t, conn.IsHealthy())
	assert.Equal(t, client, conn.GetConn())
}

func TestConnection_IsHealthy(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()

	conn := NewConnection("test-node", "127.0.0.1", 3000, client)

	// Initially healthy
	assert.True(t, conn.IsHealthy())

	// Close connection
	conn.Close()

	// Should be unhealthy
	assert.False(t, conn.IsHealthy())
}

func TestConnectionPool_Get(t *testing.T) {
	logger := log.New()
	logger.SetLevel(log.ErrorLevel)
	config := ConnectionPoolConfig{
		MaxConnections: 5,
		IdleTimeout:    time.Minute,
	}

	// Note: This test would need a real server to test actual connections
	// For now, we'll test the pool structure
	cp := NewConnectionPool(config, logger.WithField("test", "connection_pool"))
	defer cp.Close()

	// Test pool creation
	assert.NotNil(t, cp)
	assert.NotNil(t, cp.logger)

	// Test stats
	stats := cp.Stats()
	assert.Contains(t, stats, "active_connections")
	assert.Contains(t, stats, "max_connections")
	assert.Equal(t, 5, stats["max_connections"])
}

func TestConnectionPool_Remove(t *testing.T) {
	logger := log.New()
	logger.SetLevel(log.ErrorLevel)
	config := ConnectionPoolConfig{
		MaxConnections: 5,
		IdleTimeout:    time.Minute,
	}

	cp := NewConnectionPool(config, logger.WithField("test", "connection_pool"))
	defer cp.Close()

	// Try to remove non-existent connection (should not panic)
	cp.Remove("non-existent-node")
}

func TestConnectionPool_Close(t *testing.T) {
	logger := log.New()
	logger.SetLevel(log.ErrorLevel)
	config := ConnectionPoolConfig{
		MaxConnections: 5,
		IdleTimeout:    time.Minute,
	}

	cp := NewConnectionPool(config, logger.WithField("test", "connection_pool"))
	cp.Close()

	// Should not panic on double close
	cp.Close()
}

func BenchmarkConnectionPool_Get(b *testing.B) {
	logger := log.New()
	logger.SetLevel(log.ErrorLevel)
	config := ConnectionPoolConfig{
		MaxConnections: 10,
		IdleTimeout:    time.Minute,
	}

	cp := NewConnectionPool(config, logger.WithField("test", "benchmark"))
	defer cp.Close()

	// Note: This benchmark would need real connections to be meaningful
	// For now, it just tests the pool structure overhead
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// This will fail but tests the pool logic
		_, _ = cp.Get("node1", "127.0.0.1", 3000)
	}
}
