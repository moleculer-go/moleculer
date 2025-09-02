package tcp

import (
	"container/list"
	"fmt"
	"net"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// ConnectionPoolConfig holds configuration for the connection pool
type ConnectionPoolConfig struct {
	MaxConnections      int
	IdleTimeout         time.Duration
	HealthCheckInterval time.Duration
}

// Connection represents a pooled network connection
type Connection struct {
	conn      net.Conn
	nodeID    string
	address   string
	port      int
	createdAt time.Time
	lastUsed  time.Time
	isHealthy bool
	mutex     sync.RWMutex
}

// NewConnection creates a new connection
func NewConnection(nodeID, address string, port int, conn net.Conn) *Connection {
	now := time.Now()
	return &Connection{
		conn:      conn,
		nodeID:    nodeID,
		address:   address,
		port:      port,
		createdAt: now,
		lastUsed:  now,
		isHealthy: true,
	}
}

// Close closes the connection
func (c *Connection) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.isHealthy = false
	return c.conn.Close()
}

// IsHealthy checks if the connection is healthy
func (c *Connection) IsHealthy() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.isHealthy
}

// UpdateLastUsed updates the last used timestamp
func (c *Connection) UpdateLastUsed() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.lastUsed = time.Now()
}

// GetConn returns the underlying network connection
func (c *Connection) GetConn() net.Conn {
	return c.conn
}

// ConnectionPool manages a pool of reusable connections
type ConnectionPool struct {
	config      ConnectionPoolConfig
	connections sync.Map // map[string]*Connection
	idleList    *list.List
	idleMutex   sync.Mutex
	logger      *log.Entry
}

// NewConnectionPool creates a new connection pool
func NewConnectionPool(config ConnectionPoolConfig, logger *log.Entry) *ConnectionPool {
	cp := &ConnectionPool{
		config:   config,
		idleList: list.New(),
		logger:   logger,
	}

	// Start cleanup routine
	go cp.cleanupRoutine()

	return cp
}

// Get retrieves or creates a connection for the specified node
func (cp *ConnectionPool) Get(nodeID, address string, port int) (*Connection, error) {
	// Try to get existing connection
	if conn, exists := cp.connections.Load(nodeID); exists {
		connection := conn.(*Connection)
		if connection.IsHealthy() {
			connection.UpdateLastUsed()
			cp.logger.Debugf("Reusing connection for node %s", nodeID)
			return connection, nil
		}
		// Connection is unhealthy, remove it
		cp.connections.Delete(nodeID)
		connection.Close()
	}

	// If no address/port provided, we can't create a new connection
	if address == "" || port == 0 {
		return nil, NewTransportError("connect", fmt.Errorf("no existing connection for node %s and no address/port provided to create new connection", nodeID))
	}

	// Create new connection
	conn, err := cp.createConnection(address, port)
	if err != nil {
		return nil, NewTransportErrorWithAddress("connect", nodeID, address, err)
	}

	connection := NewConnection(nodeID, address, port, conn)
	cp.connections.Store(nodeID, connection)

	cp.logger.Debugf("Created new connection for node %s", nodeID)
	return connection, nil
}

// createConnection establishes a new TCP connection
func (cp *ConnectionPool) createConnection(address string, port int) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", address, port), 30*time.Second)
	if err != nil {
		return nil, err
	}

	// Set connection options
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true)
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(3 * time.Minute)
	}

	return conn, nil
}

// Remove removes a connection from the pool
func (cp *ConnectionPool) Remove(nodeID string) {
	if conn, exists := cp.connections.LoadAndDelete(nodeID); exists {
		connection := conn.(*Connection)
		connection.Close()
		cp.logger.Debugf("Removed connection for node %s", nodeID)
	}
}

// Close closes all connections in the pool
func (cp *ConnectionPool) Close() {
	cp.connections.Range(func(key, value interface{}) bool {
		connection := value.(*Connection)
		connection.Close()
		return true
	})
	cp.connections = sync.Map{} // Clear the map
	cp.logger.Info("Connection pool closed")
}

// Stats returns pool statistics
func (cp *ConnectionPool) Stats() map[string]interface{} {
	stats := make(map[string]interface{})
	count := 0
	cp.connections.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	stats["active_connections"] = count
	stats["max_connections"] = cp.config.MaxConnections
	return stats
}

// cleanupRoutine periodically cleans up idle connections
func (cp *ConnectionPool) cleanupRoutine() {
	ticker := time.NewTicker(cp.config.IdleTimeout / 4) // Check every 1/4 of idle timeout
	defer ticker.Stop()

	for range ticker.C {
		cp.cleanupIdleConnections()
	}
}

// cleanupIdleConnections removes connections that have been idle too long
func (cp *ConnectionPool) cleanupIdleConnections() {
	now := time.Now()
	removed := 0

	cp.connections.Range(func(key, value interface{}) bool {
		connection := value.(*Connection)
		if now.Sub(connection.lastUsed) > cp.config.IdleTimeout {
			cp.Remove(key.(string))
			removed++
		}
		return true
	})

	if removed > 0 {
		cp.logger.Debugf("Cleaned up %d idle connections", removed)
	}
}
