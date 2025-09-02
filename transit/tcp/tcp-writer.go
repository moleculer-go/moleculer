package tcp

import (
	"encoding/binary"
	"fmt"
	"net"
	"time"

	log "github.com/sirupsen/logrus"
)

const HEADER_SIZE = 6

type TcpWriter struct {
	connPool   *ConnectionPool
	bufferPool *BufferPool
	logger     *log.Entry
}

func NewTcpWriter(maxConnections int, logger *log.Entry) *TcpWriter {
	// Initialize connection pool
	connPoolConfig := ConnectionPoolConfig{
		MaxConnections:      maxConnections,
		IdleTimeout:         5 * time.Minute,
		HealthCheckInterval: 30 * time.Second,
	}
	connPool := NewConnectionPool(connPoolConfig, logger.WithField("component", "connection-pool"))

	return &TcpWriter{
		connPool:   connPool,
		bufferPool: NewBufferPool(),
		logger:     logger,
	}
}

func (w *TcpWriter) Connect(nodeID, host string, port int) (*net.TCPConn, error) {
	// Use connection pool to get or create connection
	conn, err := w.connPool.Get(nodeID, host, port)
	if err != nil {
		return nil, NewTransportErrorWithAddress("connect", nodeID,
			fmt.Sprintf("%s:%d", host, port), err)
	}

	// Convert to TCPConn for compatibility
	if tcpConn, ok := conn.GetConn().(*net.TCPConn); ok {
		return tcpConn, nil
	}

	// Fallback for non-TCP connections
	return nil, NewTransportError("connect",
		fmt.Errorf("connection is not a TCP connection"))
}

func (w *TcpWriter) IsConnected(nodeID string) bool {
	// For now, we'll need to check if we can get a connection
	// In a future version, we could add a separate method to check connectivity
	_, err := w.connPool.Get(nodeID, "", 0)
	return err == nil
}

func (w *TcpWriter) Broadcast(msgType byte, msgBytes []byte) error {
	// For now, broadcast is not implemented with the new connection pool
	// We'll need to add a method to enumerate all connections in the pool
	w.logger.Warn("Broadcast not yet implemented with connection pool")
	return NewTransportError("broadcast", fmt.Errorf("broadcast not implemented"))
}

func (w *TcpWriter) Send(nodeID string, msgType byte, msgBytes []byte) error {
	// Get connection from pool - for existing connections, we don't need address/port
	// The connection pool will reuse existing connections by nodeID
	conn, err := w.connPool.Get(nodeID, "", 0)
	if err != nil {
		return NewTransportErrorWithNode("send", nodeID, err)
	}

	// Use buffer pool for header construction
	header := w.bufferPool.Get(HEADER_SIZE)
	defer w.bufferPool.Put(header)

	// Construct header
	totalLen := uint32(len(msgBytes) + HEADER_SIZE)
	binary.BigEndian.PutUint32(header[1:], totalLen)
	header[5] = msgType

	// Calculate CRC
	crc := header[1] ^ header[2] ^ header[3] ^ header[4] ^ header[5]
	header[0] = crc

	// Construct payload
	payload := append(header, msgBytes...)

	// Send with timeout
	conn.GetConn().SetWriteDeadline(time.Now().Add(30 * time.Second))
	_, err = conn.GetConn().Write(payload)

	if err != nil {
		// Remove failed connection from pool
		w.connPool.Remove(nodeID)
		return NewTransportErrorWithNode("send", nodeID, err)
	}

	return nil
}

func (w *TcpWriter) Close() {
	if w.connPool != nil {
		w.connPool.Close()
	}
	w.logger.Info("TCP writer closed")
}
