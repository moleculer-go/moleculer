package tcp

import (
	"sync"
	"time"
)

// BufferPool provides reusable buffers to reduce garbage collection pressure
type BufferPool struct {
	smallPool  sync.Pool // For small buffers (1KB)
	mediumPool sync.Pool // For medium buffers (4KB)
	largePool  sync.Pool // For large buffers (16KB)

	// Optional callback for metrics events
	onBufferEvent func(eventType string, size int)
}

// NewBufferPool creates a new buffer pool with optimized sizes
func NewBufferPool() *BufferPool {
	return &BufferPool{
		smallPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 1024)
			},
		},
		mediumPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 4096)
			},
		},
		largePool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 16384)
			},
		},
	}
}

// SetBufferEventCallback sets a callback for buffer pool events
func (bp *BufferPool) SetBufferEventCallback(callback func(eventType string, size int)) {
	bp.onBufferEvent = callback
}

// GetBuffer returns a buffer of appropriate size from the pool
func (bp *BufferPool) GetBuffer(size int) []byte {
	var pool *sync.Pool

	switch {
	case size <= 1024:
		pool = &bp.smallPool
	case size <= 4096:
		pool = &bp.mediumPool
	default:
		pool = &bp.largePool
	}

	buf := pool.Get().([]byte)
	// Ensure buffer is large enough, resize if necessary
	if cap(buf) < size {
		buf = make([]byte, size)
		// Emit buffer miss event
		if bp.onBufferEvent != nil {
			bp.onBufferEvent("miss", size)
		}
	} else {
		// Emit buffer hit event
		if bp.onBufferEvent != nil {
			bp.onBufferEvent("hit", size)
		}
	}
	return buf[:size]
}

// PutBuffer returns a buffer to the appropriate pool
func (bp *BufferPool) PutBuffer(buf []byte) {
	if buf == nil {
		return
	}

	capacity := cap(buf)
	// Clear the buffer to prevent data leakage between uses
	for i := range buf {
		buf[i] = 0
	}

	switch {
	case capacity <= 1024:
		bp.smallPool.Put(buf[:1024])
	case capacity <= 4096:
		bp.mediumPool.Put(buf[:4096])
	case capacity <= 16384:
		bp.largePool.Put(buf[:16384])
		// Don't pool very large buffers to avoid memory bloat
	}
}

// ConnectionManager tracks and manages connection health
type ConnectionManager struct {
	connections   map[string]*ConnectionInfo
	lock          sync.RWMutex
	timeout       time.Duration
	cleanupTicker *time.Ticker
	stopChan      chan struct{}
}

// ConnectionInfo tracks connection metadata
type ConnectionInfo struct {
	LastUsed     time.Time
	LastPing     time.Time
	IsHealthy    bool
	MessageCount int64
}

// NewConnectionManager creates a new connection manager
func NewConnectionManager(timeout time.Duration) *ConnectionManager {
	cm := &ConnectionManager{
		connections: make(map[string]*ConnectionInfo),
		timeout:     timeout,
		stopChan:    make(chan struct{}),
	}

	// Start cleanup routine
	cm.cleanupTicker = time.NewTicker(30 * time.Second)
	go cm.cleanupRoutine()

	return cm
}

// RegisterConnection adds a new connection to tracking
func (cm *ConnectionManager) RegisterConnection(nodeID string) {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	cm.connections[nodeID] = &ConnectionInfo{
		LastUsed:     time.Now(),
		LastPing:     time.Now(),
		IsHealthy:    true,
		MessageCount: 0,
	}
}

// UpdateConnectionActivity marks a connection as recently used
func (cm *ConnectionManager) UpdateConnectionActivity(nodeID string) {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	if conn, exists := cm.connections[nodeID]; exists {
		conn.LastUsed = time.Now()
		conn.MessageCount++
	}
}

// IsConnectionHealthy checks if a connection is still healthy
func (cm *ConnectionManager) IsConnectionHealthy(nodeID string) bool {
	cm.lock.RLock()
	defer cm.lock.RUnlock()

	conn, exists := cm.connections[nodeID]
	if !exists {
		return false
	}

	return conn.IsHealthy && time.Since(conn.LastUsed) < cm.timeout
}

// MarkConnectionUnhealthy marks a connection as unhealthy
func (cm *ConnectionManager) MarkConnectionUnhealthy(nodeID string) {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	if conn, exists := cm.connections[nodeID]; exists {
		conn.IsHealthy = false
	}
}

// RemoveConnection removes a connection from tracking
func (cm *ConnectionManager) RemoveConnection(nodeID string) {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	delete(cm.connections, nodeID)
}

// GetConnectionCount returns the number of tracked connections
func (cm *ConnectionManager) GetConnectionCount() int {
	cm.lock.RLock()
	defer cm.lock.RUnlock()

	return len(cm.connections)
}

// GetConnectionStats returns statistics about connections
func (cm *ConnectionManager) GetConnectionStats() map[string]interface{} {
	cm.lock.RLock()
	defer cm.lock.RUnlock()

	stats := map[string]interface{}{
		"total_connections":     len(cm.connections),
		"healthy_connections":   0,
		"unhealthy_connections": 0,
		"total_messages":        int64(0),
	}

	for _, conn := range cm.connections {
		if conn.IsHealthy {
			stats["healthy_connections"] = stats["healthy_connections"].(int) + 1
		} else {
			stats["unhealthy_connections"] = stats["unhealthy_connections"].(int) + 1
		}
		stats["total_messages"] = stats["total_messages"].(int64) + conn.MessageCount
	}

	return stats
}

// cleanupRoutine periodically removes stale connections
func (cm *ConnectionManager) cleanupRoutine() {
	for {
		select {
		case <-cm.cleanupTicker.C:
			cm.cleanupStaleConnections()
		case <-cm.stopChan:
			cm.cleanupTicker.Stop()
			return
		}
	}
}

// cleanupStaleConnections removes connections that haven't been used recently
func (cm *ConnectionManager) cleanupStaleConnections() {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	now := time.Now()
	for nodeID, conn := range cm.connections {
		if now.Sub(conn.LastUsed) > cm.timeout*2 {
			delete(cm.connections, nodeID)
		}
	}
}

// Stop stops the connection manager
func (cm *ConnectionManager) Stop() {
	close(cm.stopChan)
}

// WorkerPool manages a limited number of goroutines for connection handling
type WorkerPool struct {
	workers  int
	jobQueue chan func()
	stopChan chan struct{}
	wg       sync.WaitGroup
}

// NewWorkerPool creates a new worker pool
func NewWorkerPool(workers int) *WorkerPool {
	wp := &WorkerPool{
		workers:  workers,
		jobQueue: make(chan func(), workers*2), // Buffer for 2x workers
		stopChan: make(chan struct{}),
	}

	// Start workers
	for i := 0; i < workers; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}

	return wp
}

// Submit submits a job to the worker pool
func (wp *WorkerPool) Submit(job func()) {
	select {
	case wp.jobQueue <- job:
		// Job submitted successfully
	case <-wp.stopChan:
		// Pool is stopping, ignore job
	default:
		// Queue is full, execute synchronously to avoid blocking
		go job()
	}
}

// worker runs a single worker goroutine
func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()

	for {
		select {
		case job := <-wp.jobQueue:
			job()
		case <-wp.stopChan:
			return
		}
	}
}

// Stop stops the worker pool
func (wp *WorkerPool) Stop() {
	close(wp.stopChan)
	wp.wg.Wait()
}

// Metrics tracks performance and memory metrics
type Metrics struct {
	lock               sync.RWMutex
	connectionCount    int64
	totalMessages      int64
	totalBytes         int64
	bufferPoolHits     int64
	bufferPoolMisses   int64
	connectionTimeouts int64
	connectionErrors   int64
	startTime          time.Time
}

// NewMetrics creates a new metrics tracker
func NewMetrics() *Metrics {
	return &Metrics{
		startTime: time.Now(),
	}
}

// IncrementConnectionCount increments the connection count
func (m *Metrics) IncrementConnectionCount() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.connectionCount++
}

// DecrementConnectionCount decrements the connection count
func (m *Metrics) DecrementConnectionCount() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.connectionCount--
}

// RecordMessage records a message transmission
func (m *Metrics) RecordMessage(size int) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.totalMessages++
	m.totalBytes += int64(size)
}

// RecordBufferPoolHit records a buffer pool hit
func (m *Metrics) RecordBufferPoolHit() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.bufferPoolHits++
}

// RecordBufferPoolMiss records a buffer pool miss
func (m *Metrics) RecordBufferPoolMiss() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.bufferPoolMisses++
}

// RecordConnectionTimeout records a connection timeout
func (m *Metrics) RecordConnectionTimeout() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.connectionTimeouts++
}

// RecordConnectionError records a connection error
func (m *Metrics) RecordConnectionError() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.connectionErrors++
}

// GetStats returns current metrics
func (m *Metrics) GetStats() map[string]interface{} {
	m.lock.RLock()
	defer m.lock.RUnlock()

	uptime := time.Since(m.startTime)

	return map[string]interface{}{
		"uptime_seconds":      uptime.Seconds(),
		"connection_count":    m.connectionCount,
		"total_messages":      m.totalMessages,
		"total_bytes":         m.totalBytes,
		"buffer_pool_hits":    m.bufferPoolHits,
		"buffer_pool_misses":  m.bufferPoolMisses,
		"connection_timeouts": m.connectionTimeouts,
		"connection_errors":   m.connectionErrors,
		"messages_per_second": float64(m.totalMessages) / uptime.Seconds(),
		"bytes_per_second":    float64(m.totalBytes) / uptime.Seconds(),
	}
}
