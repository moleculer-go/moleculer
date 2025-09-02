package tcp

import (
	"context"
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"
)

// TransportError provides structured error handling for transport operations
type TransportError struct {
	Op      string
	NodeID  string
	Addr    string
	Err     error
	Context map[string]interface{}
}

func (e *TransportError) Error() string {
	if e.Addr != "" {
		return fmt.Sprintf("transport %s failed for node %s at %s: %v", e.Op, e.NodeID, e.Addr, e.Err)
	}
	if e.NodeID != "" {
		return fmt.Sprintf("transport %s failed for node %s: %v", e.Op, e.NodeID, e.Err)
	}
	return fmt.Sprintf("transport %s failed: %v", e.Op, e.Err)
}

func (e *TransportError) Unwrap() error {
	return e.Err
}

// NewTransportError creates a new transport error
func NewTransportError(op string, err error) *TransportError {
	return &TransportError{
		Op:  op,
		Err: err,
	}
}

// NewTransportErrorWithNode creates a new transport error with node information
func NewTransportErrorWithNode(op, nodeID string, err error) *TransportError {
	return &TransportError{
		Op:     op,
		NodeID: nodeID,
		Err:    err,
	}
}

// NewTransportErrorWithAddress creates a new transport error with address information
func NewTransportErrorWithAddress(op, nodeID, addr string, err error) *TransportError {
	return &TransportError{
		Op:     op,
		NodeID: nodeID,
		Addr:   addr,
		Err:    err,
	}
}

// BufferPool provides memory-efficient buffer management using sync.Pool
type BufferPool struct {
	pool sync.Pool
}

// NewBufferPool creates a new buffer pool with 4KB default buffer size
func NewBufferPool() *BufferPool {
	return &BufferPool{
		pool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 4096) // 4KB default buffer
			},
		},
	}
}

// Get returns a buffer of at least the specified size
// For sizes <= 4096, returns a pooled buffer
// For larger sizes, allocates a new buffer
func (bp *BufferPool) Get(size int) []byte {
	if size <= 4096 {
		return bp.pool.Get().([]byte)[:size]
	}
	return make([]byte, size)
}

// Put returns a buffer to the pool if it's the right size
func (bp *BufferPool) Put(buf []byte) {
	if cap(buf) == 4096 {
		bp.pool.Put(buf[:4096])
	}
	// Large buffers are not pooled to prevent memory bloat
}

// WorkerPool provides managed goroutine pool with proper lifecycle management
type WorkerPool struct {
	numWorkers int
	taskChan   chan func()
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	logger     *log.Entry
}

// NewWorkerPool creates a new worker pool with the specified number of workers
func NewWorkerPool(numWorkers int, logger *log.Entry) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())
	wp := &WorkerPool{
		numWorkers: numWorkers,
		taskChan:   make(chan func(), 100), // buffered channel to prevent blocking
		ctx:        ctx,
		cancel:     cancel,
		logger:     logger,
	}
	wp.start()
	return wp
}

// start initializes the worker goroutines
func (wp *WorkerPool) start() {
	for i := 0; i < wp.numWorkers; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}
}

// worker is the main worker goroutine
func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()

	for {
		select {
		case task := <-wp.taskChan:
			// Execute task with panic recovery
			func() {
				defer func() {
					if r := recover(); r != nil {
						wp.logger.Errorf("Worker %d panic recovered: %v", id, r)
					}
				}()
				task()
			}()
		case <-wp.ctx.Done():
			return
		}
	}
}

// Submit submits a task to the worker pool
func (wp *WorkerPool) Submit(task func()) {
	select {
	case wp.taskChan <- task:
		// Task submitted successfully
	case <-wp.ctx.Done():
		// Pool is shutting down, don't submit new tasks
		wp.logger.Debug("Worker pool is shutting down, task not submitted")
	default:
		// Channel is full, execute synchronously as fallback
		wp.logger.Warn("Worker pool queue full, executing task synchronously")
		task()
	}
}

// Stop gracefully stops the worker pool
func (wp *WorkerPool) Stop() {
	wp.cancel()
	wp.wg.Wait()
}

// IsStopped returns true if the worker pool is stopped
func (wp *WorkerPool) IsStopped() bool {
	select {
	case <-wp.ctx.Done():
		return true
	default:
		return false
	}
}
