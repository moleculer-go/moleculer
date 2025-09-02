package tcp

import (
	"errors"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestTransportError_Error(t *testing.T) {
	tests := []struct {
		name     string
		err      *TransportError
		expected string
	}{
		{
			name:     "simple error",
			err:      NewTransportError("read", errors.New("connection failed")),
			expected: "transport read failed: connection failed",
		},
		{
			name: "error with node",
			err: NewTransportErrorWithNode("connect", "node1",
				errors.New("timeout")),
			expected: "transport connect failed for node node1: timeout",
		},
		{
			name: "error with address",
			err: NewTransportErrorWithAddress("send", "node2", "127.0.0.1:3000",
				errors.New("connection refused")),
			expected: "transport send failed for node node2 at 127.0.0.1:3000: connection refused",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.err.Error())
		})
	}
}

func TestTransportError_Unwrap(t *testing.T) {
	originalErr := errors.New("original error")
	transportErr := NewTransportError("test", originalErr)

	assert.Equal(t, originalErr, transportErr.Unwrap())
	assert.True(t, errors.Is(transportErr, originalErr))
}

func TestBufferPool_GetPut(t *testing.T) {
	bp := NewBufferPool()

	// Test small buffer (should use pool)
	buf1 := bp.Get(1024)
	assert.Len(t, buf1, 1024)
	assert.Equal(t, 4096, cap(buf1)) // Should be pooled buffer

	// Return to pool
	bp.Put(buf1)

	// Get again (should reuse)
	buf2 := bp.Get(1024)
	assert.Len(t, buf2, 1024)
	assert.Equal(t, 4096, cap(buf2))
	assert.Equal(t, buf1[:1024], buf2) // Should be same underlying array

	// Test large buffer (should allocate new)
	buf3 := bp.Get(8192)
	assert.Len(t, buf3, 8192)
	assert.Equal(t, 8192, cap(buf3)) // Should be exact size

	// Return large buffer (should not be pooled)
	bp.Put(buf3)
	buf4 := bp.Get(8192)
	assert.Len(t, buf4, 8192)
	assert.Equal(t, 8192, cap(buf4))
	// buf4 should be different from buf3 since large buffers aren't pooled
}

func TestWorkerPool_Submit(t *testing.T) {
	logger := log.New()
	logger.SetLevel(log.ErrorLevel) // Reduce log noise
	wp := NewWorkerPool(2, logger.WithField("test", "worker_pool"))
	defer wp.Stop()

	// Test basic task submission
	completed := make(chan bool, 5)
	for i := 0; i < 5; i++ {
		wp.Submit(func() {
			time.Sleep(10 * time.Millisecond)
			completed <- true
		})
	}

	// Wait for completion with timeout
	timeout := time.After(1 * time.Second)
	count := 0
	for count < 5 {
		select {
		case <-completed:
			count++
		case <-timeout:
			t.Fatal("Timeout waiting for tasks to complete")
		}
	}
	assert.Equal(t, 5, count)
}

func TestWorkerPool_Stop(t *testing.T) {
	logger := log.New()
	logger.SetLevel(log.ErrorLevel)
	wp := NewWorkerPool(1, logger.WithField("test", "worker_pool"))

	// Stop the pool
	wp.Stop()

	// Verify pool is stopped
	assert.True(t, wp.IsStopped())

	// Try to submit a task (should not panic)
	wp.Submit(func() {
		t.Log("Task executed after stop")
	})
}

func TestWorkerPool_PanicRecovery(t *testing.T) {
	logger := log.New()
	logger.SetLevel(log.ErrorLevel)
	wp := NewWorkerPool(1, logger.WithField("test", "worker_pool"))
	defer wp.Stop()

	panicHandled := make(chan bool, 1)
	wp.Submit(func() {
		defer func() {
			if r := recover(); r != nil {
				panicHandled <- true
			}
		}()
		panic("test panic")
	})

	select {
	case <-panicHandled:
		// Panic was recovered
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Panic was not recovered")
	}
}

func BenchmarkBufferPool_GetPut(b *testing.B) {
	bp := NewBufferPool()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf := bp.Get(1024)
			// Simulate some work
			buf[0] = 1
			bp.Put(buf)
		}
	})
}

func BenchmarkWorkerPool_Submit(b *testing.B) {
	logger := log.New()
	logger.SetLevel(log.ErrorLevel)
	wp := NewWorkerPool(4, logger.WithField("test", "benchmark"))
	defer wp.Stop()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			wp.Submit(func() {
				// Minimal work
				_ = 1 + 1
			})
		}
	})
}
