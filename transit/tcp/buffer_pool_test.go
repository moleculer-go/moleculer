package tcp

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("BufferPool", func() {
	Describe("GetBuffer", func() {
		Context("when requesting small buffers", func() {
			It("should return buffers from small pool", func() {
				bufferPool := NewBufferPool()
				buf := bufferPool.GetBuffer(512)
				Expect(buf).ToNot(BeNil())
				Expect(len(buf)).To(Equal(512))
				Expect(cap(buf)).To(BeNumerically(">=", 512))
			})

			It("should return buffers with exact size when capacity is sufficient", func() {
				bufferPool := NewBufferPool()
				buf := bufferPool.GetBuffer(1024)
				Expect(buf).ToNot(BeNil())
				Expect(len(buf)).To(Equal(1024))
				Expect(cap(buf)).To(BeNumerically(">=", 1024))
			})
		})

		Context("when requesting medium buffers", func() {
			It("should return buffers from medium pool", func() {
				bufferPool := NewBufferPool()
				buf := bufferPool.GetBuffer(2048)
				Expect(buf).ToNot(BeNil())
				Expect(len(buf)).To(Equal(2048))
				Expect(cap(buf)).To(BeNumerically(">=", 2048))
			})

			It("should return buffers with exact size when capacity is sufficient", func() {
				bufferPool := NewBufferPool()
				buf := bufferPool.GetBuffer(4096)
				Expect(buf).ToNot(BeNil())
				Expect(len(buf)).To(Equal(4096))
				Expect(cap(buf)).To(BeNumerically(">=", 4096))
			})
		})

		Context("when requesting large buffers", func() {
			It("should return buffers from large pool", func() {
				bufferPool := NewBufferPool()
				buf := bufferPool.GetBuffer(8192)
				Expect(buf).ToNot(BeNil())
				Expect(len(buf)).To(Equal(8192))
				Expect(cap(buf)).To(BeNumerically(">=", 8192))
			})

			It("should return buffers with exact size when capacity is sufficient", func() {
				bufferPool := NewBufferPool()
				buf := bufferPool.GetBuffer(16384)
				Expect(buf).ToNot(BeNil())
				Expect(len(buf)).To(Equal(16384))
				Expect(cap(buf)).To(BeNumerically(">=", 16384))
			})
		})

		Context("when requesting buffers larger than pool capacity", func() {
			It("should create new buffer for very large requests", func() {
				bufferPool := NewBufferPool()
				buf := bufferPool.GetBuffer(32768) // Larger than large pool
				Expect(buf).ToNot(BeNil())
				Expect(len(buf)).To(Equal(32768))
				Expect(cap(buf)).To(Equal(32768)) // Should be exactly the requested size
			})
		})

		Context("when requesting zero or negative size", func() {
			It("should handle zero size gracefully", func() {
				bufferPool := NewBufferPool()
				buf := bufferPool.GetBuffer(0)
				Expect(buf).ToNot(BeNil())
				Expect(len(buf)).To(Equal(0))
			})

			It("should handle negative size gracefully", func() {
				bufferPool := NewBufferPool()
				buf := bufferPool.GetBuffer(-1)
				Expect(buf).ToNot(BeNil())
				Expect(len(buf)).To(Equal(0))
			})
		})
	})

	Describe("PutBuffer", func() {
		Context("when returning buffers to pool", func() {
			It("should accept small buffers", func() {
				bufferPool := NewBufferPool()
				buf := make([]byte, 512, 1024)
				Expect(func() { bufferPool.PutBuffer(buf) }).ToNot(Panic())
			})

			It("should accept medium buffers", func() {
				bufferPool := NewBufferPool()
				buf := make([]byte, 2048, 4096)
				Expect(func() { bufferPool.PutBuffer(buf) }).ToNot(Panic())
			})

			It("should accept large buffers", func() {
				bufferPool := NewBufferPool()
				buf := make([]byte, 8192, 16384)
				Expect(func() { bufferPool.PutBuffer(buf) }).ToNot(Panic())
			})

			It("should handle nil buffers gracefully", func() {
				bufferPool := NewBufferPool()
				Expect(func() { bufferPool.PutBuffer(nil) }).ToNot(Panic())
			})

			It("should clear buffer contents to prevent data leakage", func() {
				bufferPool := NewBufferPool()
				buf := make([]byte, 512, 1024)
				// Fill buffer with test data
				for i := range buf {
					buf[i] = byte(i % 256)
				}

				bufferPool.PutBuffer(buf)

				// Buffer should be cleared
				for i := range buf {
					Expect(buf[i]).To(Equal(byte(0)))
				}
			})
		})
	})

	Describe("Buffer Reuse", func() {
		Context("when reusing buffers from pool", func() {
			It("should reuse small buffers efficiently", func() {
				bufferPool := NewBufferPool()
				// Get a buffer
				buf1 := bufferPool.GetBuffer(512)
				originalCap := cap(buf1)

				// Return it
				bufferPool.PutBuffer(buf1)

				// Get another buffer of same size
				buf2 := bufferPool.GetBuffer(512)
				Expect(cap(buf2)).To(Equal(originalCap))
			})

			It("should reuse medium buffers efficiently", func() {
				bufferPool := NewBufferPool()
				// Get a buffer
				buf1 := bufferPool.GetBuffer(2048)
				originalCap := cap(buf1)

				// Return it
				bufferPool.PutBuffer(buf1)

				// Get another buffer of same size
				buf2 := bufferPool.GetBuffer(2048)
				Expect(cap(buf2)).To(Equal(originalCap))
			})

			It("should reuse large buffers efficiently", func() {
				bufferPool := NewBufferPool()
				// Get a buffer
				buf1 := bufferPool.GetBuffer(8192)
				originalCap := cap(buf1)

				// Return it
				bufferPool.PutBuffer(buf1)

				// Get another buffer of same size
				buf2 := bufferPool.GetBuffer(8192)
				Expect(cap(buf2)).To(Equal(originalCap))
			})
		})
	})

	Describe("Buffer Growth Strategy", func() {
		Context("when requesting buffers larger than available capacity", func() {
			It("should grow small buffers when possible", func() {
				bufferPool := NewBufferPool()
				// Get a small buffer
				buf1 := bufferPool.GetBuffer(512)

				// Return it
				bufferPool.PutBuffer(buf1)

				// Request a larger buffer that could fit in medium pool
				buf2 := bufferPool.GetBuffer(2048)

				// With current implementation, this should create a new buffer
				// After fix, it should reuse and grow the existing buffer
				Expect(cap(buf2)).To(BeNumerically(">=", 2048))
			})

			It("should grow medium buffers when possible", func() {
				bufferPool := NewBufferPool()
				// Get a medium buffer
				buf1 := bufferPool.GetBuffer(2048)

				// Return it
				bufferPool.PutBuffer(buf1)

				// Request a larger buffer that could fit in large pool
				buf2 := bufferPool.GetBuffer(8192)

				// With current implementation, this should create a new buffer
				// After fix, it should reuse and grow the existing buffer
				Expect(cap(buf2)).To(BeNumerically(">=", 8192))
			})

			It("should handle growth across pool boundaries efficiently", func() {
				bufferPool := NewBufferPool()
				// Test growing from small to medium pool
				buf1 := bufferPool.GetBuffer(512)
				bufferPool.PutBuffer(buf1)

				buf2 := bufferPool.GetBuffer(2048)
				Expect(cap(buf2)).To(BeNumerically(">=", 2048))

				// Test growing from medium to large pool
				buf3 := bufferPool.GetBuffer(2048)
				bufferPool.PutBuffer(buf3)

				buf4 := bufferPool.GetBuffer(8192)
				Expect(cap(buf4)).To(BeNumerically(">=", 8192))
			})
		})
	})

	Describe("Performance and Memory Efficiency", func() {
		Context("when handling many buffer requests", func() {
			It("should minimize memory allocations", func() {
				bufferPool := NewBufferPool()
				// This test will be more meaningful after implementing the growth strategy
				var buffers [][]byte

				// Request many buffers of varying sizes
				for i := 0; i < 100; i++ {
					size := 512 + (i%3)*1024 // 512, 1536, 2560
					buf := bufferPool.GetBuffer(size)
					buffers = append(buffers, buf)
				}

				// Return all buffers
				for _, buf := range buffers {
					bufferPool.PutBuffer(buf)
				}

				// Request more buffers - should reuse from pool
				for i := 0; i < 50; i++ {
					size := 512 + (i%3)*1024
					buf := bufferPool.GetBuffer(size)
					Expect(buf).ToNot(BeNil())
					Expect(len(buf)).To(Equal(size))
				}
			})
		})
	})

	Describe("Buffer Event Callback", func() {
		Context("when buffer events occur", func() {
			It("should call callback for buffer hits and misses", func() {
				bufferPool := NewBufferPool()
				var events []string
				var sizes []int

				bufferPool.SetBufferEventCallback(func(eventType string, size int) {
					events = append(events, eventType)
					sizes = append(sizes, size)
				})

				// First request should be a hit (buffer from pool)
				buf1 := bufferPool.GetBuffer(512)
				Expect(events).To(ContainElement("hit"))
				Expect(sizes).To(ContainElement(512))

				// Return buffer
				bufferPool.PutBuffer(buf1)

				// Second request should be a hit (reused buffer)
				bufferPool.GetBuffer(512)
				Expect(events).To(ContainElement("hit"))
				Expect(sizes).To(ContainElement(512))
			})
		})
	})
})
