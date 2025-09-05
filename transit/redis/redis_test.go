package redis

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

func TestRedisTransporter(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "Redis Transporter Suite")
}

var _ = ginkgo.Describe("Redis Transporter", func() {
	var transporter *RedisTransporter
	var client *redis.Client
	var ctx context.Context

	ginkgo.BeforeEach(func() {
		ctx = context.Background()
		
		// Create Redis client for testing
		client = redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
			DB:   1, // Use different DB for testing
		})

		// Clear test database
		client.FlushDB(ctx)

		// Create transporter with test config
		config := &RedisConfig{
			Host:   "localhost",
			Port:   6379,
			DB:     1,
			Prefix: "test-moleculer",
		}
		transporter = NewRedisTransporter(config)
	})

	ginkgo.AfterEach(func() {
		if transporter != nil {
			transporter.Disconnect()
		}
		if client != nil {
			client.Close()
		}
	})

	ginkgo.Describe("Connection", func() {
		ginkgo.It("should connect to Redis", func() {
			// Mock registry for testing - use nil for now
			errChan := transporter.Connect(nil)
			
			select {
			case err := <-errChan:
				gomega.Expect(err).To(gomega.BeNil())
			case <-time.After(5 * time.Second):
				ginkgo.Fail("Connection timeout")
			}
			
			gomega.Expect(transporter.IsConnected()).To(gomega.BeTrue())
		})

		ginkgo.It("should disconnect from Redis", func() {
			// Mock registry for testing - use nil for now
			errChan := transporter.Connect(nil)
			
			select {
			case err := <-errChan:
				gomega.Expect(err).To(gomega.BeNil())
			case <-time.After(5 * time.Second):
				ginkgo.Fail("Connection timeout")
			}

			errChan = transporter.Disconnect()
			select {
			case err := <-errChan:
				gomega.Expect(err).To(gomega.BeNil())
			case <-time.After(5 * time.Second):
				ginkgo.Fail("Disconnection timeout")
			}
			
			gomega.Expect(transporter.IsConnected()).To(gomega.BeFalse())
		})
	})

	ginkgo.Describe("Configuration", func() {
		ginkgo.It("should return correct name", func() {
			gomega.Expect(transporter.GetName()).To(gomega.Equal("Redis"))
		})

		ginkgo.It("should return configuration", func() {
			config := transporter.GetConfig()
			gomega.Expect(config).ToNot(gomega.BeNil())
		})

		ginkgo.It("should update configuration", func() {
			newConfig := &RedisConfig{
				Host: "newhost",
				Port: 6380,
				DB:   2,
			}
			transporter.SetConfig(newConfig)
			gomega.Expect(transporter.config).To(gomega.Equal(newConfig))
		})
	})

	ginkgo.Describe("Metrics", func() {
		ginkgo.BeforeEach(func() {
			// Mock registry for testing - use nil for now
			errChan := transporter.Connect(nil)
			
			select {
			case err := <-errChan:
				gomega.Expect(err).To(gomega.BeNil())
			case <-time.After(5 * time.Second):
				ginkgo.Fail("Connection timeout")
			}
		})

		ginkgo.It("should return metrics", func() {
			metrics := transporter.GetMetrics()
			gomega.Expect(metrics).ToNot(gomega.BeNil())
			gomega.Expect(metrics).To(gomega.HaveKey("hits"))
			gomega.Expect(metrics).To(gomega.HaveKey("misses"))
			gomega.Expect(metrics).To(gomega.HaveKey("total_conns"))
		})
	})
})