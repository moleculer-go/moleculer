package main

import (
	"fmt"
	"log"
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/transit/redis"
)

func main() {
	// Advanced Redis configuration
	redisConfig := &redis.RedisConfig{
		Host:     "localhost",
		Port:     6379,
		Password: "your-redis-password", // Optional password
		DB:       1,                     // Use database 1
		Prefix:   "myapp",               // Custom prefix for channels
	}

	// Create transporter
	transporter := redis.NewRedisTransporter(redisConfig)

	// Create broker with custom configuration
	broker := moleculer.NewServiceBroker(moleculer.Config{
		LogLevel:    "debug",
		Transit:     transporter,
		ServiceName: "redis-example",
		NodeID:      "redis-node-1",
	})

	// Add a service that demonstrates Redis pub/sub
	broker.AddService(&moleculer.Service{
		Name: "notifier",
		Actions: map[string]moleculer.Action{
			"send": func(ctx moleculer.BrokerContext) moleculer.Payload {
				message := ctx.Params().Get("message").String()
				channel := ctx.Params().Get("channel").String()

				// Emit event that will be published to Redis
				ctx.Emit("notification.sent", map[string]interface{}{
					"message": message,
					"channel": channel,
					"time":    time.Now().Unix(),
				})

				return ctx.Payload().Add("status", "sent").Add("channel", channel)
			},
		},
		Events: map[string]moleculer.Event{
			"notification.sent": func(ctx moleculer.BrokerContext) {
				data := ctx.Payload().RawMap()
				fmt.Printf("📢 Notification sent: %s to channel %s\n",
					data["message"], data["channel"])
			},
		},
	})

	// Add a service that listens for events
	broker.AddService(&moleculer.Service{
		Name: "listener",
		Events: map[string]moleculer.Event{
			"notification.sent": func(ctx moleculer.BrokerContext) {
				data := ctx.Payload().RawMap()
				fmt.Printf("👂 Listener received: %s\n", data["message"])
			},
		},
	})

	// Start broker
	if err := broker.Start(); err != nil {
		log.Fatal("Failed to start broker:", err)
	}

	fmt.Println("🚀 Advanced Redis example started")
	fmt.Printf("📡 Redis Config: %s:%d (DB: %d, Prefix: %s)\n",
		redisConfig.Host, redisConfig.Port, redisConfig.DB, redisConfig.Prefix)

	// Wait for services to register
	time.Sleep(2 * time.Second)

	// Send notifications
	fmt.Println("\n📤 Sending notifications...")

	broker.Call("notifier.send", map[string]interface{}{
		"message": "Hello from Redis!",
		"channel": "general",
	})

	broker.Call("notifier.send", map[string]interface{}{
		"message": "Redis is working great!",
		"channel": "status",
	})

	// Show Redis metrics
	metrics := transporter.GetMetrics()
	fmt.Printf("\n📊 Redis Connection Pool Stats:\n")
	for key, value := range metrics {
		fmt.Printf("  %s: %v\n", key, value)
	}

	// Keep running
	time.Sleep(3 * time.Second)
	broker.Stop()
	fmt.Println("🛑 Advanced Redis example stopped")
}
