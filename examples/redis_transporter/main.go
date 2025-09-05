package main

import (
	"fmt"
	"log"
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/transit/redis"
)

func main() {
	// Create Redis transporter configuration
	redisConfig := &redis.RedisConfig{
		Host:     "localhost",
		Port:     6379,
		Password: "",          // No password
		DB:       0,           // Default database
		Prefix:   "moleculer", // Channel prefix
	}

	// Create Redis transporter
	transporter := redis.NewRedisTransporter(redisConfig)

	// Create service broker with Redis transporter
	broker := moleculer.NewServiceBroker(moleculer.Config{
		LogLevel: "info",
		Transit:  transporter,
	})

	// Add a simple service
	broker.AddService(&moleculer.Service{
		Name: "math",
		Actions: map[string]moleculer.Action{
			"add": func(ctx moleculer.BrokerContext) moleculer.Payload {
				a := ctx.Params().Get("a").Int()
				b := ctx.Params().Get("b").Int()
				return ctx.Payload().Add("result", a+b)
			},
			"multiply": func(ctx moleculer.BrokerContext) moleculer.Payload {
				a := ctx.Params().Get("a").Int()
				b := ctx.Params().Get("b").Int()
				return ctx.Payload().Add("result", a*b)
			},
		},
		Events: map[string]moleculer.Event{
			"math.calculated": func(ctx moleculer.BrokerContext) {
				fmt.Printf("Math calculation completed: %s\n", ctx.Payload().String())
			},
		},
	})

	// Add another service that uses the math service
	broker.AddService(&moleculer.Service{
		Name: "calculator",
		Actions: map[string]moleculer.Action{
			"calculate": func(ctx moleculer.BrokerContext) moleculer.Payload {
				operation := ctx.Params().Get("operation").String()
				a := ctx.Params().Get("a").Int()
				b := ctx.Params().Get("b").Int()

				var result int
				switch operation {
				case "add":
					response := ctx.Call("math.add", map[string]interface{}{
						"a": a,
						"b": b,
					})
					result = response.Get("result").Int()
				case "multiply":
					response := ctx.Call("math.multiply", map[string]interface{}{
						"a": a,
						"b": b,
					})
					result = response.Get("result").Int()
				default:
					return ctx.Payload().Add("error", "Unknown operation")
				}

				// Emit event
				ctx.Emit("math.calculated", map[string]interface{}{
					"operation": operation,
					"a":         a,
					"b":         b,
					"result":    result,
				})

				return ctx.Payload().Add("result", result)
			},
		},
	})

	// Start the broker
	if err := broker.Start(); err != nil {
		log.Fatal("Failed to start broker:", err)
	}

	fmt.Println("🚀 Moleculer-Go broker started with Redis transporter")
	fmt.Println("📡 Redis connection:", transporter.IsConnected())

	// Wait a bit for services to register
	time.Sleep(2 * time.Second)

	// Test the services
	fmt.Println("\n🧮 Testing math operations...")

	// Test addition
	result := broker.Call("calculator.calculate", map[string]interface{}{
		"operation": "add",
		"a":         10,
		"b":         5,
	})
	fmt.Printf("10 + 5 = %d\n", result.Get("result").Int())

	// Test multiplication
	result = broker.Call("calculator.calculate", map[string]interface{}{
		"operation": "multiply",
		"a":         10,
		"b":         5,
	})
	fmt.Printf("10 * 5 = %d\n", result.Get("result").Int())

	// Test direct math service
	result = broker.Call("math.add", map[string]interface{}{
		"a": 20,
		"b": 30,
	})
	fmt.Printf("20 + 30 = %d\n", result.Get("result").Int())

	// Get Redis metrics
	metrics := transporter.GetMetrics()
	fmt.Printf("\n📊 Redis Metrics:\n")
	fmt.Printf("  Total Connections: %v\n", metrics["total_conns"])
	fmt.Printf("  Idle Connections: %v\n", metrics["idle_conns"])
	fmt.Printf("  Hits: %v\n", metrics["hits"])
	fmt.Printf("  Misses: %v\n", metrics["misses"])

	// Keep running for a bit to see events
	fmt.Println("\n⏳ Running for 5 seconds to demonstrate events...")
	time.Sleep(5 * time.Second)

	// Stop the broker
	broker.Stop()
	fmt.Println("🛑 Broker stopped")
}
