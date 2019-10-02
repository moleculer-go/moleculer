package main

import (
	"fmt"
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
	"github.com/moleculer-go/moleculer/payload"
	"github.com/moleculer-go/moleculer/strategy"
	"github.com/moleculer-go/moleculer/transit/amqp"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"os"
	"sync"
	"time"
)

var mathService = moleculer.ServiceSchema{
	Name: "math",
	Actions: []moleculer.Action{
		{
			Name: "add",
			Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
				return params.Get("a").Int() + params.Get("b").Int()
			},
		},
	},
}

func main() {
	url := os.Getenv("AMQP_HOST")
	if url == "" {
		url = "guest:guest@localhost"
	}

	amqpConfig := amqp.AmqpOptions{
		Url:              []string{"amqp://" + url + ":5672"},
		AutoDeleteQueues: 20 * time.Second,
		ExchangeOptions: map[string]interface{}{
			"durable": true,
		},
		QueueOptions: map[string]interface{}{
			"durable": true,
		},
		Logger: log.WithField("transport", "amqp"),
	}

	config := moleculer.Config{
		LogLevel: "debug",
		TransporterFactory: func() interface{} {
			return amqp.CreateAmqpTransporter(amqpConfig)
		},
		StrategyFactory: func() interface{} {
			return strategy.NewRoundRobinStrategy()
		},
	}

	bkrServer := broker.New(&config)
	bkrServer.Publish(mathService)
	bkrServer.Start()

	wg := sync.WaitGroup{}
	wg.Add(1)

	bkrClient := broker.New(&config)
	bkrClient.LocalBus().Once("$registry.service.added", func(value ...interface{}) {
		if value[0].(map[string]string)["name"] != "math" {
			return
		}

		a := int(rand.Int31n(100))
		b := int(rand.Int31n(100))
		p := payload.New(map[string]int{"a": a, "b": b})

		result := <-bkrServer.Call("math.add", p)

		if result.Error() != nil {
			fmt.Printf("%s", result.Error())
		} else {
			fmt.Printf("%d * %d = %d\n", a, b, result.Int()) //$ result: 140
		}

		wg.Done()
	})

	bkrClient.Start()

	wg.Wait()

	bkrClient.Stop()
	bkrServer.Stop()
}
