package main

import (
	"fmt"
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
	"github.com/moleculer-go/moleculer/payload"
	"github.com/moleculer-go/moleculer/transit/amqp"
	"github.com/sirupsen/logrus"
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
	amqpConfig := amqp.AmqpOptions{
		Url:              []string{"amqp://guest:guest@localhost:5672"},
		AutoDeleteQueues: 20 * time.Second,
		Logger:           logrus.WithField("transport", "amqp"),
	}

	config := moleculer.Config{
		LogLevel: "debug",
		TransporterFactory: func() interface{} {
			return amqp.CreateAmqpTransporter(amqpConfig)
		},
	}

	var bkr = broker.New(&config)
	bkr.Publish(mathService)
	bkr.Start()
	result := <-bkr.Call("math.add", payload.New(map[string]int{
		"a": 10,
		"b": 130,
	}))
	fmt.Println("result: ", result.Int()) //$ result: 140

	wg := sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
}
