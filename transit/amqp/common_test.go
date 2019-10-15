package amqp

import (
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
	"github.com/moleculer-go/moleculer/strategy"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"os"
	"strconv"
	"sync"
	"time"
)

func amqpTestHost() string {
	env := os.Getenv("AMQP_HOST")
	if env == "" {
		return "localhost"
	}
	return env
}

func purge(queues []string, exchanges []string, destroy bool) {
	url := "amqp://" + amqpTestHost() + ":5672"

	connection, err := amqp.Dial(url)
	if err != nil {
		panic(err)
	}

	channel, err := connection.Channel()
	if err != nil {
		panic(err)
	}

	if destroy {
		for _, queue := range queues {
			channel.QueueDelete(queue, false, false, false)
		}

		for _, exchange := range exchanges {
			channel.ExchangeDelete(exchange, false, false)
		}
	} else {
		for _, queue := range queues {
			channel.QueuePurge(queue, false)
		}
	}

	channel.Close()
	connection.Close()
}

func merge(a, b map[string]interface{}) map[string]interface{} {
	for key, value := range b {
		a[key] = value
	}

	return a
}

func filter(logs *[]map[string]interface{}, t string) []map[string]interface{} {
	var result []map[string]interface{}
	for _, row := range *logs {
		if row["type"].(string) == t {
			result = append(result, row)
		}
	}

	return result
}

func createNode(namespace, name string, service *moleculer.ServiceSchema) *broker.ServiceBroker {
	logger := log.New()
	logger.SetLevel(log.FatalLevel)

	amqpConfig := AmqpOptions{
		Url: []string{"amqp://" + amqpTestHost() + ":5672"},
		Logger: logger.WithFields(log.Fields{
			"Unit Test": true,
			"transport": "amqp",
		}),
	}

	b := broker.New(&moleculer.Config{
		LogLevel: "Fatal",
		// TODO: ignore, not implemented in broker
		//Namespace: "test-" + namespace,
		DiscoverNodeID: func() string {
			return namespace + "-" + name
		},
		WaitForDependenciesTimeout: 10 * time.Second,
		TransporterFactory: func() interface{} {
			return CreateAmqpTransporter(amqpConfig)
		},
		StrategyFactory: func() interface{} {
			return strategy.NewRoundRobinStrategy()
		},
	})

	if service != nil {
		b.Publish(*service)
	}

	return b
}

func createActionWorker(number int, logs *[]map[string]interface{}) *broker.ServiceBroker {
	push := func(t string, params map[string]interface{}) {
		*logs = append(*logs, merge(map[string]interface{}{
			"type":      t,
			"worker":    number,
			"timestamp": time.Now(),
		}, params))
	}

	service := moleculer.ServiceSchema{
		Name: "test",
		Actions: []moleculer.Action{
			{
				Name: "hello",
				Handler: func(context moleculer.Context, params moleculer.Payload) interface{} {
					delay := params.Get("delay").Int()

					push("receive", params.RawMap())

					time.Sleep(time.Millisecond * time.Duration(delay))

					push("respond", params.RawMap())

					return merge(map[string]interface{}{
						"type":      "respond",
						"worker":    number,
						"timestamp": time.Now(),
					}, params.RawMap())
				},
			},
		},
	}

	return createNode("test-rpc", "worker"+strconv.Itoa(number), &service)
}

func createEmitWorker(name, serviceName string, logs *[]string) *broker.ServiceBroker {
	var b *broker.ServiceBroker

	service := moleculer.ServiceSchema{
		Name: serviceName,
		Events: []moleculer.Event{
			{
				Name: "hello.world2",
				Handler: func(context moleculer.Context, params moleculer.Payload) {
					if params.Get("testing").Bool() == true {
						*logs = append(*logs, name)
					}
				},
			},
		},
	}

	b = createNode("test-emit", "event-"+name, &service)

	return b
}

func createBroadcastWorker(name string, logs *[]string) *broker.ServiceBroker {
	var b *broker.ServiceBroker

	service := moleculer.ServiceSchema{
		Name: name,
		Events: []moleculer.Event{
			{
				Name: "hello.world",
				Handler: func(context moleculer.Context, params moleculer.Payload) {
					if params.Get("testing").Bool() == true {
						*logs = append(*logs, name)
					}
				},
			},
		},
	}

	b = createNode("test-broadcast", "event-"+name, &service)

	return b
}

func waitServicesNum(bkr *broker.ServiceBroker, num int) *sync.WaitGroup {
	wg := sync.WaitGroup{}
	wg.Add(num)

	serviceAddHandler := func(values ...interface{}) {
		nodeInfo := values[0].(map[string]string)
		if nodeInfo["name"][0] != '$' {
			wg.Done()
		}
	}

	go func() {
		wg.Wait()

		bkr.LocalBus().RemoveListener("$registry.service.added", serviceAddHandler)
	}()

	bkr.LocalBus().On("$registry.service.added", serviceAddHandler)

	return &wg
}

func waitServices(bkr *broker.ServiceBroker, names []string) *sync.WaitGroup {
	wg := sync.WaitGroup{}
	wg.Add(len(names))

	serviceAddHandler := func(values ...interface{}) {
		nodeInfo := values[0].(map[string]string)
		for _, name := range names {
			if nodeInfo["name"] == name {
				wg.Done()
				break
			}
		}
	}

	go func() {
		wg.Wait()

		bkr.LocalBus().RemoveListener("$registry.service.added", serviceAddHandler)
	}()

	bkr.LocalBus().On("$registry.service.added", serviceAddHandler)

	return &wg
}
