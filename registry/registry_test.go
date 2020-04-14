package registry_test

import (
	"os"
	"sync"
	"time"

	"github.com/moleculer-go/cupaloy/v2"
	bus "github.com/moleculer-go/goemitter"
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
	"github.com/moleculer-go/moleculer/transit/memory"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
)

var logLevel = "fatal"
var snap = cupaloy.New(cupaloy.FailOnUpdate(os.Getenv("UPDATE_SNAPSHOTS") == "true"))

func createPrinterBroker(mem *memory.SharedMemory) broker.ServiceBroker {
	broker := broker.New(&moleculer.Config{
		DiscoverNodeID: func() string { return "node_printerBroker" },
		LogLevel:       logLevel,
		TransporterFactory: func() interface{} {
			transport := memory.Create(log.WithField("transport", "memory"), mem)
			return &transport
		},
	})

	broker.Publish(moleculer.ServiceSchema{
		Name: "printer",
		Actions: []moleculer.Action{
			{
				Name: "print",
				Handler: func(context moleculer.Context, params moleculer.Payload) interface{} {
					context.Logger().Info("print action invoked. params: ", params)
					return params.Value()
				},
			},
		},
		Events: []moleculer.Event{
			{
				Name: "printed",
				Handler: func(context moleculer.Context, params moleculer.Payload) {
					context.Logger().Info("printer.printed --> ", params.Value())
				},
			},
		},
	})

	return (*broker)
}

func createScannerBroker(mem *memory.SharedMemory) broker.ServiceBroker {
	broker := broker.New(&moleculer.Config{
		DiscoverNodeID: func() string { return "node_scannerBroker" },
		LogLevel:       logLevel,
		TransporterFactory: func() interface{} {
			transport := memory.Create(log.WithField("transport", "memory"), mem)
			return &transport
		},
	})
	broker.Publish(moleculer.ServiceSchema{
		Name: "scanner",
		Actions: []moleculer.Action{
			{
				Name: "scan",
				Handler: func(context moleculer.Context, params moleculer.Payload) interface{} {
					context.Logger().Info("scan action invoked!")

					return params.Value()
				},
			},
		},
		Events: []moleculer.Event{
			{
				Name: "scanned",
				Handler: func(context moleculer.Context, params moleculer.Payload) {
					context.Logger().Info("scanner.scanned --> ", params.Value())
				},
			},
		},
	})

	return (*broker)
}

func createCpuBroker(mem *memory.SharedMemory) broker.ServiceBroker {
	broker := broker.New(&moleculer.Config{
		DiscoverNodeID: func() string { return "node_cpuBroker" },
		LogLevel:       logLevel,
		TransporterFactory: func() interface{} {
			transport := memory.Create(log.WithField("transport", "memory"), mem)
			return &transport
		},
	})
	broker.Publish(moleculer.ServiceSchema{
		Name: "cpu",
		Actions: []moleculer.Action{
			{
				Name: "compute",
				Handler: func(context moleculer.Context, params moleculer.Payload) interface{} {
					context.Logger().Debug("compute action invoked!")

					scanResult := <-context.Call("scanner.scan", params)

					context.Logger().Debug("scanResult: ", scanResult)

					printResult := <-context.Call("printer.print", scanResult)

					return printResult
				},
			},
		},
	})
	broker.Publish(moleculer.ServiceSchema{
		Name: "printer",
		Actions: []moleculer.Action{
			{
				Name: "print",
				Handler: func(context moleculer.Context, params moleculer.Payload) interface{} {
					return params.Value()
				},
			},
		},
		Events: []moleculer.Event{
			{
				Name: "printed",
				Handler: func(context moleculer.Context, params moleculer.Payload) {
					context.Logger().Info("printer.printed --> ", params.Value())
				},
			},
		},
	})
	return (*broker)
}

func hasNode(list []moleculer.Payload, nodeID string) bool {
	for _, p := range list {
		if p.Get("nodeID").String() == nodeID {
			return true
		}
	}
	return false
}

var _ = Describe("Registry", func() {

	Describe("Auto discovery", func() {

		It("3 brokers should auto discovery and perform local and remote Calls", func(done Done) {

			mem := &memory.SharedMemory{}

			printerBroker := createPrinterBroker(mem)

			var serviceAdded, serviceRemoved []moleculer.Payload
			events := bus.Construct()
			addedMutex := &sync.Mutex{}
			printerBroker.Publish(moleculer.ServiceSchema{
				Name: "internal-consumer",
				Events: []moleculer.Event{
					moleculer.Event{
						Name: "$registry.service.added",
						Handler: func(ctx moleculer.Context, params moleculer.Payload) {
							addedMutex.Lock()
							defer addedMutex.Unlock()
							serviceAdded = append(serviceAdded, params)
							go events.EmitSync("$registry.service.added", serviceAdded)
						},
					},
					moleculer.Event{
						Name: "$registry.service.removed",
						Handler: func(ctx moleculer.Context, params moleculer.Payload) {
							serviceRemoved = append(serviceRemoved, params)
							go events.EmitSync("$registry.service.removed", serviceRemoved)
						},
					},
				},
			})
			onEvent := func(event string, callback func(list []moleculer.Payload, cancel func())) {
				events.On(event, func(v ...interface{}) {
					list := v[0].([]moleculer.Payload)
					callback(list, func() {
						events = bus.Construct()
					})
				})
			}

			Expect(printerBroker.LocalNode().GetID()).Should(Equal("node_printerBroker"))

			scannerBroker := createScannerBroker(mem)
			Expect(scannerBroker.LocalNode().GetID()).Should(Equal("node_scannerBroker"))

			cpuBroker := createCpuBroker(mem)
			Expect(cpuBroker.LocalNode().GetID()).Should(Equal("node_cpuBroker"))

			printerBroker.Start()

			printText := "TEXT TO PRINT"
			printResult := <-printerBroker.Call("printer.print", printText)
			Expect(printResult.Error()).Should(BeNil())
			Expect(printResult.Value()).Should(Equal(printText))

			scanText := "TEXT TO SCAN"

			scanResult := <-printerBroker.Call("scanner.scan", printText)
			Expect(scanResult.IsError()).Should(BeTrue())

			scannerBroker.Start()

			step := make(chan bool)
			onEvent("$registry.service.added", func(list []moleculer.Payload, cancel func()) {
				if hasNode(serviceAdded, "node_scannerBroker") {
					cancel()
					step <- true
				}
			})
			<-step

			scanResult = <-scannerBroker.Call("scanner.scan", scanText)
			Expect(scanResult.IsError()).ShouldNot(Equal(true))
			Expect(scanResult.Value()).Should(Equal(scanText))

			scanResult = <-printerBroker.Call("scanner.scan", scanText)
			Expect(scanResult.IsError()).ShouldNot(Equal(true))
			Expect(scanResult.Value()).Should(Equal(scanText))

			cpuBroker.Start()

			serviceAdded = []moleculer.Payload{}
			step = make(chan bool)
			onEvent("$registry.service.added", func(list []moleculer.Payload, cancel func()) {
				if hasNode(serviceAdded, "node_cpuBroker") {
					cancel()
					step <- true
				}
			})
			<-step
			cpuBroker.WaitForActions("scanner.scan", "printer.print")
			time.Sleep(time.Millisecond)

			contentToCompute := "Some long long text ..."
			computeResult := <-printerBroker.Call("cpu.compute", contentToCompute)
			Expect(computeResult.Error()).Should(Succeed())
			Expect(computeResult.Value()).Should(Equal(contentToCompute))

			//stopping broker B
			scannerBroker.Stop()

			step = make(chan bool)
			onEvent("$registry.service.removed", func(list []moleculer.Payload, cancel func()) {
				if hasNode(serviceRemoved, "node_scannerBroker") {
					cancel()
					step <- true
				}
			})
			<-step

			Expect(func() {
				<-scannerBroker.Call("scanner.scan", scanText)
			}).Should(Panic()) //broker B is stopped ... so it should panic

			close(done)
		}, 3)
	})

	Describe("Namespace", func() {

		It("Services across namespaces cannos see each other", func(done Done) {

			mem := &memory.SharedMemory{}

			devBroker := broker.New(&moleculer.Config{
				DiscoverNodeID: func() string { return "node1_devBroker" },
				LogLevel:       logLevel,
				Namespace:      "dev",
				TransporterFactory: func() interface{} {
					transport := memory.Create(log.WithField("transport", "memory"), mem)
					return &transport
				},
			})

			stageBroker := broker.New(&moleculer.Config{
				DiscoverNodeID: func() string { return "node1_stageBroker" },
				LogLevel:       logLevel,
				Namespace:      "stage",
				TransporterFactory: func() interface{} {
					transport := memory.Create(log.WithField("transport", "memory"), mem)
					return &transport
				},
			})

			stage2Broker := broker.New(&moleculer.Config{
				DiscoverNodeID: func() string { return "node1_stage2Broker" },
				LogLevel:       logLevel,
				Namespace:      "stage",
				TransporterFactory: func() interface{} {
					transport := memory.Create(log.WithField("transport", "memory"), mem)
					return &transport
				},
			})

			//alarm service - prints the alarm and return the namespace :)
			alarmService := func(namemspace string) moleculer.ServiceSchema {
				return moleculer.ServiceSchema{
					Name: "alarm",
					Actions: []moleculer.Action{
						{
							Name: "bell",
							Handler: func(context moleculer.Context, params moleculer.Payload) interface{} {
								context.Logger().Info("alarm.bell ringing !!! namemspace: ", namemspace)
								return namemspace
							},
						},
					},
				}
			}

			//available in the dev namespace only
			devOnlyService := moleculer.ServiceSchema{
				Name: "devOnly",
				Actions: []moleculer.Action{
					{
						Name: "code",
						Handler: func(context moleculer.Context, params moleculer.Payload) interface{} {
							return "🧠"
						},
					},
				},
			}

			devBroker.Publish(alarmService("dev"))
			devBroker.Publish(devOnlyService)
			devBroker.Start()

			stageBroker.Start()
			stage2Broker.Publish(moleculer.ServiceSchema{
				Name: "stage2",
				Actions: []moleculer.Action{
					{
						Name: "where",
						Handler: func(context moleculer.Context, params moleculer.Payload) interface{} {
							return "🌏"
						},
					},
				},
			})
			stage2Broker.Start()

			devAlarm := <-devBroker.Call("alarm.bell", nil)
			Expect(devAlarm.IsError()).Should(BeFalse())
			Expect(devAlarm.String()).Should(Equal("dev"))

			code := <-devBroker.Call("devOnly.code", nil)
			Expect(code.IsError()).Should(BeFalse())
			Expect(code.String()).Should(Equal("🧠"))

			time.Sleep(time.Millisecond)

			//alarm.bell should not be accessible to the stage broker
			stageAlarm := <-stageBroker.Call("alarm.bell", nil)
			Expect(stageAlarm.IsError()).Should(BeTrue())
			Expect(stageAlarm.Error().Error()).Should(Equal("Registry - endpoint not found for actionName: alarm.bell namespace: stage"))

			stageBroker.Publish(alarmService("stage"))
			stageAlarm = <-stageBroker.Call("alarm.bell", nil)
			Expect(stageAlarm.IsError()).Should(BeFalse())
			Expect(stageAlarm.String()).Should(Equal("stage"))

			code = <-stageBroker.Call("good.code", nil)
			Expect(code.IsError()).Should(BeTrue())
			Expect(code.Error().Error()).Should(Equal("Registry - endpoint not found for actionName: good.code namespace: stage"))

			//make sure 2 brokers on the same namespace can talk to each other
			msg := <-stageBroker.Call("stage2.where", nil)
			Expect(msg.IsError()).Should(BeFalse())
			Expect(msg.String()).Should(Equal("🌏"))

			devBroker.Stop()
			stageBroker.Stop()
			stage2Broker.Stop()

			close(done)
		}, 2)
	})
})
