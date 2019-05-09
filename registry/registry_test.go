package registry_test

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/moleculer-go/cupaloy/v2"
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
	"github.com/moleculer-go/moleculer/test"
	"github.com/moleculer-go/moleculer/transit/memory"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
)

var logLevel = "ERROR"
var snap = cupaloy.New(cupaloy.FailOnUpdate(os.Getenv("UPDATE_SNAPSHOTS") == ""))

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

func cleanupNode(in map[string]interface{}) map[string]interface{} {
	if in == nil {
		return nil
	}
	if len(in) == 0 {
		return make(map[string]interface{})
	}
	in["ipList"] = []string{"100.100.0.100"}
	in["hostname"] = "removed"
	in["seq"] = "removed"
	return in
}

func cleanupAction(ins []map[string]interface{}) []map[string]interface{} {
	result := make([]map[string]interface{}, len(ins))
	for index, item := range ins {
		result[index] = map[string]interface{}{
			"name":      item["name"],
			"count":     "removed",
			"hasLocal":  item["hasLocal"],
			"available": item["available"],
			"endpoints": item["endpoints"],
		}
	}
	return result
}

func first(list []map[string]interface{}) map[string]interface{} {
	if list != nil && len(list) > 0 {
		return list[0]
	}
	return nil
}

func orderEndpoints(list []map[string]interface{}) []map[string]interface{} {
	result := make([]map[string]interface{}, len(list))
	for idx, item := range list {
		endpointsTemp, exists := item["endpoints"]
		if exists {
			endpoints := endpointsTemp.([]map[string]interface{})
			item["endpoints"] = test.OrderMapArray(endpoints, "nodeID")
		}
		result[idx] = item
	}
	return result
}

func findBy(field, value string, list []moleculer.Payload) []map[string]interface{} {
	result := make([]map[string]interface{}, 0)
	for _, item := range list {
		if item.Get(field).String() == value {
			result = append(result, item.RawMap())
		}
	}
	return result
}

var _ = Describe("Registry", func() {

	Describe("Local Service $node", func() {
		harness := func(action string, scenario string, params map[string]interface{}, transformer func(interface{}) interface{}) func() {
			label := fmt.Sprint(scenario, "-", action)
			return func() {
				mem := &memory.SharedMemory{}

				printerBroker := createPrinterBroker(mem)
				printerBroker.Start()

				result := <-printerBroker.Call(action, params)
				Expect(result.Exists()).Should(BeTrue())
				Expect(snap.SnapshotMulti(fmt.Sprint(label, "1"), transformer(result))).Should(Succeed())

				scannerBroker := createScannerBroker(mem)
				scannerBroker.Start()
				time.Sleep(300 * time.Millisecond)

				result = <-scannerBroker.Call(action, params)
				Expect(result.Exists()).Should(BeTrue())
				Expect(snap.SnapshotMulti(fmt.Sprint(label, "2"), transformer(result))).Should(Succeed())

				cpuBroker := createCpuBroker(mem)
				cpuBroker.Start()
				time.Sleep(300 * time.Millisecond)

				result = <-cpuBroker.Call(action, params)
				Expect(result.Exists()).Should(BeTrue())
				Expect(snap.SnapshotMulti(fmt.Sprint(label, "3"), transformer(result))).Should(Succeed())
			}
		}

		Context("$node.list action", func() {

			extractNodes := func(in interface{}) interface{} {
				list := in.(moleculer.Payload).Array()
				return map[string]map[string]interface{}{
					"noPrinterBroker":     cleanupNode(first(findBy("id", "node_printerBroker", list))),
					"nodedeScannerBroker": cleanupNode(first(findBy("id", "node_scannerBroker", list))),
					"nodeCpuBroker":       cleanupNode(first(findBy("id", "node_cpuBroker", list))),
				}
			}

			extractServices := func(in interface{}) interface{} {
				list := in.(moleculer.Payload).Array()
				return [][]map[string]interface{}{
					orderEndpoints(findBy("name", "printer", list)),
					findBy("name", "scanner", list),
					findBy("name", "cpu", list),
					findBy("name", "$node", list),
				}
			}

			extractActions := func(in interface{}) interface{} {
				list := in.(moleculer.Payload).Array()
				return [][]map[string]interface{}{
					cleanupAction(findBy("name", "printer.print", list)),
					cleanupAction(findBy("name", "scanner.scan", list)),
					cleanupAction(findBy("name", "cpu.compute", list)),
					cleanupAction(findBy("name", "$node.list", list)),
					cleanupAction(findBy("name", "$node.services", list)),
					cleanupAction(findBy("name", "$node.actions", list)),
					cleanupAction(findBy("name", "$node.events", list)),
				}
			}

			extractEvents := func(in interface{}) interface{} {
				list := in.(moleculer.Payload).Array()
				return [][]map[string]interface{}{
					cleanupAction(findBy("name", "printer.printed", list)),
					cleanupAction(findBy("name", "scanner.scanned", list)),
				}
			}

			It("$node.events - all false", harness("$node.events", "all-false", map[string]interface{}{
				"withEndpoints": false,
				"onlyAvailable": false,
				"onlyLocal":     false,
			}, extractEvents))

			It("$node.events - all true", harness("$node.events", "all-true", map[string]interface{}{
				"withEndpoints": true,
				"onlyAvailable": true,
				"onlyLocal":     true,
			}, extractEvents))

			It("$node.actions - all false", harness("$node.actions", "all-false", map[string]interface{}{
				"withEndpoints": false,
				"skipInternal":  false,
				"onlyAvailable": false,
				"onlyLocal":     false,
			}, extractActions))

			It("$node.actions - all true", harness("$node.actions", "all-true", map[string]interface{}{
				"withEndpoints": true,
				"skipInternal":  true,
				"onlyAvailable": true,
				"onlyLocal":     true,
			}, extractActions))

			It("$node.actions - withEndpoints", harness("$node.actions", "withEndpoints", map[string]interface{}{
				"withEndpoints": true,
				"skipInternal":  false,
				"onlyAvailable": false,
				"onlyLocal":     false,
			}, extractActions))

			It("$node.actions - skipInternal", harness("$node.actions", "skipInternal", map[string]interface{}{
				"withEndpoints": false,
				"skipInternal":  true,
				"onlyAvailable": false,
				"onlyLocal":     false,
			}, extractActions))

			It("$node.actions - onlyAvailable", harness("$node.actions", "onlyAvailable", map[string]interface{}{
				"withEndpoints": false,
				"skipInternal":  false,
				"onlyAvailable": true,
				"onlyLocal":     false,
			}, extractActions))

			It("$node.actions - onlyLocal", harness("$node.actions", "onlyLocal", map[string]interface{}{
				"withEndpoints": false,
				"skipInternal":  false,
				"onlyAvailable": false,
				"onlyLocal":     true,
			}, extractActions))

			It("$node.list with no services", harness("$node.list", "no-services", map[string]interface{}{
				"withServices":  false,
				"onlyAvailable": false,
			}, extractNodes))

			It("$node.list with services", harness("$node.list", "with-services", map[string]interface{}{
				"withServices":  true,
				"onlyAvailable": false,
			}, extractNodes))

			It("$node.services - all false", harness("$node.services", "all-false", map[string]interface{}{
				"withEndpoints": false,
				"withActions":   false,
				"withEvents":    false,
				"skipInternal":  false,
				"onlyAvailable": false,
				"onlyLocal":     false,
			}, extractServices))

			It("$node.services - all true", harness("$node.services", "all-true", map[string]interface{}{
				"withEndpoints": true,
				"withActions":   true,
				"withEvents":    true,
				"skipInternal":  true,
				"onlyAvailable": true,
				"onlyLocal":     true,
			}, extractServices))

			It("$node.services - withActions", harness("$node.services", "withActions", map[string]interface{}{
				"withActions":   true,
				"withEndpoints": false,
				"withEvents":    false,
				"skipInternal":  false,
				"onlyAvailable": false,
				"onlyLocal":     false,
			}, extractServices))

			It("$node.services - withEndpoints", harness("$node.services", "withEndpoints", map[string]interface{}{
				"withActions":   false,
				"withEndpoints": true,
				"withEvents":    false,
				"skipInternal":  false,
				"onlyAvailable": false,
				"onlyLocal":     false,
			}, extractServices))

			It("$node.services - withEvents", harness("$node.services", "withEvents", map[string]interface{}{
				"withActions":   false,
				"withEndpoints": false,
				"withEvents":    true,
				"skipInternal":  false,
				"onlyAvailable": false,
				"onlyLocal":     false,
			}, extractServices))

			It("$node.services - skipInternal", harness("$node.services", "skipInternal", map[string]interface{}{
				"withActions":   false,
				"withEndpoints": false,
				"withEvents":    false,
				"skipInternal":  true,
				"onlyAvailable": false,
				"onlyLocal":     false,
			}, extractServices))

			It("$node.services - onlyAvailable", harness("$node.services", "onlyAvailable", map[string]interface{}{
				"withActions":   false,
				"withEndpoints": false,
				"withEvents":    false,
				"skipInternal":  false,
				"onlyAvailable": true,
				"onlyLocal":     false,
			}, extractServices))

			It("$node.services - onlyLocal", harness("$node.services", "onlyLocal", map[string]interface{}{
				"withActions":   false,
				"withEndpoints": false,
				"withEvents":    false,
				"skipInternal":  false,
				"onlyAvailable": false,
				"onlyLocal":     true,
			}, extractServices))
		})

		It("Should subscribe for internal events and receive events when happen :)", func() {
			var serviceAdded []map[string]interface{}
			addedMutex := &sync.Mutex{}
			addedChan := make(chan bool)
			var serviceRemoved []string
			mem := &memory.SharedMemory{}
			bkr1 := broker.New(&moleculer.Config{
				DiscoverNodeID: func() string { return "test-node1" },
				LogLevel:       logLevel,
				TransporterFactory: func() interface{} {
					transport := memory.Create(log.WithField("transport", "memory"), mem)
					return &transport
				},
			})
			bkr1.Publish(moleculer.ServiceSchema{
				Name: "internal-consumer",
				Events: []moleculer.Event{
					moleculer.Event{
						Name: "$registry.service.added",
						Handler: func(ctx moleculer.Context, params moleculer.Payload) {
							addedMutex.Lock()
							defer addedMutex.Unlock()
							serviceAdded = append(serviceAdded, params.RawMap())
							addedChan <- true
						},
					},
					moleculer.Event{
						Name: "$registry.service.removed",
						Handler: func(ctx moleculer.Context, params moleculer.Payload) {
							serviceRemoved = append(serviceRemoved, params.String())
						},
					},
				},
			})
			bkr1.Start()

			bkr1.Publish(moleculer.ServiceSchema{
				Name: "service-added",
			})

			<-addedChan
			<-addedChan
			<-addedChan
			Expect(snap.SnapshotMulti("local-serviceAdded", serviceAdded)).ShouldNot(HaveOccurred())
			Expect(snap.SnapshotMulti("empty-serviceRemoved", serviceRemoved)).ShouldNot(HaveOccurred())

			//add another node.. so test service removed is invoked
			bkr2 := broker.New(&moleculer.Config{
				DiscoverNodeID: func() string { return "test-node2" },
				LogLevel:       logLevel,
				TransporterFactory: func() interface{} {
					transport := memory.Create(log.WithField("transport", "memory"), mem)
					return &transport
				},
			})
			bkr2.Publish(moleculer.ServiceSchema{
				Name:         "remote-service",
				Dependencies: []string{"internal-consumer", "service-added"},
			})
			bkr2.Start()
			time.Sleep(300 * time.Millisecond)

			<-addedChan
			sort.Strings(serviceRemoved)
			Expect(snap.SnapshotMulti("remote-serviceAdded", serviceAdded)).ShouldNot(HaveOccurred())
			Expect(snap.SnapshotMulti("empty-serviceRemoved", serviceRemoved)).ShouldNot(HaveOccurred())

			//stop broker 2 .. should remove services on broker 1
			bkr2.Stop()
			time.Sleep(200 * time.Millisecond)
			sort.Strings(serviceRemoved)
			Expect(snap.SnapshotMulti("remote-serviceRemoved", serviceRemoved)).ShouldNot(HaveOccurred())

			bkr1.Stop()
		})
	})

	Describe("Auto discovery", func() {

		It("3 brokers should auto discovery and perform local and remote Calls", func() {

			mem := &memory.SharedMemory{}

			printerBroker := createPrinterBroker(mem)
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
			time.Sleep(time.Millisecond * 300)

			scanResult = <-scannerBroker.Call("scanner.scan", scanText)
			Expect(scanResult.IsError()).ShouldNot(Equal(true))
			Expect(scanResult.Value()).Should(Equal(scanText))

			scanResult = <-printerBroker.Call("scanner.scan", scanText)
			Expect(scanResult.IsError()).ShouldNot(Equal(true))
			Expect(scanResult.Value()).Should(Equal(scanText))

			cpuBroker.Start()
			time.Sleep(time.Millisecond * 300) //sleep until services are registered

			contentToCompute := "Some long long text ..."
			computeResult := <-cpuBroker.Call("cpu.compute", contentToCompute)
			Expect(computeResult.IsError()).ShouldNot(Equal(true))
			Expect(computeResult.Value()).Should(Equal(contentToCompute))

			//stopping broker B
			scannerBroker.Stop() // TODO -> not  implemented yet
			time.Sleep(time.Millisecond * 300)

			Expect(func() {
				<-scannerBroker.Call("scanner.scan", scanText)
			}).Should(Panic()) //broker B is stopped ... so it should panic
		})
	})
})
