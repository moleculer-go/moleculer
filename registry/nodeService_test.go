package registry_test

import (
	"fmt"
	"sync"

	bus "github.com/moleculer-go/goemitter"
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
	"github.com/moleculer-go/moleculer/test"
	"github.com/moleculer-go/moleculer/transit/memory"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
)

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

var _ = Describe("nodeService", func() {
	Describe("Local Service $node", func() {
		harness := func(action string, scenario string, params map[string]interface{}, transformer func(interface{}) interface{}) func(done Done) {
			label := fmt.Sprint(scenario, "-", action)
			return func(done Done) {
				mem := &memory.SharedMemory{}

				printerBroker := createPrinterBroker(mem)
				var serviceAdded, serviceRemoved []moleculer.Payload
				events := bus.Construct()
				addedMutex := &sync.Mutex{}

				onEvent := func(event string, callback func(list []moleculer.Payload, cancel func())) {
					events.On(event, func(v ...interface{}) {
						list := v[0].([]moleculer.Payload)
						callback(list, func() {
							events = bus.Construct()
						})
					})
				}

				printerBroker.Publish(moleculer.ServiceSchema{
					Name: "internal-consumer",
					Events: []moleculer.Event{
						moleculer.Event{
							Name: "$registry.service.added",
							Handler: func(ctx moleculer.Context, params moleculer.Payload) {
								addedMutex.Lock()
								defer addedMutex.Unlock()
								serviceAdded = append(serviceAdded, params)
								events.EmitSync("$registry.service.added", serviceAdded)
							},
						},
						moleculer.Event{
							Name: "$registry.service.removed",
							Handler: func(ctx moleculer.Context, params moleculer.Payload) {
								serviceRemoved = append(serviceRemoved, params)
								events.EmitSync("$registry.service.removed", serviceRemoved)
							},
						},
					},
				})
				printerBroker.Start()

				result := <-printerBroker.Call(action, params)
				Expect(result.Exists()).Should(BeTrue())
				Expect(snap.SnapshotMulti(fmt.Sprint(label, "1"), transformer(result))).Should(Succeed())

				scannerBroker := createScannerBroker(mem)
				scannerBroker.Start()

				step := make(chan bool)
				onEvent("$registry.service.added", func(list []moleculer.Payload, cancel func()) {
					if hasService(serviceAdded, "scanner") {
						cancel()
						step <- true
					}
				})
				<-step

				result = <-scannerBroker.Call(action, params)
				Expect(result.Exists()).Should(BeTrue())
				Expect(snap.SnapshotMulti(fmt.Sprint(label, "2"), transformer(result))).Should(Succeed())

				cpuBroker := createCpuBroker(mem)
				cpuBroker.Start()
				step = make(chan bool)
				onEvent("$registry.service.added", func(list []moleculer.Payload, cancel func()) {
					if hasService(serviceAdded, "cpu") {
						cancel()
						step <- true
					}
				})
				<-step

				result = <-cpuBroker.Call(action, params)
				Expect(result.Exists()).Should(BeTrue())
				Expect(snap.SnapshotMulti(fmt.Sprint(label, "3"), transformer(result))).Should(Succeed())

				close(done)
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
			}, extractEvents), 3)

			It("$node.events - all true", harness("$node.events", "all-true", map[string]interface{}{
				"withEndpoints": true,
				"onlyAvailable": true,
				"onlyLocal":     true,
			}, extractEvents), 3)

			It("$node.actions - all false", harness("$node.actions", "all-false", map[string]interface{}{
				"withEndpoints": false,
				"skipInternal":  false,
				"onlyAvailable": false,
				"onlyLocal":     false,
			}, extractActions), 3)

			It("$node.actions - all true", harness("$node.actions", "all-true", map[string]interface{}{
				"withEndpoints": true,
				"skipInternal":  true,
				"onlyAvailable": true,
				"onlyLocal":     true,
			}, extractActions), 3)

			It("$node.actions - withEndpoints", harness("$node.actions", "withEndpoints", map[string]interface{}{
				"withEndpoints": true,
				"skipInternal":  false,
				"onlyAvailable": false,
				"onlyLocal":     false,
			}, extractActions), 3)

			It("$node.actions - skipInternal", harness("$node.actions", "skipInternal", map[string]interface{}{
				"withEndpoints": false,
				"skipInternal":  true,
				"onlyAvailable": false,
				"onlyLocal":     false,
			}, extractActions), 3)

			It("$node.actions - onlyAvailable", harness("$node.actions", "onlyAvailable", map[string]interface{}{
				"withEndpoints": false,
				"skipInternal":  false,
				"onlyAvailable": true,
				"onlyLocal":     false,
			}, extractActions), 3)

			It("$node.actions - onlyLocal", harness("$node.actions", "onlyLocal", map[string]interface{}{
				"withEndpoints": false,
				"skipInternal":  false,
				"onlyAvailable": false,
				"onlyLocal":     true,
			}, extractActions), 3)

			It("$node.list with no services", harness("$node.list", "no-services", map[string]interface{}{
				"withServices":  false,
				"onlyAvailable": false,
			}, extractNodes), 3)

			It("$node.list with services", harness("$node.list", "with-services", map[string]interface{}{
				"withServices":  true,
				"onlyAvailable": false,
			}, extractNodes), 3)

			It("$node.services - all false", harness("$node.services", "all-false", map[string]interface{}{
				"withEndpoints": false,
				"withActions":   false,
				"withEvents":    false,
				"skipInternal":  false,
				"onlyAvailable": false,
				"onlyLocal":     false,
			}, extractServices), 3)

			It("$node.services - all true", harness("$node.services", "all-true", map[string]interface{}{
				"withEndpoints": true,
				"withActions":   true,
				"withEvents":    true,
				"skipInternal":  true,
				"onlyAvailable": true,
				"onlyLocal":     true,
			}, extractServices), 3)

			It("$node.services - withActions", harness("$node.services", "withActions", map[string]interface{}{
				"withActions":   true,
				"withEndpoints": false,
				"withEvents":    false,
				"skipInternal":  false,
				"onlyAvailable": false,
				"onlyLocal":     false,
			}, extractServices), 3)

			It("$node.services - withEndpoints", harness("$node.services", "withEndpoints", map[string]interface{}{
				"withActions":   false,
				"withEndpoints": true,
				"withEvents":    false,
				"skipInternal":  false,
				"onlyAvailable": false,
				"onlyLocal":     false,
			}, extractServices), 3)

			It("$node.services - withEvents", harness("$node.services", "withEvents", map[string]interface{}{
				"withActions":   false,
				"withEndpoints": false,
				"withEvents":    true,
				"skipInternal":  false,
				"onlyAvailable": false,
				"onlyLocal":     false,
			}, extractServices), 3)

			It("$node.services - skipInternal", harness("$node.services", "skipInternal", map[string]interface{}{
				"withActions":   false,
				"withEndpoints": false,
				"withEvents":    false,
				"skipInternal":  true,
				"onlyAvailable": false,
				"onlyLocal":     false,
			}, extractServices), 3)

			It("$node.services - onlyAvailable", harness("$node.services", "onlyAvailable", map[string]interface{}{
				"withActions":   false,
				"withEndpoints": false,
				"withEvents":    false,
				"skipInternal":  false,
				"onlyAvailable": true,
				"onlyLocal":     false,
			}, extractServices), 3)

			It("$node.services - onlyLocal", harness("$node.services", "onlyLocal", map[string]interface{}{
				"withActions":   false,
				"withEndpoints": false,
				"withEvents":    false,
				"skipInternal":  false,
				"onlyAvailable": false,
				"onlyLocal":     true,
			}, extractServices), 3)
		})

		It("Should subscribe for internal events and receive events when happen :)", func() {
			var serviceAdded, serviceRemoved []moleculer.Payload
			events := bus.Construct()
			addedMutex := &sync.Mutex{}
			mem := &memory.SharedMemory{}
			bkr1 := broker.New(&moleculer.Config{
				DiscoverNodeID: func() string { return "test-node1" },
				LogLevel:       logLevel,
				TransporterFactory: func() interface{} {
					transport := memory.Create(log.WithField("transport", "memory"), mem)
					return &transport
				},
			})

			onEvent := func(event string, callback func(list []moleculer.Payload, cancel func())) {
				var cb func(v ...interface{})
				cb = func(v ...interface{}) {
					list := v[0].([]moleculer.Payload)
					callback(list, func() {
						events.RemoveListener(event, cb)
					})
				}
				events.On(event, cb)
			}

			bkr1.Publish(moleculer.ServiceSchema{
				Name: "internal-consumer",
				Events: []moleculer.Event{
					moleculer.Event{
						Name: "$registry.service.added",
						Handler: func(ctx moleculer.Context, params moleculer.Payload) {
							addedMutex.Lock()
							defer addedMutex.Unlock()
							serviceAdded = append(serviceAdded, params)
							events.EmitSync("$registry.service.added", serviceAdded)
						},
					},
					moleculer.Event{
						Name: "$registry.service.removed",
						Handler: func(ctx moleculer.Context, params moleculer.Payload) {
							serviceRemoved = append(serviceRemoved, params)
							events.EmitSync("$registry.service.removed", serviceRemoved)
						},
					},
				},
			})
			bkr1.Start()

			bkr1.Publish(moleculer.ServiceSchema{
				Name: "service-added",
			})

			step := make(chan bool)
			onEvent("$registry.service.added", func(list []moleculer.Payload, cancel func()) {
				if len(list) == 3 {
					cancel()
					step <- true
				}
			})
			<-step
			Expect(snap.SnapshotMulti("local-serviceAdded", serviceAdded)).ShouldNot(HaveOccurred())

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

			serviceAdded = []moleculer.Payload{}
			serviceRemoved = []moleculer.Payload{}

			bkr2.Start()

			step = make(chan bool)
			onEvent("$registry.service.added", func(list []moleculer.Payload, cancel func()) {
				if hasService(serviceAdded, "remote-service") {
					cancel()
					step <- true
				}
			})
			<-step
			Expect(snap.SnapshotMulti("remote-serviceAdded", serviceAdded)).ShouldNot(HaveOccurred())

			step = make(chan bool)
			onEvent("$registry.service.removed", func(list []moleculer.Payload, cancel func()) {
				if hasService(serviceRemoved, "remote-service") {
					cancel()
					step <- true
				}
			})
			//stop broker 2 .. should remove services on broker 1
			bkr2.Stop()
			<-step
			fmt.Println("after remote-service removed")
			Expect(snap.SnapshotMulti("remote-serviceRemoved", serviceRemoved)).ShouldNot(HaveOccurred())

			bkr1.Stop()
		})
	})
})

func hasService(list []moleculer.Payload, name string) bool {
	for _, p := range list {
		if p.Get("name").String() == name {
			return true
		}
	}
	return false
}
