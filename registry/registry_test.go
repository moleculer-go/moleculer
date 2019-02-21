package registry_test

import (
	"fmt"
	"time"

	snap "github.com/moleculer-go/cupaloy"
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
	"github.com/moleculer-go/moleculer/transit/memory"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
)

var logLevel = "FATAL"

func createPrinterBroker(mem *memory.SharedMemory) broker.ServiceBroker {
	broker := broker.FromConfig(&moleculer.BrokerConfig{
		DiscoverNodeID: func() string { return "node_printerBroker" },
		LogLevel:       logLevel,
		TransporterFactory: func() interface{} {
			transport := memory.Create(log.WithField("transport", "memory"), mem)
			return &transport
		},
	})

	broker.AddService(moleculer.Service{
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
	})

	return (*broker)
}

func createScannerBroker(mem *memory.SharedMemory) broker.ServiceBroker {
	broker := broker.FromConfig(&moleculer.BrokerConfig{
		DiscoverNodeID: func() string { return "node_scannerBroker" },
		LogLevel:       logLevel,
		TransporterFactory: func() interface{} {
			transport := memory.Create(log.WithField("transport", "memory"), mem)
			return &transport
		},
	})
	broker.AddService(moleculer.Service{
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
	})

	return (*broker)
}

func createCpuBroker(mem *memory.SharedMemory) broker.ServiceBroker {
	broker := broker.FromConfig(&moleculer.BrokerConfig{
		DiscoverNodeID: func() string { return "node_cpuBroker" },
		LogLevel:       logLevel,
		TransporterFactory: func() interface{} {
			transport := memory.Create(log.WithField("transport", "memory"), mem)
			return &transport
		},
	})
	broker.AddService(moleculer.Service{
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
	return in
}

func cleanupAction(ins []map[string]interface{}) []map[string]interface{} {
	result := make([]map[string]interface{}, len(ins))
	for index, item := range ins {
		result[index] = map[string]interface{}{
			"name": item["name"],
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
				time.Sleep(100 * time.Millisecond)

				result = <-scannerBroker.Call(action, params)
				Expect(result.Exists()).Should(BeTrue())
				Expect(snap.SnapshotMulti(fmt.Sprint(label, "2"), transformer(result))).Should(Succeed())

				cpuBroker := createCpuBroker(mem)
				cpuBroker.Start()
				time.Sleep(100 * time.Millisecond)

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
					findBy("name", "printer", list),
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

			It("$node.actions - all false", harness("$node.actions", "all-false", map[string]interface{}{
				"withEndpoints": false,
				"skipInternal":  false,
				"onlyAvailable": false,
				"onlyLocal":     false,
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
				"withActions":   false,
				"withEvents":    false,
				"skipInternal":  false,
				"onlyAvailable": false,
				"onlyLocal":     false,
			}, extractServices))

			It("$node.services - withActions", harness("$node.services", "withActions", map[string]interface{}{
				"withActions":   true,
				"withEvents":    false,
				"skipInternal":  false,
				"onlyAvailable": false,
				"onlyLocal":     false,
			}, extractServices))

			It("$node.services - withEvents", harness("$node.services", "withEvents", map[string]interface{}{
				"withActions":   false,
				"withEvents":    true,
				"skipInternal":  false,
				"onlyAvailable": false,
				"onlyLocal":     false,
			}, extractServices))

			It("$node.services - skipInternal", harness("$node.services", "skipInternal", map[string]interface{}{
				"withActions":   false,
				"withEvents":    false,
				"skipInternal":  true,
				"onlyAvailable": false,
				"onlyLocal":     false,
			}, extractServices))

			It("$node.services - onlyAvailable", harness("$node.services", "onlyAvailable", map[string]interface{}{
				"withActions":   false,
				"withEvents":    false,
				"skipInternal":  false,
				"onlyAvailable": true,
				"onlyLocal":     false,
			}, extractServices))
			It("$node.services - onlyLocal", harness("$node.services", "onlyLocal", map[string]interface{}{
				"withActions":   false,
				"withEvents":    false,
				"skipInternal":  false,
				"onlyAvailable": false,
				"onlyLocal":     true,
			}, extractServices))
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
			Expect(printResult.IsError()).Should(BeFalse())
			Expect(printResult.Value()).Should(Equal(printText))

			scanText := "TEXT TO SCAN"
			Expect(func() {
				<-printerBroker.Call("scanner.scan", scanText)
			}).Should(Panic()) //broker B is not started yet.. so should panic

			scannerBroker.Start()
			time.Sleep(time.Second)

			scanResult := <-scannerBroker.Call("scanner.scan", scanText)
			Expect(scanResult.IsError()).ShouldNot(Equal(true))
			Expect(scanResult.Value()).Should(Equal(scanText))

			scanResult = <-printerBroker.Call("scanner.scan", scanText)
			Expect(scanResult.IsError()).ShouldNot(Equal(true))
			Expect(scanResult.Value()).Should(Equal(scanText))

			cpuBroker.Start()
			time.Sleep(time.Second) //sleep until services are registered

			contentToCompute := "Some long long text ..."
			computeResult := <-cpuBroker.Call("cpu.compute", contentToCompute)
			Expect(computeResult.IsError()).ShouldNot(Equal(true))
			Expect(computeResult.Value()).Should(Equal(contentToCompute))

			//stopping broker B
			scannerBroker.Stop() // TODO -> not  implemented yet
			time.Sleep(time.Second)

			Expect(func() {
				<-scannerBroker.Call("scanner.scan", scanText)
			}).Should(Panic()) //broker B is stoped ... so it should panic
		})
	})
})
