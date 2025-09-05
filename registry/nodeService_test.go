package registry_test

import (
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/test"
	"github.com/moleculer-go/moleculer/transit/memory"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
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
	// Always include the port field (defaults to 0 if not set)
	in["port"] = 0
	return in
}

func cleanupAction(ins []map[string]interface{}) []map[string]interface{} {
	result := make([]map[string]interface{}, len(ins))
	for index, item := range ins {
		_, endpointsExists := item["endpoints"]
		result[index] = map[string]interface{}{
			"name":      item["name"],
			"count":     "removed",
			"hasLocal":  item["hasLocal"],
			"available": item["available"],
			"endpoints": endpointsExists,
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

		Context("$node.list action", func() {

			assertNodeList := func(result moleculer.Payload, expectedNodeCount int) {
				Expect(result.Exists()).Should(BeTrue())
				list := result.Array()
				Expect(len(list)).Should(Equal(expectedNodeCount))

				// Find the printer broker node and clean it up for testing
				printerNode := cleanupNode(first(findBy("id", "node_printerBroker", list)))
				Expect(printerNode).ShouldNot(BeNil())

				// Assert fixed values (after cleanup)
				Expect(printerNode["id"]).Should(Equal("node_printerBroker"))
				Expect(printerNode["cpu"]).Should(Equal(int64(0)))
				Expect(printerNode["cpuSeq"]).Should(Equal(int64(0)))
				Expect(printerNode["port"]).Should(Equal(0))
				Expect(printerNode["available"]).Should(Equal(true))
				Expect(printerNode["hostname"]).Should(Equal("removed"))
				Expect(printerNode["seq"]).Should(Equal("removed"))

				// Assert IP list (cleaned up)
				ipList, ok := printerNode["ipList"].([]string)
				Expect(ok).Should(BeTrue())
				Expect(len(ipList)).Should(Equal(1))
				Expect(ipList[0]).Should(Equal("100.100.0.100"))

				// Assert client info
				client, ok := printerNode["client"].(map[string]interface{})
				Expect(ok).Should(BeTrue())
				Expect(client["type"]).Should(Equal("moleculer-go"))
				Expect(client["version"]).Should(Equal("0.1.0"))
				Expect(client["langVersion"]).Should(Equal("1.5"))

				// Assert metadata exists (can be empty)
				metadata, ok := printerNode["metadata"].(map[string]interface{})
				Expect(ok).Should(BeTrue())
				Expect(metadata).ShouldNot(BeNil())

				// Assert services field exists (can be nil or any slice type)
				// Services field can be nil or a slice, both are valid
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

			assertNodeServices := func(result moleculer.Payload, expectedServiceNames []string) {
				Expect(result.Exists()).Should(BeTrue())

				// Use extractServices to transform the result into the expected format
				transformed := extractServices(result)
				serviceGroups := transformed.([][]map[string]interface{})
				Expect(len(serviceGroups)).Should(Equal(4)) // Always 4 groups: printer, scanner, cpu, $node

				// Collect all service names from all groups
				var foundServices []string
				for _, serviceGroup := range serviceGroups {
					for _, service := range serviceGroup {
						serviceName := service["name"].(string)
						foundServices = append(foundServices, serviceName)

						// Assert required fields exist
						Expect(service["name"]).ShouldNot(BeNil())
						Expect(service["version"]).ShouldNot(BeNil())
						Expect(service["hasLocal"]).ShouldNot(BeNil())
						Expect(service["available"]).ShouldNot(BeNil())

						// Assert metadata and settings exist (can be empty)
						metadata, ok := service["metadata"].(map[string]interface{})
						Expect(ok).Should(BeTrue())
						Expect(metadata).ShouldNot(BeNil())

						settings, ok := service["settings"].(map[string]interface{})
						Expect(ok).Should(BeTrue())
						Expect(settings).ShouldNot(BeNil())
					}
				}

				// Check that all expected services are present
				for _, expectedName := range expectedServiceNames {
					Expect(foundServices).Should(ContainElement(expectedName))
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

			assertNodeActions := func(result moleculer.Payload, expectedActionNames []string) {
				Expect(result.Exists()).Should(BeTrue())

				// Use extractActions to transform the result into the expected format
				transformed := extractActions(result)
				actionGroups := transformed.([][]map[string]interface{})
				Expect(len(actionGroups)).Should(Equal(7)) // Always 7 groups: printer.print, scanner.scan, cpu.compute, $node.list, $node.services, $node.actions, $node.events

				// Collect all action names from all groups
				var foundActions []string
				for _, actionGroup := range actionGroups {
					for _, action := range actionGroup {
						actionName := action["name"].(string)
						foundActions = append(foundActions, actionName)

						// Assert required fields exist
						Expect(action["name"]).ShouldNot(BeNil())
						Expect(action["count"]).ShouldNot(BeNil())
						Expect(action["hasLocal"]).ShouldNot(BeNil())
						Expect(action["available"]).ShouldNot(BeNil())
						Expect(action["endpoints"]).ShouldNot(BeNil())
					}
				}

				// Check that all expected actions are present
				for _, expectedName := range expectedActionNames {
					Expect(foundActions).Should(ContainElement(expectedName))
				}
			}

			extractEvents := func(in interface{}) interface{} {
				list := in.(moleculer.Payload).Array()
				return [][]map[string]interface{}{
					cleanupAction(findBy("name", "printer.printed", list)),
					cleanupAction(findBy("name", "scanner.scanned", list)),
				}
			}

			assertNodeEvents := func(result moleculer.Payload, expectedEventNames []string) {
				Expect(result.Exists()).Should(BeTrue())

				// Use extractEvents to transform the result into the expected format
				transformed := extractEvents(result)
				eventGroups := transformed.([][]map[string]interface{})
				Expect(len(eventGroups)).Should(Equal(2)) // Always 2 groups: printer.printed, scanner.scanned

				// Collect all event names from all groups
				var foundEvents []string
				for _, eventGroup := range eventGroups {
					for _, event := range eventGroup {
						eventName := event["name"].(string)
						foundEvents = append(foundEvents, eventName)

						// Assert required fields exist
						Expect(event["name"]).ShouldNot(BeNil())
						Expect(event["group"]).ShouldNot(BeNil())
					}
				}

				// Check that all expected events are present
				for _, expectedName := range expectedEventNames {
					Expect(foundEvents).Should(ContainElement(expectedName))
				}
			}

			It("$node.events - all false", func() {
				mem := &memory.SharedMemory{}
				printerBroker := createPrinterBroker(mem)
				printerBroker.Start()

				result := <-printerBroker.Call("$node.events", map[string]interface{}{
					"withEndpoints": false,
					"onlyAvailable": false,
					"onlyLocal":     false,
				})
				assertNodeEvents(result, []string{})

				scannerBroker := createScannerBroker(mem)
				scannerBroker.Start()
				scannerBroker.WaitForNodes("node_printerBroker")
				scannerBroker.WaitFor("printer")
				time.Sleep(time.Millisecond)

				result = <-scannerBroker.Call("$node.events", map[string]interface{}{
					"withEndpoints": false,
					"onlyAvailable": false,
					"onlyLocal":     false,
				})
				assertNodeEvents(result, []string{})

				cpuBroker := createCpuBroker(mem)
				cpuBroker.Start()
				cpuBroker.WaitForNodes("node_printerBroker", "node_scannerBroker")
				cpuBroker.WaitFor("printer", "scanner")
				time.Sleep(time.Millisecond)

				result = <-cpuBroker.Call("$node.events", map[string]interface{}{
					"withEndpoints": false,
					"onlyAvailable": false,
					"onlyLocal":     false,
				})
				assertNodeEvents(result, []string{})
			})

			It("$node.events - all true", func() {
				mem := &memory.SharedMemory{}
				printerBroker := createPrinterBroker(mem)
				printerBroker.Start()

				result := <-printerBroker.Call("$node.events", map[string]interface{}{
					"withEndpoints": true,
					"onlyAvailable": true,
					"onlyLocal":     true,
				})
				assertNodeEvents(result, []string{})

				scannerBroker := createScannerBroker(mem)
				scannerBroker.Start()
				scannerBroker.WaitForNodes("node_printerBroker")
				scannerBroker.WaitFor("printer")
				time.Sleep(time.Millisecond)

				result = <-scannerBroker.Call("$node.events", map[string]interface{}{
					"withEndpoints": true,
					"onlyAvailable": true,
					"onlyLocal":     true,
				})
				assertNodeEvents(result, []string{})

				cpuBroker := createCpuBroker(mem)
				cpuBroker.Start()
				cpuBroker.WaitForNodes("node_printerBroker", "node_scannerBroker")
				cpuBroker.WaitFor("printer", "scanner")
				time.Sleep(time.Millisecond)

				result = <-cpuBroker.Call("$node.events", map[string]interface{}{
					"withEndpoints": true,
					"onlyAvailable": true,
					"onlyLocal":     true,
				})
				assertNodeEvents(result, []string{})
			})

			It("$node.actions - all false", func() {
				mem := &memory.SharedMemory{}
				printerBroker := createPrinterBroker(mem)
				printerBroker.Start()

				result := <-printerBroker.Call("$node.actions", map[string]interface{}{
					"withEndpoints": false,
					"skipInternal":  false,
					"onlyAvailable": false,
					"onlyLocal":     false,
				})
				assertNodeActions(result, []string{"printer.print", "$node.list", "$node.services", "$node.actions", "$node.events"})

				scannerBroker := createScannerBroker(mem)
				scannerBroker.Start()
				scannerBroker.WaitForNodes("node_printerBroker")
				scannerBroker.WaitFor("printer")
				time.Sleep(time.Millisecond)

				result = <-scannerBroker.Call("$node.actions", map[string]interface{}{
					"withEndpoints": false,
					"skipInternal":  false,
					"onlyAvailable": false,
					"onlyLocal":     false,
				})
				assertNodeActions(result, []string{"printer.print", "scanner.scan", "$node.list", "$node.services", "$node.actions", "$node.events"})

				cpuBroker := createCpuBroker(mem)
				cpuBroker.Start()
				cpuBroker.WaitForNodes("node_printerBroker", "node_scannerBroker")
				cpuBroker.WaitFor("printer", "scanner")
				time.Sleep(time.Millisecond)

				result = <-cpuBroker.Call("$node.actions", map[string]interface{}{
					"withEndpoints": false,
					"skipInternal":  false,
					"onlyAvailable": false,
					"onlyLocal":     false,
				})
				assertNodeActions(result, []string{"printer.print", "scanner.scan", "cpu.compute", "$node.list", "$node.services", "$node.actions", "$node.events"})
			})

			It("$node.actions - all true", func() {
				mem := &memory.SharedMemory{}
				printerBroker := createPrinterBroker(mem)
				printerBroker.Start()

				result := <-printerBroker.Call("$node.actions", map[string]interface{}{
					"withEndpoints": true,
					"skipInternal":  true,
					"onlyAvailable": true,
					"onlyLocal":     true,
				})
				assertNodeActions(result, []string{"printer.print"})

				scannerBroker := createScannerBroker(mem)
				scannerBroker.Start()
				scannerBroker.WaitForNodes("node_printerBroker")
				scannerBroker.WaitFor("printer")
				time.Sleep(time.Millisecond)

				result = <-scannerBroker.Call("$node.actions", map[string]interface{}{
					"withEndpoints": true,
					"skipInternal":  true,
					"onlyAvailable": true,
					"onlyLocal":     true,
				})
				assertNodeActions(result, []string{"scanner.scan"})

				cpuBroker := createCpuBroker(mem)
				cpuBroker.Start()
				cpuBroker.WaitForNodes("node_printerBroker", "node_scannerBroker")
				cpuBroker.WaitFor("printer", "scanner")
				time.Sleep(time.Millisecond)

				result = <-cpuBroker.Call("$node.actions", map[string]interface{}{
					"withEndpoints": true,
					"skipInternal":  true,
					"onlyAvailable": true,
					"onlyLocal":     true,
				})
				assertNodeActions(result, []string{"cpu.compute"})
			})

			It("$node.actions - withEndpoints", func() {
				mem := &memory.SharedMemory{}
				printerBroker := createPrinterBroker(mem)
				printerBroker.Start()

				result := <-printerBroker.Call("$node.actions", map[string]interface{}{
					"withEndpoints": true,
					"skipInternal":  false,
					"onlyAvailable": false,
					"onlyLocal":     false,
				})
				assertNodeActions(result, []string{"printer.print", "$node.list", "$node.services", "$node.actions", "$node.events"})

				scannerBroker := createScannerBroker(mem)
				scannerBroker.Start()
				scannerBroker.WaitForNodes("node_printerBroker")
				scannerBroker.WaitFor("printer")
				time.Sleep(time.Millisecond)

				result = <-scannerBroker.Call("$node.actions", map[string]interface{}{
					"withEndpoints": true,
					"skipInternal":  false,
					"onlyAvailable": false,
					"onlyLocal":     false,
				})
				assertNodeActions(result, []string{"printer.print", "scanner.scan", "$node.list", "$node.services", "$node.actions", "$node.events"})

				cpuBroker := createCpuBroker(mem)
				cpuBroker.Start()
				cpuBroker.WaitForNodes("node_printerBroker", "node_scannerBroker")
				cpuBroker.WaitFor("printer", "scanner")
				time.Sleep(time.Millisecond)

				result = <-cpuBroker.Call("$node.actions", map[string]interface{}{
					"withEndpoints": true,
					"skipInternal":  false,
					"onlyAvailable": false,
					"onlyLocal":     false,
				})
				assertNodeActions(result, []string{"printer.print", "scanner.scan", "cpu.compute", "$node.list", "$node.services", "$node.actions", "$node.events"})
			})

			It("$node.actions - skipInternal", func() {
				mem := &memory.SharedMemory{}
				printerBroker := createPrinterBroker(mem)
				printerBroker.Start()

				result := <-printerBroker.Call("$node.actions", map[string]interface{}{
					"withEndpoints": false,
					"skipInternal":  true,
					"onlyAvailable": false,
					"onlyLocal":     false,
				})
				assertNodeActions(result, []string{"printer.print"})

				scannerBroker := createScannerBroker(mem)
				scannerBroker.Start()
				scannerBroker.WaitForNodes("node_printerBroker")
				scannerBroker.WaitFor("printer")
				time.Sleep(time.Millisecond)

				result = <-scannerBroker.Call("$node.actions", map[string]interface{}{
					"withEndpoints": false,
					"skipInternal":  true,
					"onlyAvailable": false,
					"onlyLocal":     false,
				})
				assertNodeActions(result, []string{"printer.print", "scanner.scan"})

				cpuBroker := createCpuBroker(mem)
				cpuBroker.Start()
				cpuBroker.WaitForNodes("node_printerBroker", "node_scannerBroker")
				cpuBroker.WaitFor("printer", "scanner")
				time.Sleep(time.Millisecond)

				result = <-cpuBroker.Call("$node.actions", map[string]interface{}{
					"withEndpoints": false,
					"skipInternal":  true,
					"onlyAvailable": false,
					"onlyLocal":     false,
				})
				assertNodeActions(result, []string{"printer.print", "scanner.scan", "cpu.compute"})
			})

			It("$node.actions - onlyAvailable", func() {
				mem := &memory.SharedMemory{}
				printerBroker := createPrinterBroker(mem)
				printerBroker.Start()

				result := <-printerBroker.Call("$node.actions", map[string]interface{}{
					"withEndpoints": false,
					"skipInternal":  false,
					"onlyAvailable": true,
					"onlyLocal":     false,
				})
				assertNodeActions(result, []string{"printer.print", "$node.list", "$node.services", "$node.actions", "$node.events"})

				scannerBroker := createScannerBroker(mem)
				scannerBroker.Start()
				scannerBroker.WaitForNodes("node_printerBroker")
				scannerBroker.WaitFor("printer")
				time.Sleep(time.Millisecond)

				result = <-scannerBroker.Call("$node.actions", map[string]interface{}{
					"withEndpoints": false,
					"skipInternal":  false,
					"onlyAvailable": true,
					"onlyLocal":     false,
				})
				assertNodeActions(result, []string{"printer.print", "scanner.scan", "$node.list", "$node.services", "$node.actions", "$node.events"})

				cpuBroker := createCpuBroker(mem)
				cpuBroker.Start()
				cpuBroker.WaitForNodes("node_printerBroker", "node_scannerBroker")
				cpuBroker.WaitFor("printer", "scanner")
				time.Sleep(time.Millisecond)

				result = <-cpuBroker.Call("$node.actions", map[string]interface{}{
					"withEndpoints": false,
					"skipInternal":  false,
					"onlyAvailable": true,
					"onlyLocal":     false,
				})
				assertNodeActions(result, []string{"printer.print", "scanner.scan", "cpu.compute", "$node.list", "$node.services", "$node.actions", "$node.events"})
			})

			It("$node.actions - onlyLocal", func() {
				mem := &memory.SharedMemory{}
				printerBroker := createPrinterBroker(mem)
				printerBroker.Start()

				result := <-printerBroker.Call("$node.actions", map[string]interface{}{
					"withEndpoints": false,
					"skipInternal":  false,
					"onlyAvailable": false,
					"onlyLocal":     true,
				})
				assertNodeActions(result, []string{"printer.print"})

				scannerBroker := createScannerBroker(mem)
				scannerBroker.Start()
				scannerBroker.WaitForNodes("node_printerBroker")
				scannerBroker.WaitFor("printer")
				time.Sleep(time.Millisecond)

				result = <-scannerBroker.Call("$node.actions", map[string]interface{}{
					"withEndpoints": false,
					"skipInternal":  false,
					"onlyAvailable": false,
					"onlyLocal":     true,
				})
				assertNodeActions(result, []string{"scanner.scan"})

				cpuBroker := createCpuBroker(mem)
				cpuBroker.Start()
				cpuBroker.WaitForNodes("node_printerBroker", "node_scannerBroker")
				cpuBroker.WaitFor("printer", "scanner")
				time.Sleep(time.Millisecond)

				result = <-cpuBroker.Call("$node.actions", map[string]interface{}{
					"withEndpoints": false,
					"skipInternal":  false,
					"onlyAvailable": false,
					"onlyLocal":     true,
				})
				assertNodeActions(result, []string{"cpu.compute"})
			})
			It("$node.list with no services", func() {
				mem := &memory.SharedMemory{}
				printerBroker := createPrinterBroker(mem)
				printerBroker.Start()

				result := <-printerBroker.Call("$node.list", map[string]interface{}{
					"withServices":  false,
					"onlyAvailable": false,
				})
				assertNodeList(result, 1)

				scannerBroker := createScannerBroker(mem)
				scannerBroker.Start()
				scannerBroker.WaitForNodes("node_printerBroker")
				scannerBroker.WaitFor("printer")
				time.Sleep(time.Millisecond)

				result = <-scannerBroker.Call("$node.list", map[string]interface{}{
					"withServices":  false,
					"onlyAvailable": false,
				})
				assertNodeList(result, 2)

				cpuBroker := createCpuBroker(mem)
				cpuBroker.Start()
				cpuBroker.WaitForNodes("node_printerBroker", "node_scannerBroker")
				cpuBroker.WaitFor("printer", "scanner")
				time.Sleep(time.Millisecond)

				result = <-cpuBroker.Call("$node.list", map[string]interface{}{
					"withServices":  false,
					"onlyAvailable": false,
				})
				assertNodeList(result, 3)
			})

			It("$node.list with services", func() {
				mem := &memory.SharedMemory{}
				printerBroker := createPrinterBroker(mem)
				printerBroker.Start()

				result := <-printerBroker.Call("$node.list", map[string]interface{}{
					"withServices":  true,
					"onlyAvailable": false,
				})
				assertNodeList(result, 1)

				scannerBroker := createScannerBroker(mem)
				scannerBroker.Start()
				scannerBroker.WaitForNodes("node_printerBroker")
				scannerBroker.WaitFor("printer")
				time.Sleep(time.Millisecond)

				result = <-scannerBroker.Call("$node.list", map[string]interface{}{
					"withServices":  true,
					"onlyAvailable": false,
				})
				assertNodeList(result, 2)

				cpuBroker := createCpuBroker(mem)
				cpuBroker.Start()
				cpuBroker.WaitForNodes("node_printerBroker", "node_scannerBroker")
				cpuBroker.WaitFor("printer", "scanner")
				time.Sleep(time.Millisecond)

				result = <-cpuBroker.Call("$node.list", map[string]interface{}{
					"withServices":  true,
					"onlyAvailable": false,
				})
				assertNodeList(result, 3)
			})

			It("$node.services - all false", func() {
				mem := &memory.SharedMemory{}
				printerBroker := createPrinterBroker(mem)
				printerBroker.Start()

				result := <-printerBroker.Call("$node.services", map[string]interface{}{
					"withEndpoints": false,
					"withActions":   false,
					"withEvents":    false,
					"skipInternal":  false,
					"onlyAvailable": false,
					"onlyLocal":     false,
				})
				assertNodeServices(result, []string{"printer", "$node"})

				scannerBroker := createScannerBroker(mem)
				scannerBroker.Start()
				scannerBroker.WaitForNodes("node_printerBroker")
				scannerBroker.WaitFor("printer")
				time.Sleep(time.Millisecond)

				result = <-scannerBroker.Call("$node.services", map[string]interface{}{
					"withEndpoints": false,
					"withActions":   false,
					"withEvents":    false,
					"skipInternal":  false,
					"onlyAvailable": false,
					"onlyLocal":     false,
				})
				assertNodeServices(result, []string{"printer", "scanner", "$node"})

				cpuBroker := createCpuBroker(mem)
				cpuBroker.Start()
				cpuBroker.WaitForNodes("node_printerBroker", "node_scannerBroker")
				cpuBroker.WaitFor("printer", "scanner")
				time.Sleep(time.Millisecond)

				result = <-cpuBroker.Call("$node.services", map[string]interface{}{
					"withEndpoints": false,
					"withActions":   false,
					"withEvents":    false,
					"skipInternal":  false,
					"onlyAvailable": false,
					"onlyLocal":     false,
				})
				assertNodeServices(result, []string{"printer", "scanner", "cpu", "$node"})
			})

			It("$node.services - all true", func() {
				mem := &memory.SharedMemory{}
				printerBroker := createPrinterBroker(mem)
				printerBroker.Start()

				result := <-printerBroker.Call("$node.services", map[string]interface{}{
					"withEndpoints": true,
					"withActions":   true,
					"withEvents":    true,
					"skipInternal":  true,
					"onlyAvailable": true,
					"onlyLocal":     true,
				})
				assertNodeServices(result, []string{"printer"})

				scannerBroker := createScannerBroker(mem)
				scannerBroker.Start()
				scannerBroker.WaitForNodes("node_printerBroker")
				scannerBroker.WaitFor("printer")
				time.Sleep(time.Millisecond)

				result = <-scannerBroker.Call("$node.services", map[string]interface{}{
					"withEndpoints": true,
					"withActions":   true,
					"withEvents":    true,
					"skipInternal":  true,
					"onlyAvailable": true,
					"onlyLocal":     true,
				})
				assertNodeServices(result, []string{"scanner"})

				cpuBroker := createCpuBroker(mem)
				cpuBroker.Start()
				cpuBroker.WaitForNodes("node_printerBroker", "node_scannerBroker")
				cpuBroker.WaitFor("printer", "scanner")
				time.Sleep(time.Millisecond)

				result = <-cpuBroker.Call("$node.services", map[string]interface{}{
					"withEndpoints": true,
					"withActions":   true,
					"withEvents":    true,
					"skipInternal":  true,
					"onlyAvailable": true,
					"onlyLocal":     true,
				})
				assertNodeServices(result, []string{"printer", "cpu"})
			})

			It("$node.services - withActions", func() {
				mem := &memory.SharedMemory{}
				printerBroker := createPrinterBroker(mem)
				printerBroker.Start()

				result := <-printerBroker.Call("$node.services", map[string]interface{}{
					"withActions":   true,
					"withEndpoints": false,
					"withEvents":    false,
					"skipInternal":  false,
					"onlyAvailable": false,
					"onlyLocal":     false,
				})
				assertNodeServices(result, []string{"printer", "$node"})

				scannerBroker := createScannerBroker(mem)
				scannerBroker.Start()
				scannerBroker.WaitForNodes("node_printerBroker")
				scannerBroker.WaitFor("printer")
				time.Sleep(time.Millisecond)

				result = <-scannerBroker.Call("$node.services", map[string]interface{}{
					"withActions":   true,
					"withEndpoints": false,
					"withEvents":    false,
					"skipInternal":  false,
					"onlyAvailable": false,
					"onlyLocal":     false,
				})
				assertNodeServices(result, []string{"printer", "scanner", "$node"})

				cpuBroker := createCpuBroker(mem)
				cpuBroker.Start()
				cpuBroker.WaitForNodes("node_printerBroker", "node_scannerBroker")
				cpuBroker.WaitFor("printer", "scanner")
				time.Sleep(time.Millisecond)

				result = <-cpuBroker.Call("$node.services", map[string]interface{}{
					"withActions":   true,
					"withEndpoints": false,
					"withEvents":    false,
					"skipInternal":  false,
					"onlyAvailable": false,
					"onlyLocal":     false,
				})
				assertNodeServices(result, []string{"printer", "scanner", "cpu", "$node"})
			})

			It("$node.services - withEndpoints", func() {
				mem := &memory.SharedMemory{}
				printerBroker := createPrinterBroker(mem)
				printerBroker.Start()

				result := <-printerBroker.Call("$node.services", map[string]interface{}{
					"withActions":   false,
					"withEndpoints": true,
					"withEvents":    false,
					"skipInternal":  false,
					"onlyAvailable": false,
					"onlyLocal":     false,
				})
				assertNodeServices(result, []string{"printer", "$node"})

				scannerBroker := createScannerBroker(mem)
				scannerBroker.Start()
				scannerBroker.WaitForNodes("node_printerBroker")
				scannerBroker.WaitFor("printer")
				time.Sleep(time.Millisecond)

				result = <-scannerBroker.Call("$node.services", map[string]interface{}{
					"withActions":   false,
					"withEndpoints": true,
					"withEvents":    false,
					"skipInternal":  false,
					"onlyAvailable": false,
					"onlyLocal":     false,
				})
				assertNodeServices(result, []string{"printer", "scanner", "$node"})

				cpuBroker := createCpuBroker(mem)
				cpuBroker.Start()
				cpuBroker.WaitForNodes("node_printerBroker", "node_scannerBroker")
				cpuBroker.WaitFor("printer", "scanner")
				time.Sleep(time.Millisecond)

				result = <-cpuBroker.Call("$node.services", map[string]interface{}{
					"withActions":   false,
					"withEndpoints": true,
					"withEvents":    false,
					"skipInternal":  false,
					"onlyAvailable": false,
					"onlyLocal":     false,
				})
				assertNodeServices(result, []string{"printer", "scanner", "cpu", "$node"})
			})

			It("$node.services - withEvents", func() {
				mem := &memory.SharedMemory{}
				printerBroker := createPrinterBroker(mem)
				printerBroker.Start()

				result := <-printerBroker.Call("$node.services", map[string]interface{}{
					"withActions":   false,
					"withEndpoints": false,
					"withEvents":    true,
					"skipInternal":  false,
					"onlyAvailable": false,
					"onlyLocal":     false,
				})
				assertNodeServices(result, []string{"printer", "$node"})

				scannerBroker := createScannerBroker(mem)
				scannerBroker.Start()
				scannerBroker.WaitForNodes("node_printerBroker")
				scannerBroker.WaitFor("printer")
				time.Sleep(time.Millisecond)

				result = <-scannerBroker.Call("$node.services", map[string]interface{}{
					"withActions":   false,
					"withEndpoints": false,
					"withEvents":    true,
					"skipInternal":  false,
					"onlyAvailable": false,
					"onlyLocal":     false,
				})
				assertNodeServices(result, []string{"printer", "scanner", "$node"})

				cpuBroker := createCpuBroker(mem)
				cpuBroker.Start()
				cpuBroker.WaitForNodes("node_printerBroker", "node_scannerBroker")
				cpuBroker.WaitFor("printer", "scanner")
				time.Sleep(time.Millisecond)

				result = <-cpuBroker.Call("$node.services", map[string]interface{}{
					"withActions":   false,
					"withEndpoints": false,
					"withEvents":    true,
					"skipInternal":  false,
					"onlyAvailable": false,
					"onlyLocal":     false,
				})
				assertNodeServices(result, []string{"printer", "scanner", "cpu", "$node"})
			})

			It("$node.services - skipInternal", func() {
				mem := &memory.SharedMemory{}
				printerBroker := createPrinterBroker(mem)
				printerBroker.Start()

				result := <-printerBroker.Call("$node.services", map[string]interface{}{
					"withActions":   false,
					"withEndpoints": false,
					"withEvents":    false,
					"skipInternal":  true,
					"onlyAvailable": false,
					"onlyLocal":     false,
				})
				assertNodeServices(result, []string{"printer"})

				scannerBroker := createScannerBroker(mem)
				scannerBroker.Start()
				scannerBroker.WaitForNodes("node_printerBroker")
				scannerBroker.WaitFor("printer")
				time.Sleep(time.Millisecond)

				result = <-scannerBroker.Call("$node.services", map[string]interface{}{
					"withActions":   false,
					"withEndpoints": false,
					"withEvents":    false,
					"skipInternal":  true,
					"onlyAvailable": false,
					"onlyLocal":     false,
				})
				assertNodeServices(result, []string{"printer", "scanner"})

				cpuBroker := createCpuBroker(mem)
				cpuBroker.Start()
				cpuBroker.WaitForNodes("node_printerBroker", "node_scannerBroker")
				cpuBroker.WaitFor("printer", "scanner")
				time.Sleep(time.Millisecond)

				result = <-cpuBroker.Call("$node.services", map[string]interface{}{
					"withActions":   false,
					"withEndpoints": false,
					"withEvents":    false,
					"skipInternal":  true,
					"onlyAvailable": false,
					"onlyLocal":     false,
				})
				assertNodeServices(result, []string{"printer", "scanner", "cpu"})
			})

			It("$node.services - onlyAvailable", func() {
				mem := &memory.SharedMemory{}
				printerBroker := createPrinterBroker(mem)
				printerBroker.Start()

				result := <-printerBroker.Call("$node.services", map[string]interface{}{
					"withActions":   false,
					"withEndpoints": false,
					"withEvents":    false,
					"skipInternal":  false,
					"onlyAvailable": true,
					"onlyLocal":     false,
				})
				assertNodeServices(result, []string{"printer", "$node"})

				scannerBroker := createScannerBroker(mem)
				scannerBroker.Start()
				scannerBroker.WaitForNodes("node_printerBroker")
				scannerBroker.WaitFor("printer")
				time.Sleep(time.Millisecond)

				result = <-scannerBroker.Call("$node.services", map[string]interface{}{
					"withActions":   false,
					"withEndpoints": false,
					"withEvents":    false,
					"skipInternal":  false,
					"onlyAvailable": true,
					"onlyLocal":     false,
				})
				assertNodeServices(result, []string{"printer", "scanner", "$node"})

				cpuBroker := createCpuBroker(mem)
				cpuBroker.Start()
				cpuBroker.WaitForNodes("node_printerBroker", "node_scannerBroker")
				cpuBroker.WaitFor("printer", "scanner")
				time.Sleep(time.Millisecond)

				result = <-cpuBroker.Call("$node.services", map[string]interface{}{
					"withActions":   false,
					"withEndpoints": false,
					"withEvents":    false,
					"skipInternal":  false,
					"onlyAvailable": true,
					"onlyLocal":     false,
				})
				assertNodeServices(result, []string{"printer", "scanner", "cpu", "$node"})
			})

			It("$node.services - onlyLocal", func() {
				mem := &memory.SharedMemory{}
				printerBroker := createPrinterBroker(mem)
				printerBroker.Start()

				result := <-printerBroker.Call("$node.services", map[string]interface{}{
					"withActions":   false,
					"withEndpoints": false,
					"withEvents":    false,
					"skipInternal":  false,
					"onlyAvailable": false,
					"onlyLocal":     true,
				})
				assertNodeServices(result, []string{"printer"})

				scannerBroker := createScannerBroker(mem)
				scannerBroker.Start()
				scannerBroker.WaitForNodes("node_printerBroker")
				scannerBroker.WaitFor("printer")
				time.Sleep(time.Millisecond)

				result = <-scannerBroker.Call("$node.services", map[string]interface{}{
					"withActions":   false,
					"withEndpoints": false,
					"withEvents":    false,
					"skipInternal":  false,
					"onlyAvailable": false,
					"onlyLocal":     true,
				})
				assertNodeServices(result, []string{"scanner"})

				cpuBroker := createCpuBroker(mem)
				cpuBroker.Start()
				cpuBroker.WaitForNodes("node_printerBroker", "node_scannerBroker")
				cpuBroker.WaitFor("printer", "scanner")
				time.Sleep(time.Millisecond)

				result = <-cpuBroker.Call("$node.services", map[string]interface{}{
					"withActions":   false,
					"withEndpoints": false,
					"withEvents":    false,
					"skipInternal":  false,
					"onlyAvailable": false,
					"onlyLocal":     true,
				})
				assertNodeServices(result, []string{"cpu"})
			})
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
