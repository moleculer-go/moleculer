package registry_test

import (
	"time"

	snap "github.com/moleculer-go/cupaloy"
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
	"github.com/moleculer-go/moleculer/transit/memory"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
)

var logLevel = "DEBUG"

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

func findById(id string, list []moleculer.Payload) map[string]interface{} {
	for _, item := range list {
		if item.Get("id").String() == id {
			iMap := item.RawMap()
			iMap["ipList"] = item.Get("ipList").StringArray()
			iMap["seq"] = item.Get("seq").Int64()
			iMap["cpu"] = item.Get("cpu").Int64()
			iMap["cpuSeq"] = item.Get("cpuSeq").Int64()
			return iMap
		}
	}
	return nil
}

var _ = Describe("Registry", func() {

	Describe("Local services", func() {
		It("Should expose the $node.list local service action", func() {

			mem := &memory.SharedMemory{}

			printerBroker := createPrinterBroker(mem)
			printerBroker.Start()

			result := <-printerBroker.Call("$node.list", nil)
			nodePrinterBroker := findById("node_printerBroker", result.Array())

			Expect(snap.SnapshotMulti("1", nodePrinterBroker)).Should(Succeed())

			scannerBroker := createScannerBroker(mem)
			scannerBroker.Start()
			time.Sleep(100 * time.Millisecond)

			result = <-scannerBroker.Call("$node.list", nil)
			list := result.Array()
			Expect(len(list)).Should(Equal(2))

			nodeScannerBroker := findById("node_scannerBroker", list)
			nodePrinterBroker = findById("node_printerBroker", list)

			Expect(snap.SnapshotMulti("2.1", nodeScannerBroker)).Should(Succeed())
			Expect(snap.SnapshotMulti("2.2", nodePrinterBroker)).Should(Succeed())

			cpuBroker := createCpuBroker(mem)
			cpuBroker.Start()
			time.Sleep(100 * time.Millisecond)

			result = <-cpuBroker.Call("$node.list", nil)
			list = result.Array()
			Expect(len(list)).Should(Equal(3))
			nodeScannerBroker = findById("node_scannerBroker", list)
			nodePrinterBroker = findById("node_printerBroker", list)
			nodeCpuBroker := findById("node_cpuBroker", list)

			Expect(snap.SnapshotMulti("3.1", nodeScannerBroker)).Should(Succeed())
			Expect(snap.SnapshotMulti("3.2", nodePrinterBroker)).Should(Succeed())
			Expect(snap.SnapshotMulti("3.3", nodeCpuBroker)).Should(Succeed())
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
