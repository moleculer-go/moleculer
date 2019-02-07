package registry_test

import (
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var logLevel = "DEBUG"

func createPrinterBroker() broker.ServiceBroker {
	broker := broker.FromConfig(&moleculer.BrokerConfig{
		DiscoverNodeID: func() string { return "node_printerBroker" },
		LogLevel:       logLevel,
	})

	broker.AddService(moleculer.Service{
		Name: "printer",
		Actions: []moleculer.Action{
			{
				Name: "print",
				Handler: func(context moleculer.Context, params moleculer.Payload) interface{} {
					context.Logger().Info("print action invoked.")
					return params.Value()
				},
			},
		},
	})

	return (*broker)
}

func createScannerBroker() broker.ServiceBroker {
	broker := broker.FromConfig(&moleculer.BrokerConfig{
		DiscoverNodeID: func() string { return "node_scannerBroker" },
		LogLevel:       logLevel,
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

func createCpuBroker() broker.ServiceBroker {
	broker := broker.FromConfig(&moleculer.BrokerConfig{
		DiscoverNodeID: func() string { return "node_cpuBroker" },
		LogLevel:       logLevel,
	})
	broker.AddService(moleculer.Service{
		Name: "cpu",
		Actions: []moleculer.Action{
			{
				Name: "compute",
				Handler: func(context moleculer.Context, params moleculer.Payload) interface{} {
					context.Logger().Info("compute action invoked!")

					scanResult := <-context.Call("scanner.scan", params.Value())

					return <-context.Call("printer.print", scanResult)
				},
			},
		},
	})

	return (*broker)
}

var _ = Describe("Registry", func() {

	Describe("Heartbeat", func() {

		It("Should call action from printerBroker to scannerBroker and retun results", func() {

			printerBroker := createPrinterBroker()
			Expect(printerBroker.LocalNode().GetID()).Should(Equal("node_printerBroker"))

			scannerBroker := createScannerBroker()
			Expect(scannerBroker.LocalNode().GetID()).Should(Equal("node_scannerBroker"))

			cpuBroker := createCpuBroker()
			Expect(cpuBroker.LocalNode().GetID()).Should(Equal("node_cpuBroker"))

			printerBroker.Start()

			printText := "TEXT TO PRINT"
			printResult := <-printerBroker.Call("printer.print", printText)
			Expect(printResult).Should(Equal(printText))

			scanText := "TEXT TO SCAN"
			Expect(func() {
				<-printerBroker.Call("scanner.scan", scanText)
			}).Should(Panic()) //broker B is not started yet.. so should panic

			scannerBroker.Start()
			time.Sleep(time.Second)

			scanResult := <-printerBroker.Call("scanner.scan", scanText)
			Expect(scanResult).Should(Equal(scanText))

			cpuBroker.Start()
			time.Sleep(time.Second) //sleep until services are registered

			contentToCompute := "Some long long text ..."
			computeResult := <-cpuBroker.Call("cpu.compute", contentToCompute)
			Expect(computeResult).Should(Equal(contentToCompute))

			//stopping broker B
			scannerBroker.Stop() // TODO -> not  implemented yet
			time.Sleep(time.Second)

			Expect(func() {
				<-scannerBroker.Call("scanner.scan", scanText)
			}).Should(Panic()) //broker B is stoped ... so it should panic

		})

	})

})
