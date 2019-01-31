package registry_test

import (
	"fmt"
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var logLevel = "DEBUG"

func createBrokerA() broker.ServiceBroker {
	broker := broker.FromConfig(&moleculer.BrokerConfig{
		DiscoverNodeID: func() string { return "node_brokerA" },
		LogLevel:       logLevel,
		Transporter:    "STAN",
	})

	broker.AddService(moleculer.Service{
		Name: "printer",
		Actions: []moleculer.Action{
			{
				Name: "print",
				Handler: func(context moleculer.Context, params moleculer.Params) interface{} {
					context.Logger().Info("print action invoked.")
					return params.Value()
				},
			},
		},
	})

	return (*broker)
}

func createBrokerB() broker.ServiceBroker {
	broker := broker.FromConfig(&moleculer.BrokerConfig{
		DiscoverNodeID: func() string { return "node_brokerB" },
		LogLevel:       logLevel,
		Transporter:    "STAN",
	})
	broker.AddService(moleculer.Service{
		Name: "scanner",
		Actions: []moleculer.Action{
			{
				Name: "scan",
				Handler: func(context moleculer.Context, params moleculer.Params) interface{} {
					context.Logger().Info("scan action invoked!")

					return params.Value()
				},
			},
		},
	})

	return (*broker)
}

func createBrokerC() broker.ServiceBroker {
	broker := broker.FromConfig(&moleculer.BrokerConfig{
		DiscoverNodeID: func() string { return "node_brokerC" },
		LogLevel:       logLevel,
		Transporter:    "STAN",
	})
	broker.AddService(moleculer.Service{
		Name: "cpu",
		Actions: []moleculer.Action{
			{
				Name: "compute",
				Handler: func(context moleculer.Context, params moleculer.Params) interface{} {
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
		//broker := CreateBroker()

		It("Should call action from brokerA to brokerB and retun results", func() {

			brokerA := createBrokerA()
			Expect(brokerA.LocalNode().GetID()).Should(Equal("node_brokerA"))

			brokerB := createBrokerB()
			Expect(brokerB.LocalNode().GetID()).Should(Equal("node_brokerB"))

			brokerC := createBrokerC()
			Expect(brokerC.LocalNode().GetID()).Should(Equal("node_brokerC"))

			brokerA.Start()

			printText := "TEXT TO PRINT"
			printResult := <-brokerA.Call("printer.print", printText)
			Expect(printResult).Should(Equal(printText))

			scanText := "TEXT TO SCAN"
			Expect(func() {
				<-brokerA.Call("scanner.scan", scanText)
			}).Should(Panic()) //broker B is not started yet.. so should panic

			brokerB.Start()
			time.Sleep(time.Second)

			fmt.Println("***** Step X")

			scanResult := <-brokerA.Call("scanner.scan", scanText)
			Expect(scanResult).Should(Equal(scanText))

			fmt.Println("***** Step Y")

			brokerC.Start()
			time.Sleep(time.Second) //sleep until services are registered

			fmt.Println("***** Step Z")

			contentToCompute := "Some long long text ..."
			computeResult := <-brokerC.Call("cpu.compute", contentToCompute)
			Expect(computeResult).Should(Equal(contentToCompute))

			//stopping broker B
			// brokerB.Stop() // TODO -> not  implemented yet
			// time.Sleep(2 * time.Second)

			// Expect(func() {
			// 	<-brokerA.Call("scanner.scan", scanText)
			// }).Should(Panic()) //broker B is stoped ... so it should panic

		})

	})

})
