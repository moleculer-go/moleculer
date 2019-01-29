package registry_test

import (
	"github.com/moleculer-go/moleculer"
	//. "github.com/moleculer-go/moleculer/registry"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func createBrokerA() moleculer.ServiceBroker {
	broker := moleculer.BrokerFromConfig(&moleculer.BrokerConfig{
		DiscoverNodeID: func() string { return "node_brokerA" },
	})

	broker.AddService(moleculer.Service{
		Name: "printer",
		Actions: []moleculer.Action{
			{
				Name: "print",
				Handler: func(context moleculer.Context, params moleculer.Params) interface{} {
					context.GetLogger().Info("print action invoked.")
					return params.Value()
				},
			},
		},
	})

	return (*broker)
}

func createBrokerB() moleculer.ServiceBroker {
	broker := moleculer.BrokerFromConfig(&moleculer.BrokerConfig{
		DiscoverNodeID: func() string { return "node_brokerB" },
	})
	broker.AddService(moleculer.Service{
		Name: "scanner",
		Actions: []moleculer.Action{
			{
				Name: "scan",
				Handler: func(context moleculer.Context, params moleculer.Params) interface{} {
					context.GetLogger().Info("scan action invoked.")
					return params.Value()
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
			Expect((*brokerA.GetInfo().GetLocalNode()).GetID()).Should(Equal("node_brokerA"))

			brokerB := createBrokerB()
			Expect((*brokerB.GetInfo().GetLocalNode()).GetID()).Should(Equal("node_brokerB"))

			brokerA.Start()

			printText := "TEXT TO PRINT"
			printResult := <-brokerA.Call("printer.print", printText)
			Expect(printResult).Should(Equal(printText))

			scanText := "TEXT TO SCAN"
			Expect(func() {
				<-brokerA.Call("scanner.scan", scanText)
			}).Should(Panic())

			brokerB.Start()

			scanResult := <-brokerA.Call("scanner.scan", scanText)
			Expect(scanResult).Should(Equal(scanText))
		})

	})

})
