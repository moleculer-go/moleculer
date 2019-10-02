package cli

import (
	. "github.com/onsi/ginkgo"
)

var _ = Describe("Cli", func() {

	Describe("Loading Configuration", func() {
		It("shuold load config file and pass it on to the broker", func() {
			// Start(
			// 	&moleculer.Config{},
			// 	func(broker *broker.ServiceBroker, cmd *cobra.Command) {
			// 		broker.Publish(moleculer.ServiceSchema{
			// 			Name: "user",
			// 			Started: func(c moleculer.BrokerContext, svc moleculer.ServiceSchema) {
			// 				Expect(svc.Settings["table"]).Should(Equal("userTable"))
			// 				Expect(svc.Settings["idField"]).Should(Equal("id"))
			// 				close(done)
			// 			},
			// 		})
			// 	})
		})
	})

})
