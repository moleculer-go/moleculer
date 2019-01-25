package transit_test

import (
	test "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	///. "github.com/moleculer-go/moleculer/common"
	. "github.com/moleculer-go/moleculer/context"
	. "github.com/moleculer-go/moleculer/serializer"
	. "github.com/moleculer-go/moleculer/transit"
)

var _ = test.Describe("Transit", func() {

	context := CreateBrokerContext(nil, nil, nil, CreateLogger, nil)
	var serializer Serializer = CreateJSONSerializer()
	url := "stan://localhost:4222"
	options := StanTransporterOptions{
		"MOL",
		url,
		"test-cluster",
		"unit-test-client-id",
		&serializer,
	}

	test.It("Should connect, subscribe, publish and disconnect", func() {
		params := map[string]string{
			"name":     "John",
			"lastName": "Snow",
		}

		actionName := "some.service.action"
		actionContext := context.NewActionContext(actionName, params)

		transporter := CreateStanTransporter(options)
		<-transporter.Connect()

		received := make(chan bool)
		transporter.Subscribe("topicA", "node1", func(message TransitMessage) {

			context := serializer.MessageToContext(message)
			Expect(context.GetActionName()).Should(Equal(actionName))
			contextParams := context.GetParams()
			Expect(contextParams.String("name")).Should(Equal("John"))
			Expect(contextParams.String("lastName")).Should(Equal("Snow"))

			received <- true
		})

		transporter.Publish("topicA", "node1", serializer.ContextToMessage(actionContext))

		Expect(<-received).Should(Equal(true))

		<-transporter.Disconnect()

	})

})
