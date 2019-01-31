package nats_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/moleculer-go/moleculer/context"
	"github.com/moleculer-go/moleculer/serializer"
	"github.com/moleculer-go/moleculer/transit"
	"github.com/moleculer-go/moleculer/transit/nats"
)

var _ = Describe("Transit", func() {

	brokerDelegates := BrokerDelegates()
	contextA := context.BrokerContext(brokerDelegates)
	logger := contextA.Logger()
	var serializer serializer.Serializer = serializer.CreateJSONSerializer(logger)
	url := "stan://localhost:4222"
	options := nats.StanOptions{
		"MOL",
		url,
		"test-cluster",
		"unit-test-client-id",
		logger,
		serializer,
		func(msg transit.Message) bool {
			return true
		},
	}

	It("Should connect, subscribe, publish and disconnect", func() {
		params := map[string]string{
			"name":     "John",
			"lastName": "Snow",
		}

		actionName := "some.service.action"
		actionContext := contextA.NewActionContext(actionName, params)

		transporter := nats.CreateStanTransporter(options)
		<-transporter.Connect()

		received := make(chan bool)
		transporter.Subscribe("topicA", "node1", func(message transit.Message) {

			contextMap := serializer.MessageToContextMap(message)
			newContext := context.RemoteActionContext(brokerDelegates, contextMap)
			Expect(newContext.ActionName()).Should(Equal(actionName))
			contextParams := newContext.Params()
			Expect(contextParams.String("name")).Should(Equal("John"))
			Expect(contextParams.String("lastName")).Should(Equal("Snow"))

			received <- true
		})

		contextMap := actionContext.AsMap()
		contextMap["sender"] = "someone"
		msg, _ := serializer.MapToMessage(&contextMap)
		transporter.Publish("topicA", "node1", msg)

		Expect(<-received).Should(Equal(true))

		<-transporter.Disconnect()

	})

})
