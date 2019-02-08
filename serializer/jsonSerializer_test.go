package serializer_test

import (
	"github.com/moleculer-go/moleculer/context"
	"github.com/moleculer-go/moleculer/payload"
	"github.com/moleculer-go/moleculer/serializer"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	log "github.com/sirupsen/logrus"
)

var _ = Describe("JSON Serializer", func() {

	brokerDelegates := BrokerDelegates("test-node")
	contextA := context.BrokerContext(brokerDelegates)

	It("Should convert between context and Transit Message", func() {
		logger := log.WithField("serializer", "JSON")
		serializer := serializer.CreateJSONSerializer(logger)

		actionName := "some.service.action"
		params := map[string]string{
			"name":     "John",
			"lastName": "Snow",
		}
		actionContext := contextA.NewActionContext(actionName, payload.Create(params))

		contextMap := actionContext.AsMap()
		contextMap["sender"] = "original_sender"
		message, _ := serializer.MapToMessage(&contextMap)

		Expect(message.Get("action").String()).Should(Equal(actionName))
		Expect(message.Get("params.name").String()).Should(Equal("John"))
		Expect(message.Get("params.lastName").String()).Should(Equal("Snow"))

		values := serializer.MessageToContextMap(message)
		contextAgain := context.RemoteActionContext(brokerDelegates, values)

		Expect(contextAgain.TargetNodeID()).Should(Equal("original_sender"))
		Expect(contextAgain.ActionName()).Should(Equal(actionName))
		Expect(contextAgain.Payload().Get("name").String()).Should(Equal("John"))
		Expect(contextAgain.Payload().Get("lastName").String()).Should(Equal("Snow"))

	})
})
