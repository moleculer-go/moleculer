package serializer_test

import (
	test "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/moleculer-go/moleculer/common"
	. "github.com/moleculer-go/moleculer/context"
	. "github.com/moleculer-go/moleculer/registry"
	. "github.com/moleculer-go/moleculer/serializer"
	log "github.com/sirupsen/logrus"
)

var _ = test.Describe("JSON Serializer", func() {

	context := CreateBrokerContext(nil, nil, nil, CreateLogger, "unit-test")

	test.It("Should convert between context and Transit Message", func() {
		logger := log.WithField("serializer", "JSON")
		serializer := CreateJSONSerializer(logger)

		broker := &BrokerInfo{
			GetDelegates: func() (ActionDelegateFunc, EventDelegateFunc, EventDelegateFunc) {
				return nil, nil, nil
			},
			GetLocalNode: func() *Node {
				node := CreateNode("local-node")
				return &node
			},
		}

		actionName := "some.service.action"
		params := map[string]string{
			"name":     "John",
			"lastName": "Snow",
		}
		actionContext := context.NewActionContext(actionName, params)

		contextMap := actionContext.AsMap()
		contextMap["sender"] = "original_sender"
		message, _ := serializer.MapToMessage(&contextMap)

		Expect(message.Get("action").String()).Should(Equal(actionName))
		Expect(message.Get("params.name").String()).Should(Equal("John"))
		Expect(message.Get("params.lastName").String()).Should(Equal("Snow"))

		values := serializer.MessageToContextMap(&message)
		contextAgain := CreateContext(broker, values)

		Expect(contextAgain.GetTargetNodeID()).Should(Equal("original_sender"))
		Expect(contextAgain.GetActionName()).Should(Equal(actionName))
		Expect(contextAgain.GetParams().String("name")).Should(Equal("John"))
		Expect(contextAgain.GetParams().String("lastName")).Should(Equal("Snow"))

	})
})
