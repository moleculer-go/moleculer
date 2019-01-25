package serializer_test

import (
	test "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/moleculer-go/moleculer/context"
	. "github.com/moleculer-go/moleculer/serializer"
)

var _ = test.Describe("JSON Serializer", func() {

	context := CreateBrokerContext(nil, nil, nil, CreateLogger, nil)

	test.It("Should convert between context and Transit Message", func() {
		serializer := CreateJSONSerializer()

		actionName := "some.service.action"
		params := map[string]string{
			"name":     "John",
			"lastName": "Snow",
		}
		actionContext := context.NewActionContext(actionName, params)

		message := serializer.ContextToMessage(actionContext)

		Expect(message.Get("action").String()).Should(Equal(actionName))
		Expect(message.Get("params.name").String()).Should(Equal("John"))
		Expect(message.Get("params.lastName").String()).Should(Equal("Snow"))

		contextAgain := serializer.MessageToContext(message)

		Expect(contextAgain.GetActionName()).Should(Equal(actionName))
		Expect(contextAgain.GetParams().String("name")).Should(Equal("John"))
		Expect(contextAgain.GetParams().String("lastName")).Should(Equal("Snow"))

	})
})
