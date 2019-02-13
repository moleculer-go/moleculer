package context

import (
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/test"

	ginkgo "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Context", func() {

	ginkgo.It("Should create a child context with metrics on", func() {

		config := moleculer.BrokerConfig{
			Metrics: true,
		}
		brokerContext := BrokerContext(test.DelegatesWithIdAndConfig("nodex", config))
		actionContext := brokerContext.ChildActionContext("actionx", nil)
		Expect(actionContext.Meta()).ShouldNot(BeNil())
		Expect((*actionContext.Meta())["metrics"]).Should(BeTrue())

		eventContext := brokerContext.ChildEventContext("eventx", nil, nil, false)
		Expect(eventContext.Meta()).ShouldNot(BeNil())
		Expect((*eventContext.Meta())["metrics"]).Should(BeTrue())

		config = moleculer.BrokerConfig{
			Metrics: false,
		}
		brokerContext = BrokerContext(test.DelegatesWithIdAndConfig("nodex", config))
		actionContext = brokerContext.ChildActionContext("actionx", nil)
		Expect(actionContext.Meta()).ShouldNot(BeNil())
		Expect((*actionContext.Meta())["metrics"]).Should(BeNil())

		eventContext = brokerContext.ChildEventContext("eventx", nil, nil, false)
		Expect(eventContext.Meta()).ShouldNot(BeNil())
		Expect((*eventContext.Meta())["metrics"]).Should(BeNil())

	})

})
