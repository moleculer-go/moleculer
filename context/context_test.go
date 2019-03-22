package context

import (
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/test"

	g "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = g.Describe("Context", func() {

	g.It("Should be able to cast into moleculer.Context", func() {
		delegates := test.DelegatesWithIdAndConfig("x", moleculer.Config{})
		rawContext := BrokerContext(delegates)
		Expect(func() {
			moleculerContest := rawContext.(moleculer.Context)
			Expect(moleculerContest).ShouldNot(BeNil())
		}).ShouldNot(Panic())
	})

	g.It("Should create a child context with metrics on", func() {

		config := moleculer.Config{
			Metrics: true,
		}
		brokerContext := BrokerContext(test.DelegatesWithIdAndConfig("nodex", config))
		actionContext := brokerContext.ChildActionContext("actionx", nil)
		Expect(actionContext.Meta()).ShouldNot(BeNil())
		Expect((*actionContext.Meta())["metrics"]).Should(BeTrue())

		eventContext := brokerContext.ChildEventContext("eventx", nil, nil, false)
		Expect(eventContext.Meta()).ShouldNot(BeNil())
		Expect((*eventContext.Meta())["metrics"]).Should(BeTrue())

		config = moleculer.Config{
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
