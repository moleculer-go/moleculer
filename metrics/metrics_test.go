package metrics

import (
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/context"
	"github.com/moleculer-go/moleculer/test"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Metrics", func() {

	It("shouldMetric() should be false", func() {
		brokerContext := context.BrokerContext(test.DelegatesWithIdAndConfig("x", moleculer.BrokerConfig{}))
		Expect(shouldMetric(brokerContext)).Should(BeFalse())

		brokerContext = context.BrokerContext(test.DelegatesWithIdAndConfig("x", moleculer.BrokerConfig{
			Metrics: false,
		}))
		actionContext := brokerContext.ChildActionContext("a", nil)
		Expect(shouldMetric(actionContext)).Should(BeFalse())
	})

	It("shouldMetric() should be true", func() {
		context := context.BrokerContext(test.DelegatesWithIdAndConfig("x", moleculer.BrokerConfig{
			Metrics: true,
		}))
		actionContext := context.ChildActionContext("a", nil)
		Expect(shouldMetric(actionContext)).Should(BeTrue())
	})

	It("shouldMetric() should be true on for half of the requests", func() {

		config := moleculer.BrokerConfig{
			Metrics:     true,
			MetricsRate: .5,
		}
		brokerConfig = config

		brokerContext := context.BrokerContext(test.DelegatesWithIdAndConfig("x", config))
		actionContext := brokerContext.ChildActionContext("a", nil)
		Expect(shouldMetric(actionContext)).Should(BeFalse())
		Expect(shouldMetric(actionContext)).Should(BeTrue())
		Expect(shouldMetric(actionContext)).Should(BeFalse())
		Expect(shouldMetric(actionContext)).Should(BeTrue())
	})

	It("shouldMetric() should be true 1/10 of the requests", func() {

		config := moleculer.BrokerConfig{
			Metrics:     true,
			MetricsRate: .1,
		}
		brokerConfig = config

		brokerContext := context.BrokerContext(test.DelegatesWithIdAndConfig("x", config))
		actionContext := brokerContext.ChildActionContext("a", nil)
		Expect(shouldMetric(actionContext)).Should(BeFalse())
		Expect(shouldMetric(actionContext)).Should(BeFalse())
		Expect(shouldMetric(actionContext)).Should(BeFalse())
		Expect(shouldMetric(actionContext)).Should(BeFalse())
		Expect(shouldMetric(actionContext)).Should(BeFalse())
		Expect(shouldMetric(actionContext)).Should(BeFalse())
		Expect(shouldMetric(actionContext)).Should(BeFalse())
		Expect(shouldMetric(actionContext)).Should(BeFalse())
		Expect(shouldMetric(actionContext)).Should(BeFalse())
		Expect(shouldMetric(actionContext)).Should(BeTrue())
		Expect(shouldMetric(actionContext)).Should(BeFalse())
		Expect(shouldMetric(actionContext)).Should(BeFalse())
	})

})
