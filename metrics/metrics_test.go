package metrics

import (
	"errors"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/context"
	"github.com/moleculer-go/moleculer/payload"
	"github.com/moleculer-go/moleculer/test"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Metrics", func() {

	It("metricEnd() should record  endTime, duration and fire span.finish event", func() {
		var eventPayload moleculer.Payload
		delegates := test.DelegatesWithIdAndConfig("nodex", moleculer.Config{})
		delegates.EmitEvent = func(context moleculer.BrokerContext) {
			if context.EventName() == "metrics.trace.span.finish" {
				eventPayload = context.Payload()
			} else if context.EventName() == "metrics.trace.span.start" {
				eventPayload = nil
			} else {
				Fail("Invalid event name")
			}
		}
		delegates.ServiceForAction = func(string) []*moleculer.ServiceSchema {
			return []*moleculer.ServiceSchema{&moleculer.ServiceSchema{
				Name:    "math",
				Version: "2",
			}}
		}
		actionContext := context.BrokerContext(delegates).ChildActionContext("math.add", payload.New(nil))
		result := payload.New(errors.New("some error"))

		//calling metricEnd without calling metricStart should not
		//emit the event, since there is not startTime in the context
		metricEnd(actionContext, result)
		Expect(eventPayload).Should(BeNil())

		metricStart(actionContext)

		metricEnd(actionContext, result)
		Expect(eventPayload).ShouldNot(BeNil())
		Expect(eventPayload.Exists()).Should(BeTrue())

		Expect(eventPayload.Get("error").Exists()).Should(BeTrue())
		Expect(eventPayload.Get("error").Get("message").String()).Should(Equal("some error"))

		Expect(eventPayload.Get("duration").Exists()).Should(BeTrue())
		Expect(eventPayload.Get("endTime").Exists()).Should(BeTrue())

		Expect(eventPayload.Get("id").Exists()).Should(BeTrue())
		Expect(eventPayload.Get("level").Exists()).Should(BeTrue())
		Expect(eventPayload.Get("level").Int()).Should(Equal(2))
		Expect(eventPayload.Get("action").Exists()).Should(BeTrue())
		Expect(eventPayload.Get("action").Get("name").String()).Should(Equal("math.add"))

		Expect(eventPayload.Get("service").Exists()).Should(BeTrue())
		Expect(eventPayload.Get("service").Get("name").String()).Should(Equal("math"))
		Expect(eventPayload.Get("service").Get("version").String()).Should(Equal("2"))

		Expect(eventPayload.Get("meta").Exists()).Should(BeTrue())
		Expect(eventPayload.Get("meta").Get("duration").Exists()).Should(Equal(true))
		Expect(eventPayload.Get("meta").Get("duration").Int()).Should(Equal(0))
		Expect(eventPayload.Get("meta").Get("startTime").Exists()).Should(BeTrue())
		Expect(eventPayload.Get("nodeID").String()).Should(Equal("nodex"))
		Expect(eventPayload.Get("remoteCall").Bool()).Should(Equal(true))
		Expect(eventPayload.Get("callerNodeID").String()).Should(Equal(""))

	})

	It("metricStart() should record startTime and fire span.start event", func() {
		var eventPayload moleculer.Payload
		delegates := test.DelegatesWithIdAndConfig("nodex", moleculer.Config{})
		delegates.EmitEvent = func(context moleculer.BrokerContext) {
			Expect(context.EventName()).Should(Equal("metrics.trace.span.start"))
			eventPayload = context.Payload()
		}
		delegates.ServiceForAction = func(string) []*moleculer.ServiceSchema {
			return []*moleculer.ServiceSchema{&moleculer.ServiceSchema{
				Name:    "math",
				Version: "2",
			}}
		}
		actionContext := context.BrokerContext(delegates).ChildActionContext("math.add", payload.New(nil))
		metricStart(actionContext)
		Expect(eventPayload).ShouldNot(BeNil())
		Expect(eventPayload.Exists()).Should(BeTrue())
		Expect(eventPayload.Get("id").Exists()).Should(BeTrue())
		Expect(eventPayload.Get("level").Exists()).Should(BeTrue())
		Expect(eventPayload.Get("level").Int()).Should(Equal(2))
		Expect(eventPayload.Get("action").Exists()).Should(BeTrue())
		Expect(eventPayload.Get("action").Get("name").String()).Should(Equal("math.add"))

		Expect(eventPayload.Get("service").Exists()).Should(BeTrue())
		Expect(eventPayload.Get("service").Get("name").String()).Should(Equal("math"))
		Expect(eventPayload.Get("service").Get("version").String()).Should(Equal("2"))

		Expect(eventPayload.Get("meta").Exists()).Should(BeTrue())
		Expect(eventPayload.Get("meta").Get("duration").Exists()).Should(Equal(true))
		Expect(eventPayload.Get("meta").Get("duration").Int()).Should(Equal(0))
		Expect(eventPayload.Get("meta").Get("startTime").Exists()).Should(BeTrue())
		Expect(eventPayload.Get("nodeID").String()).Should(Equal("nodex"))
		Expect(eventPayload.Get("remoteCall").Bool()).Should(Equal(true))
		Expect(eventPayload.Get("callerNodeID").String()).Should(Equal(""))

	})

	It("createShouldMetric() should be false", func() {

		shouldMetric := createShouldMetric(moleculer.Config{})

		brokerContext := context.BrokerContext(test.DelegatesWithIdAndConfig("x", moleculer.Config{}))
		Expect(shouldMetric(brokerContext)).Should(BeFalse())

		brokerContext = context.BrokerContext(test.DelegatesWithIdAndConfig("x", moleculer.Config{
			Metrics: false,
		}))
		actionContext := brokerContext.ChildActionContext("a", payload.New(nil))
		Expect(shouldMetric(actionContext)).Should(BeFalse())
	})

	It("createShouldMetric() should be true", func() {
		config := moleculer.Config{
			Metrics:     true,
			MetricsRate: 1,
		}
		shouldMetric := createShouldMetric(config)
		context := context.BrokerContext(test.DelegatesWithIdAndConfig("x", config))
		actionContext := context.ChildActionContext("a", payload.New(nil))
		Expect(shouldMetric(actionContext)).Should(BeTrue())
	})

	It("createShouldMetric() should be true on for half of the requests", func() {

		config := moleculer.Config{
			Metrics:     true,
			MetricsRate: .5,
		}
		shouldMetric := createShouldMetric(config)

		brokerContext := context.BrokerContext(test.DelegatesWithIdAndConfig("x", config))
		actionContext := brokerContext.ChildActionContext("a", payload.New(nil))
		Expect(shouldMetric(actionContext)).Should(BeFalse())
		Expect(shouldMetric(actionContext)).Should(BeTrue())
		Expect(shouldMetric(actionContext)).Should(BeFalse())
		Expect(shouldMetric(actionContext)).Should(BeTrue())
	})

	It("createShouldMetric() should be true 1/10 of the requests", func() {

		config := moleculer.Config{
			Metrics:     true,
			MetricsRate: .1,
		}
		shouldMetric := createShouldMetric(config)

		brokerContext := context.BrokerContext(test.DelegatesWithIdAndConfig("x", config))
		actionContext := brokerContext.ChildActionContext("a", payload.New(nil))
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
