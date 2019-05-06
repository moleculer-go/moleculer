package context

import (
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/payload"
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

	g.It("Should create an action context", func() {
		delegates := test.DelegatesWithIdAndConfig("x", moleculer.Config{})
		actionContext := ActionContext(delegates, map[string]interface{}{
			"sender":   "test",
			"id":       "id",
			"action":   "action",
			"level":    2,
			"timeout":  20,
			"parentID": "parentID",
			"params":   map[string]interface{}{},
			"meta":     map[string]interface{}{},
		})
		Expect(actionContext).ShouldNot(BeNil())
		Expect(len(actionContext.AsMap())).Should(Equal(10))
		Expect(actionContext.ActionName()).Should(Equal("action"))
		Expect(actionContext.Payload()).Should(Equal(payload.Empty()))
		Expect(actionContext.ID()).Should(Equal("id"))
		Expect(actionContext.Logger()).ShouldNot(BeNil())

		Expect(func() {
			ActionContext(delegates, map[string]interface{}{
				"sender":   "test",
				"id":       "id",
				"level":    2,
				"parentID": "parentID",
				"params":   map[string]interface{}{},
			})
		}).Should(Panic())
	})

	g.It("Should call SetTargetNodeID", func() {
		delegates := test.DelegatesWithIdAndConfig("x", moleculer.Config{})
		rawContext := BrokerContext(delegates)
		rawContext.SetTargetNodeID("some id")
		Expect(rawContext.TargetNodeID()).Should(Equal("some id"))
	})

	g.It("Should call Logger()", func() {
		delegates := test.DelegatesWithIdAndConfig("x", moleculer.Config{})
		rawContext := BrokerContext(delegates)
		Expect(rawContext.Logger()).ShouldNot(BeNil())
	})

	g.It("Should call RequestID", func() {
		delegates := test.DelegatesWithIdAndConfig("x", moleculer.Config{})
		rawContext := BrokerContext(delegates)
		Expect(rawContext.RequestID()).Should(Equal(""))
	})

	g.It("Should create an event context", func() {
		delegates := test.DelegatesWithIdAndConfig("x", moleculer.Config{})
		eventContext := EventContext(delegates, map[string]interface{}{
			"sender":    "test",
			"id":        "id",
			"event":     "event",
			"data":      map[string]interface{}{},
			"groups":    []string{"a", "b"},
			"broadcast": true,
		})
		Expect(eventContext).ShouldNot(BeNil())
		Expect(eventContext.IsBroadcast()).Should(BeTrue())
		Expect(len(eventContext.AsMap())).Should(Equal(8))
		Expect(eventContext.EventName()).Should(Equal("event"))
		Expect(eventContext.Groups()).Should(Equal([]string{"a", "b"}))
		Expect(eventContext.Payload()).Should(Equal(payload.Empty()))
		Expect(eventContext.ID()).Should(Equal("id"))
		Expect(eventContext.Logger()).ShouldNot(BeNil())

		Expect(func() {
			EventContext(delegates, map[string]interface{}{
				"sender": "test",
				"id":     "id",
			})
		}).Should(Panic())
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
		Expect(eventContext.RequestID()).ShouldNot(Equal(""))
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

	g.It("Should call MCall and delegate it to broker", func() {
		delegates := test.DelegatesWithIdAndConfig("x", moleculer.Config{})
		called := false
		delegates.MultActionDelegate = func(callMaps map[string]map[string]interface{}) chan map[string]moleculer.Payload {
			called = true
			result := make(chan map[string]moleculer.Payload, 1)
			result <- map[string]moleculer.Payload{"key": payload.New("value")}
			return result
		}
		rawContext := BrokerContext(delegates).(moleculer.Context)
		r := <-rawContext.MCall(map[string]map[string]interface{}{})
		Expect(len(r)).Should(Equal(1))
		Expect(called).Should(BeTrue())
	})

	g.It("Should call Call and delegate it to broker", func() {
		delegates := test.DelegatesWithIdAndConfig("x", moleculer.Config{})
		called := false
		delegates.ActionDelegate = func(context moleculer.BrokerContext, opts ...moleculer.OptionsFunc) chan moleculer.Payload {
			called = true
			result := make(chan moleculer.Payload, 1)
			result <- payload.New("value")
			return result
		}
		rawContext := BrokerContext(delegates).(moleculer.Context)
		r := <-rawContext.Call("service.action", "param")
		Expect(r.String()).Should(Equal("value"))
		Expect(called).Should(BeTrue())
	})

	g.It("Should call Emit and delegate it to broker", func() {
		delegates := test.DelegatesWithIdAndConfig("x", moleculer.Config{})
		called := false
		delegates.EmitEvent = func(context moleculer.BrokerContext) {
			called = true
		}
		rawContext := BrokerContext(delegates).(moleculer.Context)
		rawContext.Emit("service.action", "param")
		Expect(called).Should(BeTrue())
	})

	g.It("Should call Broadcast and delegate it to broker", func() {
		delegates := test.DelegatesWithIdAndConfig("x", moleculer.Config{})
		called := false
		delegates.BroadcastEvent = func(context moleculer.BrokerContext) {
			called = true
		}
		rawContext := BrokerContext(delegates).(moleculer.Context)
		rawContext.Broadcast("service.action", "param")
		Expect(called).Should(BeTrue())
	})

	g.It("Should call Publish and delegate it to broker", func() {
		delegates := test.DelegatesWithIdAndConfig("x", moleculer.Config{})
		called := false
		delegates.Publish = func(svcs ...interface{}) {
			called = true
		}
		rawContext := BrokerContext(delegates)
		rawContext.Publish(moleculer.ServiceSchema{})
		Expect(called).Should(BeTrue())
	})

})
