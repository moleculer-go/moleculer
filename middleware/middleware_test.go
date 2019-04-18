package middleware

import (
	"fmt"

	"github.com/moleculer-go/moleculer/context"
	"github.com/moleculer-go/moleculer/service"
	"github.com/moleculer-go/moleculer/test"

	"github.com/moleculer-go/moleculer"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	bus "github.com/moleculer-go/goemitter"
	log "github.com/sirupsen/logrus"
)

var logger = log.WithField("unit test pkg", "middleware_test")

func createLogger(name string, value string) *log.Entry {
	return logger.WithField(name, value)
}

func brokerDelegates(nodeID string) *moleculer.BrokerDelegates {
	localBus := bus.Construct()
	localNode := test.NodeMock{ID: nodeID}
	broker := &moleculer.BrokerDelegates{
		LocalNode: func() moleculer.Node {
			return &localNode
		},
		Logger: createLogger,
		Bus: func() *bus.Emitter {
			return localBus
		}}
	return broker
}

var _ = Describe("Dispatcher", func() {

	It("Add handlers and invoke them", func() {
		dispatcher := Dispatcher(createLogger("midlewares", "dispatcher"))

		hits := 0
		handlers := make(map[string]moleculer.MiddlewareHandler)
		handlers["serviceStarting"] = func(params interface{}, next func(...interface{})) {
			service := params.(*service.Service)
			fmt.Println("serviceStarting #1 -> service: ", service.Name())
			hits = hits + 1
			next()
		}
		dispatcher.Add(handlers)

		handlers = make(map[string]moleculer.MiddlewareHandler)
		handlers["serviceStarting"] = func(params interface{}, next func(...interface{})) {
			service := params.(*service.Service)
			fmt.Println("serviceStarting #2 -> service: ", service.Name())
			hits = hits + 1
			next()
		}
		dispatcher.Add(handlers)

		svc := service.FromSchema(moleculer.ServiceSchema{Name: "serviceA"}, logger)
		resultSvc := dispatcher.CallHandlers("serviceStarting", svc)

		Expect(hits).Should(Equal(2))
		Expect(resultSvc).Should(BeEquivalentTo(svc))

		hits = 0
		handlers = make(map[string]moleculer.MiddlewareHandler)
		handlers["beforeLocalAction"] = func(params interface{}, next func(...interface{})) {
			context := params.(moleculer.BrokerContext)

			hits = hits + 1
			fmt.Println("beforeLocalAction -> context: ", context, " Payload: ", context.Payload())

			context.SetTargetNodeID("changed id")

			next(context)
		}
		dispatcher.Add(handlers)
		initialContext := context.BrokerContext(brokerDelegates("test-midlewares"))
		initialContext.SetTargetNodeID("initial id")
		value := dispatcher.CallHandlers("beforeLocalAction", initialContext)
		newContext := value.(moleculer.BrokerContext)
		Expect(newContext.TargetNodeID()).Should(Equal("changed id"))
		Expect(hits).Should(Equal(1))
	})

})
