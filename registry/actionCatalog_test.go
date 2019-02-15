package registry_test

import (
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/registry"
	"github.com/moleculer-go/moleculer/service"
	"github.com/moleculer-go/moleculer/strategy"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
)

var _ = Describe("Actions Catalog", func() {
	logger := log.WithField("unit test pkg", "registry_test")
	strategy := strategy.RoundRobinStrategy{}
	params := moleculer.ParamsSchema{}
	node1 := registry.CreateNode("node-test-1", logger)
	node2 := registry.CreateNode("node-test-2", logger)
	handler := func(ctx moleculer.Context, params moleculer.Payload) interface{} {
		return "default action result"
	}
	bankCreditAction := service.CreateServiceAction("bank", "credit", handler, params)

	Describe("Invoking Actions ", func() {
		It("Should find next action by name", func() {

			msg := "message from action"
			catalog := registry.CreateActionCatalog(logger)
			peopleCreate := func(ctx moleculer.Context, params moleculer.Payload) interface{} {
				return msg
			}
			testService := service.Service{}
			testAction := service.CreateServiceAction("people", "create", peopleCreate, params)

			catalog.Add(node1.GetID(), testAction, &testService, true)

			actionName := "people.create"
			actionEntry := catalog.Next(actionName, strategy)
			Expect(actionEntry).Should(Not(BeNil()))

		})
	})

	Describe("Actions Catalog - Add, Next and NextEndpointFromNode", func() {
		//broker := CreateBroker()
		It("Should create a ActionCatalog and should be size 0", func() {

			catalog := registry.CreateActionCatalog(logger)

			Expect(catalog).Should(Not(BeNil()))

			//Expect(catalog.Size()).Should(Equal(0))

		})

		It("Should add a local action to Action Catalog", func() {

			catalog := registry.CreateActionCatalog(logger)

			nextActionEntry := catalog.Next("bank.credit", strategy)
			Expect(nextActionEntry).Should(BeNil())
			testService := service.Service{}
			catalog.Add(node1.GetID(), bankCreditAction, &testService, true)

			//Expect(catalog.Size()).Should(Equal(1))

			nextActionEntry = catalog.Next("bank.credit", strategy)
			Expect(nextActionEntry).Should(Not(BeNil()))
			Expect(nextActionEntry.IsLocal()).Should(Equal(true))

		})

		It("Should add actions and return using Next and NextEndpointFromNode", func() {

			catalog := registry.CreateActionCatalog(logger)

			nextAction := catalog.Next("bank.credit", strategy)
			Expect(nextAction).Should(BeNil())

			testService := service.Service{}

			catalog.Add(node1.GetID(), bankCreditAction, &testService, true)

			//Expect(catalog.Size()).Should(Equal(1))

			nextAction = catalog.Next("bank.credit", strategy)
			Expect(nextAction).Should(Not(BeNil()))
			Expect(nextAction.IsLocal()).Should(Equal(true))

			nextAction = catalog.Next("user.signUp", strategy)
			Expect(nextAction).Should(BeNil())

			catalog.Add(node1.GetID(), service.CreateServiceAction("user", "signUp", handler, params), &testService, true)

			//Expect(catalog.Size()).Should(Equal(2))
			nextAction = catalog.Next("user.signUp", strategy)
			Expect(nextAction).Should(Not(BeNil()))
			Expect(nextAction.IsLocal()).Should(Equal(true))

			catalog.Add(node2.GetID(), service.CreateServiceAction("user", "signUp", handler, params), &testService, false)
			//Expect(catalog.Size()).Should(Equal(2))

			//local action on node 1
			nextAction = catalog.NextFromNode("user.signUp", node1.GetID())
			Expect(nextAction).Should(Not(BeNil()))
			Expect(nextAction.IsLocal()).Should(Equal(true))

			//remote action on node 2
			nextAction = catalog.NextFromNode("user.signUp", node2.GetID())
			Expect(nextAction).Should(Not(BeNil()))
			Expect(nextAction.IsLocal()).Should(Equal(false))

			//invalid node id
			nextAction = catalog.NextFromNode("user.signUp", "invalid node id")
			Expect(nextAction).Should(BeNil())

		})

	})

})
