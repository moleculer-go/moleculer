package registry_test

import (
	. "github.com/moleculer-go/moleculer/common"
	. "github.com/moleculer-go/moleculer/context"
	. "github.com/moleculer-go/moleculer/registry"
	. "github.com/moleculer-go/moleculer/service"
	. "github.com/moleculer-go/moleculer/strategy"
	test "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = test.Describe("Actions Catalog", func() {
	strategy := RoundRobinStrategy{}
	actionSchema := ActionSchema{}
	node1 := CreateNode("node-test-1")
	node2 := CreateNode("node-test-2")
	handler := func(ctx Context, params Params) interface{} {
		return "default action result"
	}
	bankCreditAction := CreateServiceAction("bank", "credit", handler, actionSchema)

	context := CreateBrokerContext(nil, nil, nil, CreateLogger, node1.GetID())

	test.Describe("Invoking Actions ", func() {
		test.It("Should invoke action on action endpoint", func() {

			msg := "message from action"
			catalog := CreateActionCatalog(nil)
			peopleCreate := func(ctx Context, params Params) interface{} {
				return msg
			}
			testAction := CreateServiceAction("people", "create", peopleCreate, actionSchema)

			catalog.Add(node1.GetID(), testAction, true)

			actionName := "people.create"
			actionEnpoint := catalog.NextEndpoint(actionName, strategy)
			Expect(actionEnpoint).Should(Not(BeNil()))

			actionContext := context.NewActionContext(actionName, nil)
			resultChannel := actionEnpoint.InvokeAction(&actionContext)
			Expect(resultChannel).Should(Not(BeNil()))

			result := <-resultChannel
			Expect(result).Should(Equal(msg))

		})
	})

	test.Describe("Actions Catalog - Add, NextEndpoint and NextEndpointFromNode", func() {
		//broker := CreateBroker()
		test.It("Should create a ActionCatalog and should be size 0", func() {

			catalog := CreateActionCatalog(nil)

			Expect(catalog).Should(Not(BeNil()))

			Expect(catalog.Size()).Should(Equal(0))

		})

		test.It("Should add a local action to Action Catalog", func() {

			catalog := CreateActionCatalog(nil)

			nextEndpoint := catalog.NextEndpoint("bank.credit", strategy)
			Expect(nextEndpoint).Should(BeNil())

			catalog.Add(node1.GetID(), bankCreditAction, true)

			Expect(catalog.Size()).Should(Equal(1))

			nextEndpoint = catalog.NextEndpoint("bank.credit", strategy)
			Expect(nextEndpoint).Should(Not(BeNil()))
			Expect(nextEndpoint.IsLocal()).Should(Equal(true))

		})

		test.It("Should add actions and return using NextEndpoint and NextEndpointFromNode", func() {

			catalog := CreateActionCatalog(nil)

			nextEndpoint := catalog.NextEndpoint("bank.credit", strategy)
			Expect(nextEndpoint).Should(BeNil())

			catalog.Add(node1.GetID(), bankCreditAction, true)

			Expect(catalog.Size()).Should(Equal(1))

			nextEndpoint = catalog.NextEndpoint("bank.credit", strategy)
			Expect(nextEndpoint).Should(Not(BeNil()))
			Expect(nextEndpoint.IsLocal()).Should(Equal(true))

			nextEndpoint = catalog.NextEndpoint("user.signUp", strategy)
			Expect(nextEndpoint).Should(BeNil())

			catalog.Add(node1.GetID(), CreateServiceAction("user", "signUp", handler, actionSchema), true)

			Expect(catalog.Size()).Should(Equal(2))
			nextEndpoint = catalog.NextEndpoint("user.signUp", strategy)
			Expect(nextEndpoint).Should(Not(BeNil()))
			Expect(nextEndpoint.IsLocal()).Should(Equal(true))

			catalog.Add(node2.GetID(), CreateServiceAction("user", "signUp", handler, actionSchema), false)
			Expect(catalog.Size()).Should(Equal(2))

			//local action on node 1
			nextEndpoint = catalog.NextEndpointFromNode("user.signUp", node1.GetID())
			Expect(nextEndpoint).Should(Not(BeNil()))
			Expect(nextEndpoint.IsLocal()).Should(Equal(true))

			//remote action on node 2
			nextEndpoint = catalog.NextEndpointFromNode("user.signUp", node2.GetID())
			Expect(nextEndpoint).Should(Not(BeNil()))
			Expect(nextEndpoint.IsLocal()).Should(Equal(false))

			//invalid node id
			nextEndpoint = catalog.NextEndpointFromNode("user.signUp", "invalid node id")
			Expect(nextEndpoint).Should(BeNil())

		})

	})

})
