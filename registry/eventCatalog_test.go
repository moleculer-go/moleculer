package registry_test

import (
	"fmt"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/registry"
	"github.com/moleculer-go/moleculer/service"
	"github.com/moleculer-go/moleculer/strategy"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Actions Catalog", func() {
	strategy := strategy.RoundRobinStrategy{}
	params := moleculer.ParamsSchema{}
	node1 := registry.CreateNode("node-test-1")
	node2 := registry.CreateNode("node-test-2")
	handler := func(ctx moleculer.Context, params moleculer.Payload) {
		fmt.Println("params: ", params)
	}
	//bankCreditAction := service.CreateServiceAction("bank", "credit", handler, params)

	Describe("Event Catalog", func() {
		It("Should add events and find them using Next()", func() {

			catalog := CreateEventCatalog()

			srv := service.FromSchema(moleculer.Service{
				events: []moleculer.Event{
					moleculer.Event{
						Name:    "user.added",
						Handler: handler,
					},
				},
			})
			catalog.Add("node-test-1", srv.Events()[0], true)

		})
	})

})
