package registry_test

import (
	"fmt"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/registry"
	"github.com/moleculer-go/moleculer/service"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
)

var _ = Describe("Event Catalog", func() {
	handler := func(ctx moleculer.Context, params moleculer.Payload) {
		fmt.Println("params: ", params)
	}
	It("Should add events and find them using Next()", func() {

		catalog := registry.CreateEventCatalog(log.New().WithField("catalog", "events"))

		srv := service.FromSchema(moleculer.Service{
			Name: "x",
			Events: []moleculer.Event{
				moleculer.Event{
					Name:    "user.added",
					Handler: handler,
				},
			},
		})
		catalog.Add("node-test-1", srv.Events()[0], true)
		Expect(catalog).ShouldNot(BeNil())
	})
})
