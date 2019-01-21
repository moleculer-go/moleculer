package registry_test

import (
	. "github.com/moleculer-go/moleculer/registry"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Service Catalog", func() {

	Describe("Create a Service Catalog", func() {
		broker := CreateBroker()
		It("Should create a registry and ...", func() {

			registry := CreateRegistry(broker)

			Expect(registry).Should(Not(BeNil()))

		})

	})

})
