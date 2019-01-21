package registry_test

import (
	. "github.com/moleculer-go/moleculer/registry"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Registry", func() {

	Describe("Create a Registry", func() {
		broker := CreateBroker()
		It("Should create a valid registry", func() {
			registry := CreateRegistry(broker)

			Expect(registry).Should(Not(BeNil()))

		})

	})

})
