package math_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/moleculer-go/moleculer/examples/math"

	. "github.com/moleculer-go/moleculer"
)

var _ = Describe("Math", func() {

	It("Can create a valid service definition", func() {
		serviceDefinition := CreateServiceSchema()

		Expect(serviceDefinition).Should(Not(BeNil()))
		Expect(serviceDefinition.Name).To(Equal("math"))

		Expect(serviceDefinition.Actions).Should(HaveLen(3))
		Expect(serviceDefinition.Actions[0].Name).To(Equal("add"))
		Expect(serviceDefinition.Actions[1].Name).To(Equal("sub"))
		Expect(serviceDefinition.Actions[2].Name).To(Equal("mult"))

		Expect(serviceDefinition.Events).Should(HaveLen(2))
		Expect(serviceDefinition.Events[0].Name).To(Equal("math.add.called"))
		Expect(serviceDefinition.Events[1].Name).To(Equal("math.sub.called"))

	})

	It("Can start broker with service and call actions", func() {
		serviceDefinition := CreateServiceSchema()

		broker := BrokerFromConfig()
		broker.AddService(serviceDefinition)
		broker.Start()

		Expect(broker).Should(Not(BeNil()))

		result := broker.Call("add", map[string]int{
			"a": 1,
			"b": 10,
		})

		Expect(result).Should(Not(BeNil()))
		Expect(result).Should(Equal(11))
	})
})
