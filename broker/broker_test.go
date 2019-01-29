package broker_test

import (
	"fmt"

	test "github.com/onsi/ginkgo"

	. "github.com/onsi/gomega"

	. "github.com/moleculer-go/moleculer"
)

var _ = test.Describe("Broker", func() {

	test.It("Should make a local call and return results", func() {
		actionResult := "abra cadabra"
		service := Service{
			Name: "do",
			Actions: []Action{
				Action{
					Name: "stuff",
					Handler: func(ctx Context, params Params) interface{} {
						return actionResult
					},
				},
			},
		}

		broker := BrokerFromConfig()
		broker.AddService(service)
		broker.Start()

		result := <-broker.Call("do.stuff", 1)

		fmt.Printf("Results from action: %s", result)

		Expect(result).Should(Equal(actionResult))

	})

	test.It("Should call multiple local calls (in chain)", func() {

		actionResult := "step 1 done ! -> step 2: step 2 done ! -> magic: Just magic !!!"
		service := Service{
			Name: "machine",
			Actions: []Action{
				Action{
					Name: "step1",
					Handler: func(ctx Context, params Params) interface{} {
						step2Result := <-ctx.Call("machine.step2", 0)
						return fmt.Sprintf("step 1 done ! -> step 2: %s", step2Result.(string))
					},
				},
				Action{
					Name: "step2",
					Handler: func(ctx Context, params Params) interface{} {
						magicResult := <-ctx.Call("machine.magic", 0)
						return fmt.Sprintf("step 2 done ! -> magic: %s", magicResult.(string))
					},
				},
				Action{
					Name: "magic",
					Handler: func(ctx Context, params Params) interface{} {
						ctx.Emit("magic.happened, params", "Always !")
						return "Just magic !!!"
					},
				},
			},
		}

		broker := BrokerFromConfig()
		broker.AddService(service)
		broker.Start()

		result := <-broker.Call("machine.step1", 1)

		fmt.Printf("Results from action: %s", result)

		Expect(result).Should(Equal(actionResult))
	})

	test.It("Should make a remove call and return results", func() {
		//TODO
	})

})
