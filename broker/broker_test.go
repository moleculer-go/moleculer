package broker_test

import (
	"context"
	"fmt"

	. "github.com/onsi/ginkgo"

	. "github.com/onsi/gomega"

	"github.com/moleculer-go/moleculer"
	. "github.com/moleculer-go/moleculer/broker"
	. "github.com/moleculer-go/moleculer/params"
)

var _ = Describe("Broker", func() {

	It("Should make a local call and return results", func() {
		actionResult := "abra cadabra"
		service := moleculer.Service{
			Name: "do",
			Actions: []moleculer.Action{
				moleculer.Action{
					Name: "stuff",
					Handler: func(ctx context.Context, params Params) interface{} {
						return actionResult
					},
				},
			},
		}

		broker := FromConfig()
		broker.AddService(service)
		broker.Start()

		result := <-broker.Call("do.stuff", 1)

		fmt.Printf("Results from action: %s", result)

		Expect(result).Should(Equal(actionResult))

	})

	It("Should call multiple local calls (in chain)", func() {

		actionResult := "abra cadabra"
		service := moleculer.Service{
			Name: "machine",
			Actions: []moleculer.Action{
				moleculer.Action{
					Name: "step1",
					Handler: func(ctx context.Context, params Params) interface{} {
						////////////////////////////////////////
						////
						//// TODO NEXT --> create my own context, to chain calls like ctx.Call() ...
						////  also to help model what I need to pass forward for the context to work..
						////  also implement all context funcionality and params also.. so I can make more useful tests :)
						////
						////////////////////////////////////////
						broker := FromContext(&ctx)
						step2Result := <-broker.Call("machine.step2", 0)
						return fmt.Sprintf("step 1 done ! -> step 2: %s", step2Result.(string))
					},
				},
				moleculer.Action{
					Name: "step2",
					Handler: func(ctx context.Context, params Params) interface{} {
						broker := FromContext(&ctx)
						magicResult := <-broker.Call("machine.magic", 0)
						return fmt.Sprintf("step 2 done ! -> magic: %s", magicResult.(string))
					},
				},
				moleculer.Action{
					Name: "magic",
					Handler: func(ctx context.Context, params Params) interface{} {
						return "Just magic !!!"
					},
				},
			},
		}

		broker := FromConfig()
		broker.AddService(service)
		broker.Start()

		result := <-broker.Call("do.stuff", 1)

		fmt.Printf("Results from action: %s", result)

		Expect(result).Should(Equal(actionResult))
	})

})
