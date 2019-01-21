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

	})

})
