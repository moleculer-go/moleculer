package broker

import (
	"fmt"

	"github.com/moleculer-go/moleculer"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Broker Internals", func() {

	It("Should register user middlewares", func() {

		config := moleculer.BrokerConfig{DisableInternalMiddlewares: true}
		bkr := FromConfig(&config)
		Expect(bkr.middlewares.Has("brokerConfig")).Should(BeFalse())

		config = moleculer.BrokerConfig{
			DisableInternalMiddlewares: true,
			Middlewares: []moleculer.Middlewares{
				map[string]moleculer.MiddlewareHandler{
					"brokerConfig": func(params interface{}, next func(...interface{})) {
						next()
					},
				},
			},
		}
		bkr = FromConfig(&config)
		fmt.Println(bkr.config)
		fmt.Println(bkr.middlewares)
		Expect(bkr.middlewares.Has("brokerConfig")).Should(BeTrue())
		Expect(bkr.middlewares.Has("anotherOne")).Should(BeFalse())
	})

	It("Should call brokerConfig middleware on Start and not change the config", func() {

		brokerConfigCalls := 0
		config := moleculer.BrokerConfig{
			DontWaitForNeighbours:      true,
			DisableInternalMiddlewares: true,
			Middlewares: []moleculer.Middlewares{
				map[string]moleculer.MiddlewareHandler{
					"brokerConfig": func(params interface{}, next func(...interface{})) {
						brokerConfigCalls++
						next()
					},
				},
			},
		}
		bkr := FromConfig(&config)
		Expect(bkr.middlewares.Has("brokerConfig")).Should(BeTrue())
		bkr.Start()
		Expect(brokerConfigCalls).Should(Equal(1))
		bkr.Stop()
	})

	It("Should call brokerConfig middleware on Start and not change the config", func() {

		brokerConfigCalls := 0
		config := moleculer.BrokerConfig{
			DontWaitForNeighbours: true,
			Metrics:               true,
			Middlewares: []moleculer.Middlewares{
				map[string]moleculer.MiddlewareHandler{
					"brokerConfig": func(params interface{}, next func(...interface{})) {
						brokerConfig := params.(moleculer.BrokerConfig)
						brokerConfig.Metrics = false
						brokerConfigCalls++
						next(brokerConfig)
					},
				},
			},
		}
		Expect(config.Metrics).Should(BeTrue())
		bkr := FromConfig(&config)
		bkr.Start()
		Expect(brokerConfigCalls).Should(Equal(1))
		Expect(bkr.config.Metrics).Should(BeFalse())
		bkr.Stop()
	})

})
