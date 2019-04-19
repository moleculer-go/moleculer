package service_test

import (
	"fmt"
	"time"

	"github.com/moleculer-go/moleculer/test"

	log "github.com/sirupsen/logrus"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/service"
)

var logger = log.WithField("Unit Test", true)

type MathService struct {
}

func (s MathService) Name() string {
	return "math"
}

func (s *MathService) Add(params moleculer.Payload) int {
	return params.Get("a").Int() + params.Get("b").Int()
}

func (s *MathService) Sub(a int, b int) int {
	return a - b
}

var _ = Describe("moleculer/service", func() {

	moonMixIn := moleculer.Mixin{
		Name: "moon",
		Settings: map[string]interface{}{
			"craters": true,
			"round":   true,
		},
		Metadata: map[string]interface{}{
			"resolution": "high",
		}, Actions: []moleculer.Action{
			{
				Name: "tide",
				Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
					return "tide influence in the oceans"
				},
			},
		},
		Events: []moleculer.Event{
			{
				Name: "earth.rotates",
				Handler: func(ctx moleculer.Context, params moleculer.Payload) {
					fmt.Println("update tide in relation to the moon")
				},
			},
			{
				Name: "moon.isClose",
				Handler: func(ctx moleculer.Context, params moleculer.Payload) {
					fmt.Println("rise the tide !")
				},
			},
		},
	}

	serviceSchema := moleculer.ServiceSchema{
		Name:    "earth",
		Version: "0.2",
		Settings: map[string]interface{}{
			"dinosauros": true,
			"round":      false,
		},
		Metadata: map[string]interface{}{
			"star-system": "sun",
		},
		Mixins: []moleculer.Mixin{moonMixIn},
		Actions: []moleculer.Action{
			{
				Name: "rotate",
				Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
					return "Hellow Leleu ;) I'm rotating ..."
				},
			},
		},
		Events: []moleculer.Event{
			{
				Name: "earth.rotates",
				Handler: func(ctx moleculer.Context, params moleculer.Payload) {
					fmt.Println("spining spining spining")
				},
			},
		},
	}

	It("Should merge and overwrite existing actions", func() {

		svcCreatedCalled := false
		serviceSchema.Created = func(svc moleculer.ServiceSchema, log *log.Entry) {
			svcCreatedCalled = true
		}
		svcStartedCalled := false
		serviceSchema.Started = func(ctx moleculer.BrokerContext, svc moleculer.ServiceSchema) {
			svcStartedCalled = true
		}
		svcStoppedCalled := false
		serviceSchema.Stopped = func(ctx moleculer.BrokerContext, svc moleculer.ServiceSchema) {
			svcStoppedCalled = true
		}

		mixCreatedCalled := false
		serviceSchema.Mixins[0].Created = func(svc moleculer.ServiceSchema, log *log.Entry) {
			mixCreatedCalled = true
		}
		mixStartedCalled := false
		serviceSchema.Mixins[0].Started = func(ctx moleculer.BrokerContext, svc moleculer.ServiceSchema) {
			mixStartedCalled = true
		}
		mixStoppedCalled := false
		serviceSchema.Mixins[0].Stopped = func(ctx moleculer.BrokerContext, svc moleculer.ServiceSchema) {
			mixStoppedCalled = true
		}

		svc := service.FromSchema(serviceSchema, test.DelegatesWithId("test"))
		name := svc.Name()
		Expect(name).Should(Equal(serviceSchema.Name))
		Expect(name).Should(Not(Equal(moonMixIn.Name)))

		Expect(len(svc.Actions())).Should(Equal(2))
		Expect(svc.Actions()[0].Name()).Should(Equal("rotate"))
		Expect(svc.Actions()[1].Name()).Should(Equal("tide"))

		Expect(len(svc.Events())).Should(Equal(2))
		Expect(svc.Events()[0].Name()).Should(Equal("earth.rotates"))
		Expect(svc.Events()[1].Name()).Should(Equal("moon.isClose"))

		Expect(len(svc.Settings())).Should(Equal(3))
		Expect(svc.Settings()["craters"]).Should(Equal(true))
		Expect(svc.Settings()["dinosauros"]).Should(Equal(true))
		Expect(svc.Settings()["round"]).Should(Equal(false))

		svc.Start(nil)
		svc.Stop(nil)
		time.Sleep(time.Millisecond * 100)
		Expect(svcCreatedCalled).Should(BeTrue())
		Expect(svcStartedCalled).Should(BeTrue())
		Expect(svcStoppedCalled).Should(BeTrue())
		Expect(mixCreatedCalled).Should(BeTrue())
		Expect(mixStartedCalled).Should(BeTrue())
		Expect(mixStoppedCalled).Should(BeTrue())
	})

	It("Should publish a service that is an object (not an schema)", func() {
		math := MathService{}
		svc, err := service.FromObject(math, test.DelegatesWithId("test"))
		Expect(err).Should(BeNil())
		Expect(svc.Name()).Should(Equal(math.Name()))
	})

})
