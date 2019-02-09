package service_test

import (
	"fmt"

	log "github.com/sirupsen/logrus"

	test "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/service"
)

var logger = log.WithField("Unit Test", true)

var _ = test.Describe("MergeActions", func() {

	serviceSchema := Service{
		Name:    "earth",
		Version: "0.2",
		Settings: map[string]interface{}{
			"dinosauros": true,
		},
		Metadata: map[string]interface{}{
			"star-system": "sun",
		},
		Actions: []Action{
			Action{
				Name: "rotate",
				Handler: func(ctx Context, params Payload) interface{} {
					return "Hellow Leleu ;) I'm rotating ..."
				},
			},
		},
		Events: []Event{
			Event{
				Name: "earth.rotates",
				Handler: func(ctx Context, params Payload) {
					fmt.Println("spining spining spining")
				},
			},
		},
	}

	moonMixIn := Mixin{
		Name: "moon",
		Settings: map[string]interface{}{
			"craters": true,
		},
		Metadata: map[string]interface{}{
			"resolution": "high",
		}, Actions: []Action{
			Action{
				Name: "tide",
				Handler: func(ctx Context, params Payload) interface{} {
					return "tide influence in the oceans"
				},
			},
		},
		Events: []Event{
			Event{
				Name: "earth.rotates",
				Handler: func(ctx Context, params Payload) {
					fmt.Println("update tide in relation to the moon")
				},
			},
			Event{
				Name: "moon.isClose",
				Handler: func(ctx Context, params Payload) {
					fmt.Println("rise the tide !")
				},
			},
		},
	}

	test.It("Should merge and overwrite existing actions", func() {

		//just to avoid the "not used errors"
		Expect(serviceSchema).Should(Not(BeNil()))
		Expect(moonMixIn).Should(Not(BeNil()))

		thisService := service.FromSchema(serviceSchema)
		thisName := thisService.Name()
		Expect(thisName).Should(Equal(serviceSchema.Name))
		Expect(thisName).Should(Not(Equal(moonMixIn.Name)))

	})

})
