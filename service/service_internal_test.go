package service

import (
	"context"
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
)

var logger = log.WithField("Unit Test", true)

var _ = Describe("MergeActions", func() {

	rotateFunc := func(ctx context.Context, params Params) interface{} {
		return "Hellow Leleu ;) I'm rotating ..."
	}

	rotatesEventFunc := func(ctx context.Context, params Params) {
		fmt.Println("spining spining spining")
	}

	mixinTideFunc := func(ctx context.Context, params Params) interface{} {
		return "tide influence in the oceans"
	}

	mixinRotatesFunc := func(ctx context.Context, params Params) {
		fmt.Println("update tide in relation to the moon")
	}

	mixinMoonIsCloseFunc := func(ctx context.Context, params Params) {
		fmt.Println("rise the tide !")
	}

	serviceSchema := ServiceSchema{
		Name:    "earth",
		Version: "0.2",
		Settings: map[string]interface{}{
			"dinosauros": true,
		},
		Metadata: map[string]interface{}{
			"star-system": "sun",
		},
		Hooks: map[string]interface{}{
			"solar-system": "true",
		},
		Actions: []ServiceActionSchema{
			ServiceActionSchema{
				Name:    "rotate",
				Handler: rotateFunc,
			},
		},
		Events: []ServiceEventSchema{
			ServiceEventSchema{
				Name:    "earth.rotates",
				Handler: rotatesEventFunc,
			},
		},
	}

	moonMixIn := MixinSchema{
		Name: "moon",
		Settings: map[string]interface{}{
			"craters": true,
		},
		Metadata: map[string]interface{}{
			"resolution": "high",
		},
		Hooks: map[string]interface{}{
			"earth": "true",
		},
		Actions: []ServiceActionSchema{
			ServiceActionSchema{
				Name:    "tide",
				Handler: mixinTideFunc,
			},
		},
		Events: []ServiceEventSchema{
			ServiceEventSchema{
				Name:    "earth.rotates",
				Handler: mixinRotatesFunc,
			},
			ServiceEventSchema{
				Name:    "moon.isClose",
				Handler: mixinMoonIsCloseFunc,
			},
		},
	}

	It("Should merge and overwrite existing actions", func() {

		mergedServiceAction := mergeActions(serviceSchemaOriginal, &serviceSchemaMixin)

		Expect(mergedServiceAction.Actions).Should(Equal(serviceSchemaMixin.Actions))
		Expect(mergedServiceAction.Actions).Should(Not(Equal(serviceSchemaOriginal.Actions)))
	})

	It("Should merge and overwrite existing events", func() {

		mergedServiceEvent := mergeEvents(serviceSchemaOriginal, &serviceSchemaMixin)

		Expect(mergedService.Actions).Should(Equal([]ServiceActionSchema{
			ServiceActionSchema{
				Name:    "rotate",
				Handler: rotateFunc,
			},
			ServiceActionSchema{
				Name:    "tide",
				Handler: mixinTideFunc,
			},
		}))

	})

	It("Should merge and overwrite existing events", func() {

		mergedService := mergeEvents(serviceSchema, moonMixIn)
		Expect(mergedService.Events).Should(Equal([]ServiceEventSchema{
			ServiceEventSchema{
				Name:    "earth.rotates",
				Handler: rotatesEventFunc,
			},
			ServiceEventSchema{
				Name:    "moon.isClose",
				Handler: mixinMoonIsCloseFunc,
			},
		},
		))
	})

	It("Should merge and overwrite existing settings", func() {

		mergedService := mergeSettings(serviceSchema, moonMixIn)
		Expect(mergedService.Settings).Should(Equal(map[string]interface{}{
			"dinosauros": true,
			"craters":    true,
		},
		))
	})

	It("Should merge and overwrite existing metadata", func() {

		mergedService := mergeMetadata(serviceSchema, moonMixIn)
		Expect(mergedService.Metadata).Should(Equal(map[string]interface{}{
			"star-system": "sun",
			"resolution":  "high",
		},
		))
	})

	It("Should merge and overwrite existing hooks", func() {

		mergedService := mergeHooks(serviceSchema, moonMixIn)
		Expect(mergedService.Hooks).Should(Equal(map[string]interface{}{
			"solar-system": "true",
			"earth":        "true",
		},
		))
	})

})
