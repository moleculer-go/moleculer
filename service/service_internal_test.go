package service

import (
	"fmt"

	. "github.com/moleculer-go/moleculer/common"
	test "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
)

var logger = log.WithField("Unit Test", true)

var _ = test.Describe("MergeActions", func() {

	rotateFunc := func(ctx Context, params Params) interface{} {
		return "Hellow Leleu ;) I'm rotating ..."
	}

	rotatesEventFunc := func(ctx Context, params Params) {
		fmt.Println("spining spining spining")
	}

	mixinTideFunc := func(ctx Context, params Params) interface{} {
		return "tide influence in the oceans"
	}

	mixinRotatesFunc := func(ctx Context, params Params) {
		fmt.Println("update tide in relation to the moon")
	}

	mixinMoonIsCloseFunc := func(ctx Context, params Params) {
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
		Mixins: []MixinSchema{
			MixinSchema{
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
			},
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

	test.It("Should merge and overwrite existing actions", func() {

		mergedServiceActions := mergeActions(serviceSchema, &moonMixIn)

		Expect(mergedServiceActions.Actions).Should(Equal([]ServiceActionSchema{
			ServiceActionSchema{
				Name:    "rotate",
				Handler: rotateFunc,
			},
			ServiceActionSchema{
				Name:    "tide",
				Handler: mixinTideFunc,
			},
		},
		))
	})

	test.It("Should merge and overwrite existing events", func() {

		mergedServiceEvents := mergeEvents(serviceSchema, &moonMixIn)
		Expect(mergedServiceEvents.Events).Should(Equal([]ServiceEventSchema{
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

	test.It("Should merge and overwrite existing settings", func() {

		mergedServiceSettings := mergeSettings(serviceSchema, &moonMixIn)
		Expect(mergedServiceSettings.Settings).Should(Equal(map[string]interface{}{
			"dinosauros": true,
			"craters":    true,
		},
		))
	})

	test.It("Should merge and overwrite existing metadata", func() {

		mergedServiceMetadata := mergeMetadata(serviceSchema, &moonMixIn)
		Expect(mergedServiceMetadata.Metadata).Should(Equal(map[string]interface{}{
			"star-system": "sun",
			"resolution":  "high",
		},
		))
	})

	test.It("Should merge and overwrite existing hooks", func() {

		mergedServiceHooks := mergeHooks(serviceSchema, &moonMixIn)
		Expect(mergedServiceHooks.Hooks).Should(Equal(map[string]interface{}{
			"solar-system": "true",
			"earth":        "true",
		},
		))
	})

	test.It("Should apply mixins collectively", func() {

		mergedService := applyMixins(serviceSchema)
		Expect(mergedService).Should(Equal(ServiceSchema{
			Name:    "earth",
			Version: "0.2",
			Settings: map[string]interface{}{
				"dinosauros": true,
				"craters":    true,
			},
			Metadata: map[string]interface{}{
				"star-system": "sun",
				"resolution":  "high",
			},
			Hooks: map[string]interface{}{
				"solar-system": "true",
				"earth":        "true",
			},
			Mixins: []MixinSchema{
				MixinSchema{
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
				},
			},
			Actions: []ServiceActionSchema{
				ServiceActionSchema{
					Name:    "rotate",
					Handler: rotateFunc,
				},
				ServiceActionSchema{
					Name:    "tide",
					Handler: mixinTideFunc,
				},
			},
			Events: []ServiceEventSchema{
				ServiceEventSchema{
					Name:    "earth.rotates",
					Handler: rotatesEventFunc,
				},
				ServiceEventSchema{
					Name:    "moon.isClose",
					Handler: mixinMoonIsCloseFunc,
				},
			},
		},
		))
	})

})
