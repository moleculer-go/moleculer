package service

import (
	"fmt"

	snap "github.com/moleculer-go/cupaloy"
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
		Mixins: []MixinSchema{moonMixIn},
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

	test.It("Should merge and overwrite existing actions", func() {
		merged := extendActions(serviceSchema, &moonMixIn)
		Expect(merged.Actions).Should(HaveLen(2))
		Expect(snap.Snapshot(merged.Actions)).Should(Succeed())
	})

	test.It("Should merge and overwrite existing events", func() {
		merged := concatenateEvents(serviceSchema, &moonMixIn)
		Expect(merged.Events).Should(HaveLen(3))
		Expect(snap.Snapshot(merged.Events)).Should(Succeed())
	})

	test.It("Should merge and overwrite existing settings", func() {

		mergedServiceSettings := extendSettings(serviceSchema, &moonMixIn)
		Expect(mergedServiceSettings.Settings).Should(Equal(map[string]interface{}{
			"dinosauros": true,
			"craters":    true,
		},
		))
	})

	test.It("Should merge and overwrite existing metadata", func() {

		mergedServiceMetadata := extendMetadata(serviceSchema, &moonMixIn)
		Expect(mergedServiceMetadata.Metadata).Should(Equal(map[string]interface{}{
			"star-system": "sun",
			"resolution":  "high",
		},
		))
	})

	test.It("Should merge and overwrite existing hooks", func() {
		mergedServiceHooks := extendHooks(serviceSchema, &moonMixIn)
		Expect(mergedServiceHooks.Hooks).Should(Equal(map[string]interface{}{
			"solar-system": "true",
			"earth":        "true",
		},
		))
	})

	test.It("Should apply mixins collectively", func() {
		merged := applyMixins(serviceSchema)
		Expect(merged.Actions).Should(HaveLen(2))
		Expect(merged.Events).Should(HaveLen(3))
		Expect(snap.Snapshot(merged)).Should(Succeed())
	})

})
