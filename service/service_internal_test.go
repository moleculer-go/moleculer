package service

import (
	"fmt"

	snap "github.com/moleculer-go/cupaloy"
	"github.com/moleculer-go/moleculer"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
)

var logger = log.WithField("Unit Test", true)

var _ = Describe("Service", func() {

	It("isInternalEvent() should reconized internal events", func() {

		Expect(isInternalEvent(Event{
			name:        "$services.changed",
			serviceName: "source service",
		})).Should(BeTrue())

		Expect(isInternalEvent(Event{
			name:        "someservice.act$on",
			serviceName: "source service",
		})).Should(BeFalse())

	})

	rotateFunc := func(ctx moleculer.Context, params moleculer.Payload) interface{} {
		return "Hellow Leleu ;) I'm rotating ..."
	}

	rotatesEventFunc := func(ctx moleculer.Context, params moleculer.Payload) {
		fmt.Println("spining spining spining")
	}

	mixinTideFunc := func(ctx moleculer.Context, params moleculer.Payload) interface{} {
		return "tide influence in the oceans"
	}

	mixinRotatesFunc := func(ctx moleculer.Context, params moleculer.Payload) {
		fmt.Println("update tide in relation to the moon")
	}

	mixinMoonIsCloseFunc := func(ctx moleculer.Context, params moleculer.Payload) {
		fmt.Println("rise the tide !")
	}

	moonMixIn := moleculer.Mixin{
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
		Actions: []moleculer.Action{
			moleculer.Action{
				Name:    "tide",
				Handler: mixinTideFunc,
			},
		},
		Events: []moleculer.Event{
			moleculer.Event{
				Name:    "earth.rotates",
				Handler: mixinRotatesFunc,
			},
			moleculer.Event{
				Name:    "moon.isClose",
				Handler: mixinMoonIsCloseFunc,
			},
		},
	}

	serviceSchema := moleculer.Service{
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
		Mixins: []moleculer.Mixin{moonMixIn},
		Actions: []moleculer.Action{
			moleculer.Action{
				Name:    "rotate",
				Handler: rotateFunc,
			},
		},
		Events: []moleculer.Event{
			moleculer.Event{
				Name:    "earth.rotates",
				Handler: rotatesEventFunc,
			},
		},
	}

	It("Should merge and overwrite existing actions", func() {
		merged := extendActions(serviceSchema, &moonMixIn)
		actions := merged.Actions
		Expect(actions).Should(HaveLen(2))
		Expect(snap.Snapshot(actions)).Should(Succeed())
	})

	It("Should merge and overwrite existing events", func() {
		merged := concatenateEvents(serviceSchema, &moonMixIn)
		Expect(merged.Events).Should(HaveLen(2))
		Expect(snap.Snapshot(merged.Events)).Should(Succeed())
	})

	It("Should merge and overwrite existing settings", func() {

		mergedServiceSettings := extendSettings(serviceSchema, &moonMixIn)
		Expect(mergedServiceSettings.Settings).Should(Equal(map[string]interface{}{
			"dinosauros": true,
			"craters":    true,
		},
		))
	})

	It("Should merge and overwrite existing metadata", func() {

		mergedServiceMetadata := extendMetadata(serviceSchema, &moonMixIn)
		Expect(mergedServiceMetadata.Metadata).Should(Equal(map[string]interface{}{
			"star-system": "sun",
			"resolution":  "high",
		},
		))
	})

	It("Should merge and overwrite existing hooks", func() {
		mergedServiceHooks := extendHooks(serviceSchema, &moonMixIn)
		Expect(mergedServiceHooks.Hooks).Should(Equal(map[string]interface{}{
			"solar-system": "true",
			"earth":        "true",
		},
		))
	})

	It("Should apply mixins collectively", func() {
		merged := applyMixins(serviceSchema)
		Expect(merged.Actions).Should(HaveLen(2))
		Expect(merged.Events).Should(HaveLen(2))
		Expect(snap.Snapshot(merged)).Should(Succeed())
	})

})
