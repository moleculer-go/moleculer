package service_test

//
//import (
//	"context"
//	"fmt"
//
//	log "github.com/sirupsen/logrus"
//
//	. "github.com/onsi/ginkgo"
//	. "github.com/onsi/gomega"
//
//	. "github.com/moleculer-go/moleculer"
//)
//
//var logger = log.WithField("Unit Test", true)
//
//var _ = Describe("MergeActions", func() {
//
//	serviceSchema := Service{
//		Name:    "earth",
//		Version: "0.2",
//		Settings: map[string]interface{}{
//			"dinosauros": true,
//		},
//		Metadata: map[string]interface{}{
//			"star-system": "sun",
//		},
//		Actions: []Action{
//			Action{
//				Name: "rotate",
//				Handler: func(ctx context.Context, params Params) interface{} {
//					return "Hellow Leleu ;) I'm rotating ..."
//				},
//			},
//		},
//		Events: []Event{
//			Event{
//				Name: "earth.rotates",
//				Handler: func(ctx context.Context, params Params) {
//					fmt.Println("spining spining spining")
//				},
//			},
//		},
//	}
//
//	moonMixIn := Mixin{
//		Name: "moon",
//		Settings: map[string]interface{}{
//			"craters": true,
//		},
//		Metadata: map[string]interface{}{
//			"resolution": "high",
//		}, Actions: []Action{
//			Action{
//				Name: "tide",
//				Handler: func(ctx context.Context, params Params) interface{} {
//					return "tide influence in the oceans"
//				},
//			},
//		},
//		Events: []Event{
//			Event{
//				Name: "earth.rotates",
//				Handler: func(ctx context.Context, params Params) {
//					fmt.Println("update tide in relation to the moon")
//				},
//			},
//			Event{
//				Name: "moon.isClose",
//				Handler: func(ctx context.Context, params Params) {
//					fmt.Println("rise the tide !")
//				},
//			},
//		},
//	}
//
//	It("Should merge and overwrite existing actions", func() {
//
//		//just to avoid the "not used errors"
//		Expect(serviceSchema).Should(Not(BeNil()))
//		Expect(moonMixIn).Should(Not(BeNil()))
//
//		// merged := ...
//
//		// thisName := serviceSchema.GetName()
//		// Expect(thisName).Should(Equal(serviceSchemaOriginal.Name))
//		// Expect(thisName).Should(Not(Equal(serviceSchemaMixin.Name)))
//
//	})
//
//})
