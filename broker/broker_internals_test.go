package broker

import (
	"fmt"

	"time"

	snap "github.com/moleculer-go/cupaloy"
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/context"
	"github.com/moleculer-go/moleculer/test"
	"github.com/moleculer-go/moleculer/transit/memory"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
)

var _ = Describe("Broker Internals", func() {

	Describe("Broker events", func() {
		eventsTestSize := 1
		currentStep := 0
		//TODO needs refactoring.. the test is not realiable and fail from time to time.
		Measure("Local and remote events", func(bench Benchmarker) {
			logLevel := "ERROR"
			verse := "3 little birds..."
			chorus := "don't worry..."
			mem := &memory.SharedMemory{}
			baseConfig := &moleculer.BrokerConfig{
				LogLevel: logLevel,
				TransporterFactory: func() interface{} {
					transport := memory.Create(log.WithField("transport", "memory"), mem)
					return &transport
				},
			}
			counters := test.Counter()

			bench.Time("start broker and send events", func() {
				currentStep++
				fmt.Println("\n############# New Test Cycle step: ", currentStep, " #############")

				soundsBroker := FromConfig(baseConfig, &moleculer.BrokerConfig{
					DiscoverNodeID: func() string { return "SoundsBroker" },
				})
				soundsBroker.AddService(moleculer.Service{
					Name: "music",
					Actions: []moleculer.Action{
						moleculer.Action{
							Name: "start",
							Handler: func(ctx moleculer.Context, verse moleculer.Payload) interface{} {
								ctx.Logger().Debug(" ** !!! ### music.start ### !!! ** ")
								ctx.Emit("music.verse", verse)
								counters.Inc(ctx.(*context.Context).BrokerDelegates().LocalNode().GetID(), "music.start")
								return nil
							},
						},
						moleculer.Action{
							Name: "end",
							Handler: func(ctx moleculer.Context, chorus moleculer.Payload) interface{} {
								ctx.Emit("music.chorus", chorus)
								counters.Inc(ctx.(*context.Context).BrokerDelegates().LocalNode().GetID(), "music.end")
								return nil
							},
						},
					},
					Events: []moleculer.Event{
						moleculer.Event{
							Name: "music.verse",
							Handler: func(ctx moleculer.Context, verse moleculer.Payload) {
								ctx.Logger().Debug("music.verse --> ", verse.String())
								ctx.Emit("music.chorus", verse)
								counters.Inc(ctx.(*context.Context).BrokerDelegates().LocalNode().GetID(), "music.music.verse")
							},
						},
						moleculer.Event{
							Name: "music.chorus",
							Handler: func(ctx moleculer.Context, chorus moleculer.Payload) {
								ctx.Logger().Debug("music.chorus --> ", chorus.String())
								counters.Inc(ctx.(*context.Context).BrokerDelegates().LocalNode().GetID(), "music.music.chorus")
							},
						},
					},
				})
				djService := moleculer.Service{
					Name:         "dj",
					Dependencies: []string{"music"},
					Events: []moleculer.Event{
						moleculer.Event{
							Name: "music.verse",
							Handler: func(ctx moleculer.Context, verse moleculer.Payload) {
								ctx.Logger().Debug("DJ music.verse --> ", verse.String())
								ctx.Emit("music.chorus", verse)
								counters.Inc(ctx.(*context.Context).BrokerDelegates().LocalNode().GetID(), "dj.music.verse")
							},
						},
						moleculer.Event{
							Name: "music.chorus",
							Handler: func(ctx moleculer.Context, chorus moleculer.Payload) {
								ctx.Logger().Debug("DJ  music.chorus --> ", chorus.String())
								counters.Inc(ctx.(*context.Context).BrokerDelegates().LocalNode().GetID(), "dj.music.chorus")
							},
						},
						moleculer.Event{
							Name: "music.tone",
							Handler: func(ctx moleculer.Context, ring moleculer.Payload) {
								ctx.Logger().Debug("DJ  music.tone ring --> ", ring.String())
								counters.Inc(ctx.(*context.Context).BrokerDelegates().LocalNode().GetID(), "dj.music.tone")
							},
						},
					},
				}
				soundsBroker.AddService(djService)

				// soundsBroker.delegates.EmitEvent = func(context moleculer.BrokerContext) {
				// 	entries := soundsBroker.registry.LoadBalanceEvent(context)
				// 	fmt.Println("entries -> ", entries)
				// 	Expect(snap.SnapshotMulti("entries_1-music.verse_2-music.chorus", entries)).Should(Succeed())
				// }
				soundsBroker.Start()
				Expect(snap.SnapshotMulti("soundsBroker-KnownNodes", soundsBroker.registry.KnownNodes())).Should(Succeed())

				//Scenario: action music.start will emit music.verse wich emits music.chorus - becuase there are 2 listeners for music.serve
				//there should be too emits to music.chorus
				<-soundsBroker.Call("music.start", verse)

				Expect(counters.Check("music.music.verse", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("dj.music.verse", 1)).ShouldNot(HaveOccurred())

				Expect(counters.Check("music.music.chorus", 2)).ShouldNot(HaveOccurred()) //failed here
				Expect(counters.Check("dj.music.chorus", 2)).ShouldNot(HaveOccurred())

				//Scenario: music.end will emit music.chorus once.
				<-soundsBroker.Call("music.end", chorus)

				Expect(counters.Check("music.music.verse", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("dj.music.verse", 1)).ShouldNot(HaveOccurred())

				Expect(counters.Check("music.music.chorus", 3)).ShouldNot(HaveOccurred())
				Expect(counters.Check("dj.music.chorus", 3)).ShouldNot(HaveOccurred())

				visualBroker := FromConfig(baseConfig, &moleculer.BrokerConfig{
					DiscoverNodeID: func() string { return "VisualBroker" },
				})
				visualBroker.localBus.On("$node.disconnected", func(data ...interface{}) {
					nodeID := data[0].(string)
					fmt.Println("\n############# visualBroker -> $node.disconnected -> node id: ", nodeID, " #############")
				})
				vjService := moleculer.Service{
					Name:         "vj",
					Dependencies: []string{"music", "dj"},
					Events: []moleculer.Event{
						moleculer.Event{
							Name: "music.verse",
							Handler: func(ctx moleculer.Context, verse moleculer.Payload) {
								ctx.Logger().Debug("VJ music.verse --> ", verse.String())
								counters.Inc(ctx.(*context.Context).BrokerDelegates().LocalNode().GetID(), "vj.music.verse")
							},
						},
						moleculer.Event{
							Name: "music.chorus",
							Handler: func(ctx moleculer.Context, chorus moleculer.Payload) {
								ctx.Logger().Debug("VJ  music.chorus --> ", chorus.String())
								counters.Inc(ctx.(*context.Context).BrokerDelegates().LocalNode().GetID(), "vj.music.chorus")
							},
						},
						moleculer.Event{
							Name: "music.tone",
							Handler: func(ctx moleculer.Context, ring moleculer.Payload) {
								ctx.Logger().Debug("VJ  music.tone ring --> ", ring.String())
								counters.Inc(ctx.(*context.Context).BrokerDelegates().LocalNode().GetID(), "vj.music.tone")
							},
						},
					},
				}
				visualBroker.AddService(vjService)

				visualBroker.Start()
				Expect(snap.SnapshotMulti("visualBroker-KnownNodes", visualBroker.registry.KnownNodes())).Should(Succeed())

				time.Sleep(400 * time.Millisecond)

				counters.Clear()

				//Scenario: same action music.start as before, but now we added a new broker and new service.
				visualBroker.Call("music.start", verse)

				Expect(counters.Check("music.start", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("music.music.verse", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("dj.music.verse", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("vj.music.verse", 1)).ShouldNot(HaveOccurred())

				Expect(counters.Check("music.music.chorus", 2)).ShouldNot(HaveOccurred()) //failed here
				Expect(counters.Check("dj.music.chorus", 2)).ShouldNot(HaveOccurred())
				Expect(counters.Check("vj.music.chorus", 2)).ShouldNot(HaveOccurred()) // failed here

				<-visualBroker.Call("music.end", chorus)

				Expect(counters.Check("music.end", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("music.music.verse", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("dj.music.verse", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("vj.music.verse", 1)).ShouldNot(HaveOccurred())

				Expect(counters.Check("music.music.chorus", 3)).ShouldNot(HaveOccurred())
				Expect(counters.Check("dj.music.chorus", 3)).ShouldNot(HaveOccurred())
				Expect(counters.Check("vj.music.chorus", 3)).ShouldNot(HaveOccurred())

				fmt.Println("\n############# second instance of the VJ service #############")
				//add a second instance of the vj service, but only one should receive emit events.
				aquaBroker := FromConfig(baseConfig, &moleculer.BrokerConfig{
					DiscoverNodeID: func() string { return "AquaBroker" },
				})
				aquaBroker.AddService(vjService)
				aquaBroker.Start()
				Expect(snap.SnapshotMulti("aquaBroker-KnownNodes", aquaBroker.registry.KnownNodes())).Should(Succeed())
				Expect(snap.SnapshotMulti("aquaBroker-KnownEventListeners", aquaBroker.registry.KnownEventListeners(true))).Should(Succeed())

				counters.Clear()

				aquaBroker.Call("music.start", chorus)

				Expect(counters.Check("music.start", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("music.music.verse", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("dj.music.verse", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("vj.music.verse", 1)).ShouldNot(HaveOccurred())

				Expect(counters.Check("music.music.chorus", 2)).ShouldNot(HaveOccurred())
				Expect(counters.Check("dj.music.chorus", 2)).ShouldNot(HaveOccurred()) //failed here
				Expect(counters.Check("vj.music.chorus", 2)).ShouldNot(HaveOccurred())

				<-visualBroker.Call("music.end", chorus)

				Expect(counters.Check("music.end", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("music.music.verse", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("dj.music.verse", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("vj.music.verse", 1)).ShouldNot(HaveOccurred())

				Expect(counters.Check("music.music.chorus", 3)).ShouldNot(HaveOccurred())
				Expect(counters.Check("dj.music.chorus", 3)).ShouldNot(HaveOccurred())
				Expect(counters.Check("vj.music.chorus", 3)).ShouldNot(HaveOccurred())

				fmt.Println("\n############# second instance of the DJ service #############")
				//add a second instance of the dj service
				stormBroker := FromConfig(baseConfig, &moleculer.BrokerConfig{
					DiscoverNodeID: func() string { return "StormBroker" },
				})
				stormBroker.AddService(djService)
				stormBroker.localBus.On("$node.disconnected", func(data ...interface{}) {
					nodeID := data[0].(string)
					fmt.Println("\n############# stormBroker -> $node.disconnected -> node id: ", nodeID, " #############")
				})
				stormBroker.Start()
				Expect(snap.SnapshotMulti("stormBroker-KnownNodes", stormBroker.registry.KnownNodes())).Should(Succeed())
				Expect(snap.SnapshotMulti("stormBroker-KnownEventListeners", stormBroker.registry.KnownEventListeners(true))).Should(Succeed())

				counters.Clear()

				stormBroker.Call("music.start", verse)

				Expect(counters.Check("music.start", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("music.music.verse", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("dj.music.verse", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("vj.music.verse", 1)).ShouldNot(HaveOccurred())

				Expect(counters.Check("music.music.chorus", 2)).ShouldNot(HaveOccurred())
				Expect(counters.Check("dj.music.chorus", 2)).ShouldNot(HaveOccurred())
				Expect(counters.Check("vj.music.chorus", 2)).ShouldNot(HaveOccurred())

				<-stormBroker.Call("music.end", chorus)

				Expect(counters.Check("music.end", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("music.music.verse", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("dj.music.verse", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("vj.music.verse", 1)).ShouldNot(HaveOccurred())

				Expect(counters.Check("music.music.chorus", 3)).ShouldNot(HaveOccurred())
				Expect(counters.Check("dj.music.chorus", 3)).ShouldNot(HaveOccurred())
				Expect(counters.Check("vj.music.chorus", 3)).ShouldNot(HaveOccurred())

				fmt.Println("\n############# Broadcasts #############")

				counters.Clear()

				Expect(snap.SnapshotMulti("before-stormBroker.Broadcast-stormBroker-KnownNodes", stormBroker.registry.KnownNodes())).Should(Succeed())
				Expect(snap.SnapshotMulti("before-stormBroker.Broadcast-stormBroker-KnownEventListeners", stormBroker.registry.KnownEventListeners(true))).Should(Succeed())

				//now broadcast and every music.tone event listener should receive it.
				stormBroker.Broadcast("music.tone", "broad< storm >cast")

				Expect(counters.Check("dj.music.tone", 2)).ShouldNot(HaveOccurred()) //failed here
				Expect(counters.Check("vj.music.tone", 2)).ShouldNot(HaveOccurred()) //failed here, again, again

				counters.Clear()

				//emit and only 2 shuold be accounted
				stormBroker.Emit("music.tone", "Emit< storm >cast")

				Expect(counters.Check("dj.music.tone", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("vj.music.tone", 1)).ShouldNot(HaveOccurred())

				fmt.Println("\n############# Broadcasts - Remove one DJ Service #############")
				//remove one dj service
				stormBroker.Stop()
				time.Sleep(time.Second)
				counters.Clear()

				Expect(snap.SnapshotMulti("stormBroker-stoped-aquaBroker-KnownNodes", aquaBroker.registry.KnownNodes())).Should(Succeed())
				Expect(snap.SnapshotMulti("stormBroker-stoped-visualBroker-KnownNodes", visualBroker.registry.KnownNodes())).Should(Succeed())
				Expect(snap.SnapshotMulti("stormBroker-stoped-soundsBroker-KnownNodes", soundsBroker.registry.KnownNodes())).Should(Succeed())

				aquaBroker.Broadcast("music.tone", "broad< aqua 1 >cast")

				Expect(counters.Check("dj.music.tone", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("vj.music.tone", 2)).ShouldNot(HaveOccurred()) //failed here, again and again

				fmt.Println("\n############# Broadcasts - Remove second DJ Service #############")
				//remove the other dj service
				soundsBroker.Stop()
				time.Sleep(time.Second)

				counters.Clear()

				Expect(snap.SnapshotMulti("soundsBroker-Stoped-aquaBroker-KnownNodes", aquaBroker.registry.KnownNodes())).Should(Succeed())
				Expect(snap.SnapshotMulti("soundsBroker-Stoped-visualBroker-KnownNodes", visualBroker.registry.KnownNodes())).Should(Succeed())

				aquaBroker.Broadcast("music.tone", "broad< aqua 2 >cast")
				time.Sleep(time.Second)

				Expect(counters.Check("dj.music.tone", 0)).ShouldNot(HaveOccurred())
				Expect(counters.Check("vj.music.tone", 2)).ShouldNot(HaveOccurred())

				counters.Clear()
				aquaBroker.Emit("music.tone", "Emit< aqua >cast")

				Expect(counters.Check("dj.music.tone", 0)).ShouldNot(HaveOccurred())
				Expect(counters.Check("vj.music.tone", 1)).ShouldNot(HaveOccurred())

				fmt.Println("\n############# End of Test #############")

				visualBroker.Stop()
				aquaBroker.Stop()

			})
		}, eventsTestSize)
	})

	Describe("Broker.MCall", func() {

		It("MCall on $node service actions with all params false", func() {
			MCallTimeout := 20 * time.Second
			actionHandler := func(result string) func(moleculer.Context, moleculer.Payload) interface{} {
				return func(ctx moleculer.Context, param moleculer.Payload) interface{} {
					result := fmt.Sprint("input: (", param.String(), " ) -> output: ( ", result, " )")
					fmt.Println("MCALL Action --> ", result)
					return result
				}
			}
			logLevel := "FATAL"
			mem := &memory.SharedMemory{}
			bkr1 := FromConfig(
				&moleculer.BrokerConfig{
					MCallTimeout:   MCallTimeout,
					LogLevel:       logLevel,
					DiscoverNodeID: func() string { return "test-broker1" },
					TransporterFactory: func() interface{} {
						transport := memory.Create(log.WithField("transport", "memory"), mem)
						return &transport
					},
				},
			)
			bkr1.AddService(moleculer.Service{
				Name: "music",
				Actions: []moleculer.Action{
					moleculer.Action{
						Name:    "start",
						Handler: actionHandler("start result"),
					},
					moleculer.Action{
						Name:    "end",
						Handler: actionHandler("end result"),
					},
				},
			})

			bkr2 := FromConfig(
				&moleculer.BrokerConfig{
					MCallTimeout:   MCallTimeout,
					LogLevel:       logLevel,
					DiscoverNodeID: func() string { return "test-broker2" },
					TransporterFactory: func() interface{} {
						transport := memory.Create(log.WithField("transport", "memory"), mem)
						return &transport
					},
				},
			)
			bkr2.AddService(moleculer.Service{
				Name:         "food",
				Dependencies: []string{"music"},
				Actions: []moleculer.Action{
					moleculer.Action{
						Name:    "lunch",
						Handler: actionHandler("lunch result"),
					},
					moleculer.Action{
						Name:    "dinner",
						Handler: actionHandler("dinner result"),
					},
				},
			})

			bkr1.Start()
			bkr2.Start()
			time.Sleep(100 * time.Millisecond)

			mParams := map[string]map[string]interface{}{
				"food-lunch": map[string]interface{}{
					"action": "food.lunch",
					"params": "lunch param",
				},
				"food-dinner": map[string]interface{}{
					"action": "food.dinner",
					"params": "dinner param",
				},
				"music-start": map[string]interface{}{
					"action": "music.start",
					"params": "start param",
				},
				"music-end": map[string]interface{}{
					"action": "music.end",
					"params": "end param",
				},
			}

			mcallResults := <-bkr2.MCall(mParams)
			Expect(snap.SnapshotMulti("bkr2-results", mcallResults)).Should(Succeed())

			mcallResults = <-bkr1.MCall(mParams)
			Expect(snap.SnapshotMulti("bkr1-results", mcallResults)).Should(Succeed())

			bkr1.Stop()
			bkr2.Stop()
		})

		// }

		// orderResults := func(values map[string]moleculer.Payload) interface{} {
		// 	result := make(map[string][]map[string]interface{})
		// 	for key, payload := range values {
		// 		orderBy := "name"
		// 		if key == "nodes" {
		// 			orderBy = "id"
		// 		}
		// 		result[key] = test.OrderMapArray(payload.MapArray(), orderBy)
		// 	}
		// 	return result
		// }

		// It("MCall on $node service actions with all params false",
		// 	harness("all-false",
		// 		map[string]interface{}{
		// 			"withServices":  false,
		// 			"withActions":   false,
		// 			"onlyAvailable": false,
		// 			"withEndpoints": false,
		// 			"skipInternal":  false,
		// 		}, orderResults))

		// It("MCall on $node service actions with all params true",
		// 	harness("all-true",
		// 		map[string]interface{}{
		// 			"withServices":  true,
		// 			"withActions":   true,
		// 			"onlyAvailable": true,
		// 			"withEndpoints": true,
		// 			"skipInternal":  true,
		// 		}, orderResults))

	})

	Context("Middlewares", func() {

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

})
