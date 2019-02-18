package broker_test

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/moleculer-go/moleculer/transit/memory"
	log "github.com/sirupsen/logrus"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
	"github.com/moleculer-go/moleculer/context"
	"github.com/moleculer-go/moleculer/util"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var counterCheckTimeout = 2 * time.Second

type counterCheck struct {
	observe           *sync.Map
	observeWithPrefix *sync.Map
}

func (c *counterCheck) Inc(ctx moleculer.Context, name string) {
	if value, exists := c.observe.Load(name); exists {
		c.observe.Store(name, value.(int)+1)
	} else {
		c.observe.Store(name, 1)
	}

	prefixed := attachNodeID(ctx, name)
	if value, exists := c.observeWithPrefix.Load(prefixed); exists {
		c.observeWithPrefix.Store(prefixed, value.(int)+1)
	} else {
		c.observeWithPrefix.Store(prefixed, 1)
	}
}

func (c *counterCheck) Clear() {
	c.observe = &sync.Map{}
	c.observeWithPrefix = &sync.Map{}
}

func attachNodeID(ctx moleculer.Context, name string) string {
	actionContext := ctx.(*context.Context)
	localNodeID := actionContext.BrokerDelegates().LocalNode().GetID()
	return fmt.Sprint(name, "-", localNodeID)
}

func (c *counterCheck) CheckPrefixed(name string, value int) error {
	return c.checkAbs(c.observeWithPrefix, name, value)
}

func (c *counterCheck) Check(name string, value int) error {
	return c.checkAbs(c.observe, name, value)
}

func (c *counterCheck) checkAbs(values *sync.Map, name string, target int) error {
	result := make(chan error)
	go func() {
		start := time.Now()
		for {
			value, exists := values.Load(name)
			if exists && value.(int) >= target || target == 0 {
				result <- nil
			}
			//fmt.Println("values.Load(name) -> ", value, " name:", name)
			if time.Since(start) > counterCheckTimeout {
				result <- errors.New(fmt.Sprint("counter check timed out! -> name: ", name, " target: ", target, " current: ", value))
			}
			time.Sleep(300 * time.Millisecond)
		}
	}()
	return <-result
}

var _ = Describe("Broker", func() {

	It("Should make a local call and return results", func() {
		actionResult := "abra cadabra"
		service := moleculer.Service{
			Name: "do",
			Actions: []moleculer.Action{
				moleculer.Action{
					Name: "stuff",
					Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
						return actionResult
					},
				},
			},
		}

		broker := broker.FromConfig(&moleculer.BrokerConfig{
			LogLevel: "ERROR",
		})
		broker.AddService(service)
		broker.Start()

		result := <-broker.Call("do.stuff", 1)

		fmt.Printf("Results from action: %s", result)

		Expect(result.Value()).Should(Equal(actionResult))

	})

	It("Should make a local call, call should panic and returned paylod should contain the error", func() {
		//actionResult := "abra cadabra"
		service := moleculer.Service{
			Name: "do",
			Actions: []moleculer.Action{
				moleculer.Action{
					Name: "panic",
					Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
						if params.Bool() {
							panic(errors.New("some random error..."))
						}
						return "no panic"
					},
				},
			},
		}
		mem := &memory.SharedMemory{}
		baseConfig := &moleculer.BrokerConfig{
			LogLevel: "FATAL",
			TransporterFactory: func() interface{} {
				transport := memory.Create(log.WithField("transport", "memory"), mem)
				return &transport
			},
		}
		bkrConfig := &moleculer.BrokerConfig{
			DiscoverNodeID: func() string { return "do-broker" },
		}
		bkr := broker.FromConfig(baseConfig, bkrConfig)
		bkr.AddService(service)
		bkr.Start()

		result := <-bkr.Call("do.panic", true)

		Expect(result.IsError()).Should(Equal(true))
		Expect(result.Error()).Should(BeEquivalentTo(errors.New("some random error...")))

		service = moleculer.Service{
			Name: "remote",
			Actions: []moleculer.Action{
				moleculer.Action{
					Name: "panic",
					Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
						result := <-ctx.Call("do.panic", params)
						ctx.Logger().Debug("params: ", params, " result: ", result.Value())
						if result.IsError() {
							panic(result.Error())
						}
						return result
					},
				},
			},
		}
		bkrConfig = &moleculer.BrokerConfig{
			DiscoverNodeID: func() string { return "remote-broker" },
		}
		bkr = broker.FromConfig(baseConfig, bkrConfig)
		bkr.AddService(service)
		bkr.Start()

		result = <-bkr.Call("remote.panic", true)

		Expect(result.IsError()).Should(Equal(true))
		Expect(result.Error()).Should(BeEquivalentTo(errors.New("some random error...")))

		result = <-bkr.Call("remote.panic", false)

		Expect(result.IsError()).Should(Equal(false))
		Expect(result.String()).Should(BeEquivalentTo("no panic"))
	})

	It("Should call multiple local calls (in chain)", func() {

		actionResult := "step 1 done ! -> step 2: step 2 done ! -> magic: Just magic !!!"
		service := moleculer.Service{
			Name: "machine",
			Actions: []moleculer.Action{
				moleculer.Action{
					Name: "step1",
					Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
						step2Result := <-ctx.Call("machine.step2", 0)
						return fmt.Sprintf("step 1 done ! -> step 2: %s", step2Result.String())
					},
				},
				moleculer.Action{
					Name: "step2",
					Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
						magicResult := <-ctx.Call("machine.magic", 0)
						return fmt.Sprintf("step 2 done ! -> magic: %s", magicResult.String())
					},
				},
				moleculer.Action{
					Name: "magic",
					Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
						ctx.Emit("magic.happened, params", "Always !")
						return "Just magic !!!"
					},
				},
			},
		}

		broker := broker.FromConfig(&moleculer.BrokerConfig{
			LogLevel: "ERROR",
		})
		broker.AddService(service)
		broker.Start()

		result := <-broker.Call("machine.step1", 1)

		fmt.Printf("Results from action: %s", result)

		Expect(result.Value()).Should(Equal(actionResult))
	})

	Describe("Broker events", func() {
		eventsTestSize := 10
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
			counters := counterCheck{&sync.Map{}, &sync.Map{}}

			bench.Time("start broker and send events", func() {
				fmt.Println("############# New Test Cycle #############")

				soundsBroker := broker.FromConfig(baseConfig, &moleculer.BrokerConfig{
					DiscoverNodeID: func() string { return fmt.Sprint("SoundsBroker-", util.RandomString(4)) },
				})
				soundsBroker.AddService(moleculer.Service{
					Name: "music",
					Actions: []moleculer.Action{
						moleculer.Action{
							Name: "start",
							Handler: func(ctx moleculer.Context, verse moleculer.Payload) interface{} {
								ctx.Logger().Debug(" ** !!! ### music.start ### !!! ** ")
								ctx.Emit("music.verse", verse)
								counters.Inc(ctx, "music.start")
								return nil
							},
						},
						moleculer.Action{
							Name: "end",
							Handler: func(ctx moleculer.Context, chorus moleculer.Payload) interface{} {
								ctx.Emit("music.chorus", chorus)
								counters.Inc(ctx, "music.end")
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
								counters.Inc(ctx, "music.music.verse")
							},
						},
						moleculer.Event{
							Name: "music.chorus",
							Handler: func(ctx moleculer.Context, chorus moleculer.Payload) {
								ctx.Logger().Debug("music.chorus --> ", chorus.String())
								counters.Inc(ctx, "music.music.chorus")
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
								counters.Inc(ctx, "dj.music.verse")
							},
						},
						moleculer.Event{
							Name: "music.chorus",
							Handler: func(ctx moleculer.Context, chorus moleculer.Payload) {
								ctx.Logger().Debug("DJ  music.chorus --> ", chorus.String())
								counters.Inc(ctx, "dj.music.chorus")
							},
						},
						moleculer.Event{
							Name: "music.tone",
							Handler: func(ctx moleculer.Context, ring moleculer.Payload) {
								ctx.Logger().Debug("DJ  music.tone ring --> ", ring.String())
								counters.Inc(ctx, "dj.music.tone")
							},
						},
					},
				}
				soundsBroker.AddService(djService)
				soundsBroker.Start()

				<-soundsBroker.Call("music.start", verse)

				Expect(counters.Check("music.music.verse", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("dj.music.verse", 1)).ShouldNot(HaveOccurred())

				Expect(counters.Check("music.music.chorus", 2)).ShouldNot(HaveOccurred())
				Expect(counters.Check("dj.music.chorus", 2)).ShouldNot(HaveOccurred())

				<-soundsBroker.Call("music.end", chorus)

				Expect(counters.Check("music.music.verse", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("dj.music.verse", 1)).ShouldNot(HaveOccurred())

				Expect(counters.Check("music.music.chorus", 3)).ShouldNot(HaveOccurred())
				Expect(counters.Check("dj.music.chorus", 3)).ShouldNot(HaveOccurred())

				visualBroker := broker.FromConfig(baseConfig, &moleculer.BrokerConfig{
					DiscoverNodeID: func() string { return fmt.Sprint("VisualBroker-", util.RandomString(4)) },
				})
				vjService := moleculer.Service{
					Name:         "vj",
					Dependencies: []string{"music", "dj"},
					Events: []moleculer.Event{
						moleculer.Event{
							Name: "music.verse",
							Handler: func(ctx moleculer.Context, verse moleculer.Payload) {
								ctx.Logger().Debug("VJ music.verse --> ", verse.String())
								counters.Inc(ctx, "vj.music.verse")
							},
						},
						moleculer.Event{
							Name: "music.chorus",
							Handler: func(ctx moleculer.Context, chorus moleculer.Payload) {
								ctx.Logger().Debug("VJ  music.chorus --> ", chorus.String())
								counters.Inc(ctx, "vj.music.chorus")
							},
						},
						moleculer.Event{
							Name: "music.tone",
							Handler: func(ctx moleculer.Context, ring moleculer.Payload) {
								ctx.Logger().Debug("VJ  music.tone ring --> ", ring.String())
								counters.Inc(ctx, "vj.music.tone")
							},
						},
					},
				}
				visualBroker.AddService(vjService)

				visualBroker.Start()

				time.Sleep(400 * time.Millisecond)

				counters.Clear()

				visualBroker.Call("music.start", verse)

				Expect(counters.Check("music.start", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("music.music.verse", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("dj.music.verse", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("vj.music.verse", 1)).ShouldNot(HaveOccurred())

				Expect(counters.Check("music.music.chorus", 2)).ShouldNot(HaveOccurred())
				Expect(counters.Check("dj.music.chorus", 2)).ShouldNot(HaveOccurred())
				Expect(counters.Check("vj.music.chorus", 2)).ShouldNot(HaveOccurred())

				<-visualBroker.Call("music.end", chorus)

				Expect(counters.Check("music.end", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("music.music.verse", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("dj.music.verse", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("vj.music.verse", 1)).ShouldNot(HaveOccurred())

				Expect(counters.Check("music.music.chorus", 3)).ShouldNot(HaveOccurred())
				Expect(counters.Check("dj.music.chorus", 3)).ShouldNot(HaveOccurred())
				Expect(counters.Check("vj.music.chorus", 3)).ShouldNot(HaveOccurred())

				fmt.Println("############# second instance of the VJ service #############")
				//add a second instance of the vj service, but only one should receive emit events.
				aquaBroker := broker.FromConfig(baseConfig, &moleculer.BrokerConfig{
					DiscoverNodeID: func() string { return fmt.Sprint("AquaBroker-", util.RandomString(4)) },
				})
				aquaBroker.AddService(vjService)
				aquaBroker.Start()

				counters.Clear()

				aquaBroker.Call("music.start", chorus)

				Expect(counters.Check("music.start", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("music.music.verse", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("dj.music.verse", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("vj.music.verse", 1)).ShouldNot(HaveOccurred())

				Expect(counters.Check("music.music.chorus", 2)).ShouldNot(HaveOccurred())
				Expect(counters.Check("dj.music.chorus", 2)).ShouldNot(HaveOccurred())
				Expect(counters.Check("vj.music.chorus", 2)).ShouldNot(HaveOccurred())

				<-visualBroker.Call("music.end", chorus)

				Expect(counters.Check("music.end", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("music.music.verse", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("dj.music.verse", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("vj.music.verse", 1)).ShouldNot(HaveOccurred())

				Expect(counters.Check("music.music.chorus", 3)).ShouldNot(HaveOccurred())
				Expect(counters.Check("dj.music.chorus", 3)).ShouldNot(HaveOccurred())
				Expect(counters.Check("vj.music.chorus", 3)).ShouldNot(HaveOccurred())

				fmt.Println("############# second instance of the DJ service #############")
				//add a second instance of the dj service
				stormBroker := broker.FromConfig(baseConfig, &moleculer.BrokerConfig{
					DiscoverNodeID: func() string { return fmt.Sprint("StormBroker-", util.RandomString(4)) },
				})
				stormBroker.AddService(djService)
				stormBroker.Start()

				counters.Clear()

				stormBroker.Call("music.start", verse)

				Expect(counters.Check("music.start", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("music.music.verse", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("dj.music.verse", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("vj.music.verse", 1)).ShouldNot(HaveOccurred())

				Expect(counters.Check("music.music.chorus", 2)).ShouldNot(HaveOccurred())
				Expect(counters.Check("dj.music.chorus", 2)).ShouldNot(HaveOccurred())
				Expect(counters.Check("vj.music.chorus", 2)).ShouldNot(HaveOccurred())

				<-visualBroker.Call("music.end", chorus)

				Expect(counters.Check("music.end", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("music.music.verse", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("dj.music.verse", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("vj.music.verse", 1)).ShouldNot(HaveOccurred())

				Expect(counters.Check("music.music.chorus", 3)).ShouldNot(HaveOccurred())
				Expect(counters.Check("dj.music.chorus", 3)).ShouldNot(HaveOccurred())
				Expect(counters.Check("vj.music.chorus", 3)).ShouldNot(HaveOccurred())

				fmt.Println("############# Broadcasts #############")

				counters.Clear()
				//now broadcast and every music.tone event listener should receive it.
				stormBroker.Broadcast("music.tone", "broad< storm >cast")

				Expect(counters.Check("dj.music.tone", 2)).ShouldNot(HaveOccurred())
				Expect(counters.Check("vj.music.tone", 2)).ShouldNot(HaveOccurred())

				counters.Clear()

				//emit and only 2 shuold be accounted
				stormBroker.Emit("music.tone", "Emit< storm >cast")

				Expect(counters.Check("dj.music.tone", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("vj.music.tone", 1)).ShouldNot(HaveOccurred())

				fmt.Println("############# Broadcasts - Remove one DJ Service #############")
				//remove one dj service
				stormBroker.Stop()
				time.Sleep(time.Second)

				counters.Clear()

				aquaBroker.Broadcast("music.tone", "broad< aqua 1 >cast")

				Expect(counters.Check("dj.music.tone", 1)).ShouldNot(HaveOccurred())
				Expect(counters.Check("vj.music.tone", 2)).ShouldNot(HaveOccurred())

				fmt.Println("############# Broadcasts - Remove second DJ Service #############")
				//remove the other dj service
				soundsBroker.Stop()
				time.Sleep(time.Second)

				counters.Clear()
				aquaBroker.Broadcast("music.tone", "broad< aqua 2 >cast")
				time.Sleep(time.Second)

				Expect(counters.Check("dj.music.tone", 0)).ShouldNot(HaveOccurred())
				Expect(counters.Check("vj.music.tone", 2)).ShouldNot(HaveOccurred())

				counters.Clear()
				aquaBroker.Emit("music.tone", "Emit< aqua >cast")

				Expect(counters.Check("dj.music.tone", 0)).ShouldNot(HaveOccurred())
				Expect(counters.Check("vj.music.tone", 1)).ShouldNot(HaveOccurred())

				fmt.Println("############# End of Test #############")

				visualBroker.Stop()
				aquaBroker.Stop()

			})
		}, eventsTestSize)
	})

})
