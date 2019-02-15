package broker_test

import (
	"errors"
	"fmt"
	"time"

	"github.com/moleculer-go/moleculer/transit/memory"
	log "github.com/sirupsen/logrus"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
	"github.com/moleculer-go/moleculer/util"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

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
		bkrConfig := &moleculer.BrokerConfig{
			LogLevel: "ERROR",
			TransporterFactory: func() interface{} {
				transport := memory.Create(log.WithField("transport", "memory"), mem)
				return &transport
			},
		}
		bkr := broker.FromConfig(bkrConfig)
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
		bkr = broker.FromConfig(bkrConfig)
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
		eventsTestSize := 1
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

			bench.Time("start broker and send events", func() {
				fmt.Println("############# New Test Cycle #############")

				musicVerseCount := 0
				musicChorusCount := 0
				djMusicVerseCount := 0
				djMusicChorusCount := 0
				djToneCount := 0
				vjMusicVerseCount := 0
				vjChorusCount := 0
				vjToneCount := 0

				resetCounts := func() {
					musicVerseCount = 0
					musicChorusCount = 0
					djMusicVerseCount = 0
					djMusicChorusCount = 0
					djToneCount = 0
					vjMusicVerseCount = 0
					vjChorusCount = 0
					vjToneCount = 0
				}

				verseReceived := make(chan int)
				djVerseReceived := make(chan int)
				vjVerseReceived := make(chan int)
				chorusReceived := make(chan int)
				djChorusReceived := make(chan int)
				vjChorusReceived := make(chan int)
				vjToneReceived := make(chan int)
				djToneReceived := make(chan int)

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
								return nil
							},
						},
						moleculer.Action{
							Name: "end",
							Handler: func(ctx moleculer.Context, chorus moleculer.Payload) interface{} {
								ctx.Emit("music.chorus", chorus)
								return nil
							},
						},
					},
					Events: []moleculer.Event{
						moleculer.Event{
							Name: "music.verse",
							Handler: func(ctx moleculer.Context, verse moleculer.Payload) {
								ctx.Logger().Debug("music.verse --> ", verse.String(), " musicVerseCount: ", musicVerseCount)
								ctx.Emit("music.chorus", verse)
								musicVerseCount = musicVerseCount + 1
								verseReceived <- musicVerseCount
							},
						},
						moleculer.Event{
							Name: "music.chorus",
							Handler: func(ctx moleculer.Context, chorus moleculer.Payload) {
								ctx.Logger().Debug("music.chorus --> ", chorus.String())
								musicChorusCount = musicChorusCount + 1
								chorusReceived <- musicChorusCount
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
								djMusicVerseCount = djMusicVerseCount + 1
								djVerseReceived <- djMusicVerseCount
							},
						},
						moleculer.Event{
							Name: "music.chorus",
							Handler: func(ctx moleculer.Context, chorus moleculer.Payload) {
								ctx.Logger().Debug("DJ  music.chorus --> ", chorus.String())
								djMusicChorusCount = djMusicChorusCount + 1
								djChorusReceived <- djMusicChorusCount
							},
						},
						moleculer.Event{
							Name: "music.tone",
							Handler: func(ctx moleculer.Context, ring moleculer.Payload) {
								ctx.Logger().Debug("DJ  music.tone ring --> ", ring.String())
								djToneCount = djToneCount + 1
								djToneReceived <- djToneCount
							},
						},
					},
				}
				soundsBroker.AddService(djService)
				soundsBroker.Start()

				<-soundsBroker.Call("music.start", verse)

				Expect(<-verseReceived).Should(Equal(1))
				Expect(<-djVerseReceived).Should(Equal(1))

				Expect(<-chorusReceived).Should(Equal(1))
				Expect(<-chorusReceived).Should(Equal(2))
				Expect(<-djChorusReceived).Should(Equal(1))
				Expect(<-djChorusReceived).Should(Equal(2))

				<-soundsBroker.Call("music.end", chorus)

				Expect(musicVerseCount).Should(Equal(1))
				Expect(djMusicVerseCount).Should(Equal(1))

				Expect(<-chorusReceived).Should(Equal(3))
				Expect(<-djChorusReceived).Should(Equal(3))

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
								vjMusicVerseCount = vjMusicVerseCount + 1
								vjVerseReceived <- vjMusicVerseCount
							},
						},
						moleculer.Event{
							Name: "music.chorus",
							Handler: func(ctx moleculer.Context, chorus moleculer.Payload) {
								ctx.Logger().Debug("VJ  music.chorus --> ", chorus.String())
								vjChorusCount = vjChorusCount + 1
								vjChorusReceived <- vjChorusCount
							},
						},
						moleculer.Event{
							Name: "music.tone",
							Handler: func(ctx moleculer.Context, ring moleculer.Payload) {
								ctx.Logger().Debug("VJ  music.tone ring --> ", ring.String())
								vjToneCount = vjToneCount + 1
								vjToneReceived <- vjToneCount
							},
						},
					},
				}
				visualBroker.AddService(vjService)

				visualBroker.Start()

				time.Sleep(400 * time.Millisecond)

				resetCounts()

				visualBroker.Call("music.start", verse)

				Expect(<-verseReceived).Should(Equal(1))
				Expect(<-djVerseReceived).Should(Equal(1))
				Expect(<-vjVerseReceived).Should(Equal(1))

				Expect(<-chorusReceived).Should(Equal(1))
				Expect(<-chorusReceived).Should(Equal(2))

				Expect(<-djChorusReceived).Should(Equal(1))
				Expect(<-djChorusReceived).Should(Equal(2))

				Expect(<-vjChorusReceived).Should(Equal(1))
				Expect(<-vjChorusReceived).Should(Equal(2))

				<-visualBroker.Call("music.end", chorus)

				Expect(musicVerseCount).Should(Equal(1))
				Expect(djMusicVerseCount).Should(Equal(1))

				Expect(<-chorusReceived).Should(Equal(3))
				Expect(<-vjChorusReceived).Should(Equal(3))
				Expect(<-djChorusReceived).Should(Equal(3))

				//add a second instance of the vj service, but only one should receive emit events.
				aquaBroker := broker.FromConfig(baseConfig, &moleculer.BrokerConfig{
					DiscoverNodeID: func() string { return fmt.Sprint("AquaBroker-", util.RandomString(4)) },
				})
				aquaBroker.AddService(vjService)
				aquaBroker.Start()

				resetCounts()

				aquaBroker.Call("music.start", chorus)

				Expect(<-verseReceived).Should(Equal(1))
				Expect(<-djVerseReceived).Should(Equal(1))
				Expect(<-vjVerseReceived).Should(Equal(1))

				Expect(<-chorusReceived).Should(Equal(1))
				Expect(<-chorusReceived).Should(Equal(2))

				Expect(<-djChorusReceived).Should(Equal(1))
				Expect(<-djChorusReceived).Should(Equal(2))

				Expect(<-vjChorusReceived).Should(Equal(1))
				Expect(<-vjChorusReceived).Should(Equal(2))

				<-visualBroker.Call("music.end", chorus)

				Expect(musicVerseCount).Should(Equal(1))
				Expect(djMusicVerseCount).Should(Equal(1))

				Expect(<-chorusReceived).Should(Equal(3))
				Expect(<-vjChorusReceived).Should(Equal(3))
				Expect(<-djChorusReceived).Should(Equal(3))

				//add a second instance of the dj service
				stormBroker := broker.FromConfig(baseConfig, &moleculer.BrokerConfig{
					DiscoverNodeID: func() string { return fmt.Sprint("StormBroker-", util.RandomString(4)) },
				})
				stormBroker.AddService(djService)
				stormBroker.Start()

				resetCounts()

				stormBroker.Call("music.start", verse)

				Expect(<-verseReceived).Should(Equal(1))
				Expect(<-djVerseReceived).Should(Equal(1))
				Expect(<-vjVerseReceived).Should(Equal(1))

				Expect(<-chorusReceived).Should(Equal(1))
				Expect(<-chorusReceived).Should(Equal(2))

				Expect(<-djChorusReceived).Should(Equal(1))
				Expect(<-djChorusReceived).Should(Equal(2))

				Expect(<-vjChorusReceived).Should(Equal(1))
				Expect(<-vjChorusReceived).Should(Equal(2))

				<-visualBroker.Call("music.end", chorus)

				Expect(musicVerseCount).Should(Equal(1))
				Expect(djMusicVerseCount).Should(Equal(1))

				Expect(<-chorusReceived).Should(Equal(3))
				Expect(<-vjChorusReceived).Should(Equal(3))
				Expect(<-djChorusReceived).Should(Equal(3))

				resetCounts()
				//now broadcast and every music.tone event listener should receive it.
				stormBroker.Broadcast("music.tone", "broad< storm >cast")

				Expect(<-djToneReceived).Should(Equal(1))
				Expect(<-djToneReceived).Should(Equal(2))
				Expect(<-vjToneReceived).Should(Equal(1))
				Expect(<-vjToneReceived).Should(Equal(2))

				resetCounts()

				//emit and only 2 shuold be accounted
				stormBroker.Emit("music.tone", "Emit< storm >cast")

				Expect(<-djToneReceived).Should(Equal(1))
				Expect(<-vjToneReceived).Should(Equal(1))

				djToneCount = 0
				vjToneCount = 0

				//remove one dj service
				stormBroker.Stop()

				time.Sleep(500 * time.Millisecond)

				aquaBroker.Broadcast("music.tone", "broad< aqua 1 >cast")

				Expect(<-djToneReceived).Should(Equal(1))
				Expect(<-vjToneReceived).Should(Equal(1))
				Expect(<-vjToneReceived).Should(Equal(2))

				djToneCount = 0
				vjToneCount = 0

				//remove the other dj service
				soundsBroker.Stop()
				aquaBroker.Broadcast("music.tone", "broad< aqua 2 >cast")

				Expect(djToneCount).Should(Equal(0))
				Expect(<-vjToneReceived).Should(Equal(1))
				Expect(<-vjToneReceived).Should(Equal(2))

				vjToneCount = 0
				aquaBroker.Emit("music.tone", "Emit< aqua >cast")

				Expect(djToneCount).Should(Equal(0))
				//Expect(<-vjToneReceived).Should(Equal(1))

				fmt.Println("############# End of Test #############")

				visualBroker.Stop()
				aquaBroker.Stop()

			})
		}, eventsTestSize)
	})

})
