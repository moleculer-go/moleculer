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

	Describe("Measurements", func() {
		eventsTestSize := 3
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

				musicVerseList := make([]string, 0)
				musicChorusList := make([]string, 0)
				djMusicVerseList := make([]string, 0)
				djMusicChorusList := make([]string, 0)
				vjMusicVerseList := make([]string, 0)
				vjMusicChorusList := make([]string, 0)

				verseReceived := make(chan bool)
				djVerseReceived := make(chan bool)
				vjVerseReceived := make(chan bool)

				chorusReceived := make(chan bool)
				djChorusReceived := make(chan bool)

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
								ctx.Logger().Debug("music.verse --> ", verse.String(), " len(musicVerseList): ", len(musicVerseList))
								ctx.Emit("music.chorus", verse)
								musicVerseList = append(musicVerseList, verse.String())
								verseReceived <- true
							},
						},
						moleculer.Event{
							Name: "music.chorus",
							Handler: func(ctx moleculer.Context, chorus moleculer.Payload) {
								ctx.Logger().Debug("music.chorus --> ", chorus.String())
								musicChorusList = append(musicChorusList, chorus.String())
								chorusReceived <- true
							},
						},
					},
				})
				djToneReceived := make(chan int)
				djToneCount := 0
				djService := moleculer.Service{
					Name:         "dj",
					Dependencies: []string{"music"},
					Events: []moleculer.Event{
						moleculer.Event{
							Name: "music.verse",
							Handler: func(ctx moleculer.Context, verse moleculer.Payload) {
								ctx.Logger().Debug("DJ music.verse --> ", verse.String())
								ctx.Emit("music.chorus", verse)
								djMusicVerseList = append(djMusicVerseList, verse.String())
								djVerseReceived <- true
							},
						},
						moleculer.Event{
							Name: "music.chorus",
							Handler: func(ctx moleculer.Context, chorus moleculer.Payload) {
								ctx.Logger().Debug("DJ  music.chorus --> ", chorus.String())
								djMusicChorusList = append(djMusicChorusList, chorus.String())
								djChorusReceived <- true
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

				<-verseReceived
				<-djVerseReceived
				Expect(len(musicVerseList)).Should(Equal(1))
				Expect(len(djMusicVerseList)).Should(Equal(1))

				Expect(musicVerseList[0]).Should(Equal(verse))
				Expect(djMusicVerseList[0]).Should(Equal(verse))

				<-chorusReceived
				<-chorusReceived
				<-djChorusReceived
				<-djChorusReceived
				Expect(len(musicChorusList)).Should(Equal(2))
				Expect(len(djMusicChorusList)).Should(Equal(2))

				Expect(musicChorusList[0]).Should(Equal(verse))
				Expect(djMusicChorusList[0]).Should(Equal(verse))

				<-soundsBroker.Call("music.end", chorus)

				Expect(len(musicVerseList)).Should(Equal(1))
				Expect(len(djMusicVerseList)).Should(Equal(1))

				<-chorusReceived
				<-djChorusReceived
				Expect(len(musicChorusList)).Should(Equal(3))
				Expect(len(djMusicChorusList)).Should(Equal(3))

				Expect(musicChorusList[2]).Should(Equal(chorus))
				Expect(djMusicChorusList[2]).Should(Equal(chorus))

				visualBroker := broker.FromConfig(baseConfig, &moleculer.BrokerConfig{
					DiscoverNodeID: func() string { return fmt.Sprint("VisualBroker-", util.RandomString(4)) },
				})
				vjChorusReceived := make(chan int)
				vjToneReceived := make(chan int)
				vjChorusCount := 0
				vjToneCount := 0
				vjService := moleculer.Service{
					Name:         "vj",
					Dependencies: []string{"music", "dj"},
					Events: []moleculer.Event{
						moleculer.Event{
							Name: "music.verse",
							Handler: func(ctx moleculer.Context, verse moleculer.Payload) {
								ctx.Logger().Debug("VJ music.verse --> ", verse.String())
								vjMusicVerseList = append(vjMusicVerseList, verse.String())
								vjVerseReceived <- true
							},
						},
						moleculer.Event{
							Name: "music.chorus",
							Handler: func(ctx moleculer.Context, chorus moleculer.Payload) {
								ctx.Logger().Debug("VJ  music.chorus --> ", chorus.String())
								vjMusicChorusList = append(vjMusicChorusList, chorus.String())
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

				visualBroker.Call("music.start", verse)

				<-verseReceived
				<-djVerseReceived
				<-vjVerseReceived
				Expect(len(musicVerseList)).Should(Equal(2))
				Expect(len(djMusicVerseList)).Should(Equal(2))
				Expect(len(vjMusicVerseList)).Should(Equal(1))

				Expect(musicVerseList[1]).Should(Equal(verse))
				Expect(djMusicVerseList[1]).Should(Equal(verse))
				Expect(vjMusicVerseList[0]).Should(Equal(verse))

				<-chorusReceived
				<-chorusReceived
				Expect(len(musicChorusList)).Should(Equal(5))

				<-djChorusReceived
				<-djChorusReceived
				Expect(len(djMusicChorusList)).Should(Equal(5))

				<-vjChorusReceived
				Expect(<-vjChorusReceived).Should(Equal(2))

				Expect(musicChorusList[4]).Should(Equal(verse))
				Expect(djMusicChorusList[4]).Should(Equal(verse))
				Expect(vjMusicChorusList[1]).Should(Equal(verse))

				<-visualBroker.Call("music.end", chorus)

				<-chorusReceived
				<-vjChorusReceived
				<-djChorusReceived
				Expect(len(musicVerseList)).Should(Equal(2))
				Expect(len(djMusicVerseList)).Should(Equal(2))
				Expect(len(vjMusicVerseList)).Should(Equal(1))

				Expect(len(musicChorusList)).Should(Equal(6))
				Expect(len(djMusicChorusList)).Should(Equal(6))
				Expect(len(vjMusicChorusList)).Should(Equal(3))

				Expect(musicChorusList[5]).Should(Equal(chorus))
				Expect(djMusicChorusList[5]).Should(Equal(chorus))
				Expect(vjMusicChorusList[2]).Should(Equal(chorus))

				//add a second instance of the vj service, to only one should receive emit events.
				aquaBroker := broker.FromConfig(baseConfig, &moleculer.BrokerConfig{
					DiscoverNodeID: func() string { return fmt.Sprint("AquaBroker-", util.RandomString(4)) },
				})
				aquaBroker.AddService(vjService)
				aquaBroker.Start()
				aquaBroker.Call("music.start", chorus)

				<-verseReceived
				<-djVerseReceived
				<-vjVerseReceived
				Expect(len(vjMusicVerseList)).Should(Equal(2))
				Expect(vjMusicVerseList[1]).Should(Equal(chorus))

				<-chorusReceived
				<-chorusReceived
				<-djChorusReceived
				<-djChorusReceived
				<-vjChorusReceived
				Expect(<-vjChorusReceived).Should(Equal(5))

				<-aquaBroker.Call("music.end", verse)

				<-chorusReceived
				<-djChorusReceived
				<-vjChorusReceived
				Expect(len(vjMusicVerseList)).Should(Equal(2))
				Expect(len(vjMusicChorusList)).Should(Equal(6))
				Expect(vjMusicChorusList[5]).Should(Equal(verse))

				//add a second instance of the dj service
				stormBroker := broker.FromConfig(baseConfig, &moleculer.BrokerConfig{
					DiscoverNodeID: func() string { return fmt.Sprint("StormBroker-", util.RandomString(4)) },
				})
				stormBroker.AddService(djService)
				stormBroker.Start()

				djMusicVerseList = make([]string, 0)
				djMusicChorusList = make([]string, 0)

				stormBroker.Call("music.start", verse)

				<-verseReceived
				<-djVerseReceived
				<-vjVerseReceived
				Expect(len(djMusicVerseList)).Should(Equal(1))
				Expect(djMusicVerseList[0]).Should(Equal(verse))

				<-chorusReceived
				<-chorusReceived
				<-djChorusReceived
				<-djChorusReceived
				<-vjChorusReceived
				<-vjChorusReceived
				Expect(len(djMusicChorusList)).Should(Equal(2))

				Expect(djMusicChorusList[1]).Should(Equal(verse))

				<-visualBroker.Call("music.end", chorus)

				<-chorusReceived
				<-djChorusReceived
				<-vjChorusReceived
				Expect(len(djMusicVerseList)).Should(Equal(1))

				Expect(len(djMusicChorusList)).Should(Equal(3))

				Expect(djMusicChorusList[2]).Should(Equal(chorus))

				//now broadcast and every music.tone event listener should receive it.
				stormBroker.Broadcast("music.tone", "broad< storm >cast")

				Expect(<-djToneReceived).Should(Equal(1))
				Expect(<-djToneReceived).Should(Equal(2))
				Expect(<-vjToneReceived).Should(Equal(1))
				Expect(<-vjToneReceived).Should(Equal(2))

				djToneCount = 0
				vjToneCount = 0

				//emit and only 2 shuold be accounted
				stormBroker.Emit("music.tone", "Emit< storm >cast")

				Expect(<-djToneReceived).Should(Equal(1))
				Expect(<-vjToneReceived).Should(Equal(1))

				djToneCount = 0
				vjToneCount = 0

				//remove one dj service
				stormBroker.Stop()

				time.Sleep(400 * time.Millisecond)

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
