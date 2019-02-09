package broker_test

import (
	"errors"
	"fmt"
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"

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
			LogLevel: "DEBUG",
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

		bkr := broker.FromConfig(&moleculer.BrokerConfig{
			LogLevel: "DEBUG",
		})
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
		bkr = broker.FromConfig(&moleculer.BrokerConfig{
			LogLevel: "DEBUG",
		})
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
			LogLevel: "DEBUG",
		})
		broker.AddService(service)
		broker.Start()

		result := <-broker.Call("machine.step1", 1)

		fmt.Printf("Results from action: %s", result)

		Expect(result.Value()).Should(Equal(actionResult))
	})

	It("Should listen and reveice events", func() {

		verse := "3 little birds..."
		chorus := "don't worry..."
		musicVerseList := make([]string, 0)
		musicChorusList := make([]string, 0)
		service := moleculer.Service{
			Name: "music",
			Actions: []moleculer.Action{
				moleculer.Action{
					Name: "start",
					Handler: func(ctx moleculer.Context, verse moleculer.Payload) interface{} {
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
						fmt.Println("music.verse --> ", verse.String())
						ctx.Emit("music.chorus", verse)
						musicVerseList = append(musicVerseList, verse.String())
					},
				},
				moleculer.Event{
					Name: "music.chorus",
					Handler: func(ctx moleculer.Context, chorus moleculer.Payload) {
						fmt.Println("music.chorus --> ", chorus.String())
						musicChorusList = append(musicChorusList, chorus.String())
					},
				},
			},
		}

		broker := broker.FromConfig(&moleculer.BrokerConfig{
			LogLevel: "DEBUG",
		})
		broker.AddService(service)
		broker.Start()

		<-broker.Call("music.start", verse)
		time.Sleep(time.Second)

		Expect(len(musicVerseList)).Should(Equal(1))
		Expect(musicVerseList[0]).Should(Equal(verse))
		Expect(len(musicChorusList)).Should(Equal(1))
		Expect(musicChorusList[0]).Should(Equal(verse))

		<-broker.Call("music.stop", chorus)
		time.Sleep(time.Second)

		Expect(len(musicVerseList)).Should(Equal(1))
		Expect(len(musicChorusList)).Should(Equal(2))
		Expect(musicChorusList[1]).Should(Equal(chorus))

		broker.Stop()

	})

})
