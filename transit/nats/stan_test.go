package nats_test

import (
	"fmt"
	"os"

	"github.com/moleculer-go/moleculer/payload"
	"github.com/moleculer-go/moleculer/util"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
	"github.com/moleculer-go/moleculer/context"
	"github.com/moleculer-go/moleculer/serializer"
	"github.com/moleculer-go/moleculer/transit/nats"
)

func addUserService(bkr *broker.ServiceBroker) {
	bkr.AddService(moleculer.Service{
		Name: "user",
		Actions: []moleculer.Action{
			{
				Name: "update",
				Handler: func(context moleculer.Context, params moleculer.Payload) interface{} {
					list := params.StringArray()
					list = append(list, "user update")
					return list
				},
			},
		},
	})
}

func addContactService(bkr *broker.ServiceBroker) {
	bkr.AddService(moleculer.Service{
		Name: "contact",
		Actions: []moleculer.Action{
			{
				Name: "update",
				Handler: func(context moleculer.Context, params moleculer.Payload) interface{} {
					list := params.StringArray()
					list = append(list, "contact update")
					return list
				},
			},
		},
	})
}

func addProfileService(bkr *broker.ServiceBroker) {
	bkr.AddService(moleculer.Service{
		Name:         "profile",
		Dependencies: []string{"user", "contact"},
		Actions: []moleculer.Action{
			{
				Name: "update",
				Handler: func(context moleculer.Context, params moleculer.Payload) interface{} {
					paramsList := params.StringArray()

					contactList := <-context.Call("contact.update", []string{"profile update"})
					userUpdate := <-context.Call("user.update", contactList)

					userList := userUpdate.StringArray()
					for _, item := range paramsList {
						userList = append(userList, item)
					}

					return userList
				},
			},
		},
	})
}

func stopBrokers(brokers ...*broker.ServiceBroker) {
	for _, bkr := range brokers {
		bkr.Stop()
	}
}

var _ = Describe("NATS Streaming Transit", func() {
	brokerDelegates := BrokerDelegates()
	contextA := context.BrokerContext(brokerDelegates)
	logger := contextA.Logger()
	var serializer serializer.Serializer = serializer.CreateJSONSerializer(logger)
	url := "stan://" + os.Getenv("STAN_HOST") + ":4222"
	options := nats.StanOptions{
		"MOL",
		url,
		"test-cluster",
		"unit-test-client-id",
		logger,
		serializer,
		func(msg moleculer.Payload) bool {
			return true
		},
	}

	stringSize := 50
	arraySize := 100
	var longList []interface{}
	for i := 0; i < arraySize; i++ {
		randomString := util.RandomString(stringSize)
		longList = append(longList, randomString)
	}

	Describe("Start / Stop Cycles.", func() {
		logLevel := "FATAL"
		numberOfLoops := 10
		loopNumber := 0
		Measure("Creation of multiple brokers with connect/disconnect cycles running on stan transporter.", func(bench Benchmarker) {

			var userBroker, contactBroker, profileBroker *broker.ServiceBroker
			bench.Time("brokers creation", func() {
				userBroker = broker.New(&moleculer.Config{
					LogLevel:    logLevel,
					Transporter: "STAN",
				})
				addUserService(userBroker)
				userBroker.Start()

				contactBroker = broker.New(&moleculer.Config{
					LogLevel:    logLevel,
					Transporter: "STAN",
				})
				addContactService(contactBroker)
				contactBroker.Start()

				profileBroker = broker.New(&moleculer.Config{
					LogLevel:    logLevel,
					Transporter: "STAN",
				})
				addProfileService(profileBroker)
				profileBroker.Start()
			})

			bench.Time("local calls", func() {
				result := <-userBroker.Call("user.update", longList)
				Expect(len(result.StringArray())).Should(Equal(arraySize + 1))

				result = <-contactBroker.Call("contact.update", longList)
				Expect(len(result.StringArray())).Should(Equal(arraySize + 1))
			})

			bench.Time("5 remote calls", func() {
				result := <-userBroker.Call("contact.update", longList)
				Expect(len(result.StringArray())).Should(Equal(arraySize + 1))

				result = <-contactBroker.Call("user.update", longList)
				Expect(len(result.StringArray())).Should(Equal(arraySize + 1))

				result = <-profileBroker.Call("profile.update", longList)
				Expect(len(result.StringArray())).Should(Equal(arraySize + 3))

				result = <-contactBroker.Call("profile.update", longList)
				Expect(len(result.StringArray())).Should(Equal(arraySize + 3))

				result = <-userBroker.Call("profile.update", longList)
				Expect(len(result.StringArray())).Should(Equal(arraySize + 3))
			})

			bench.Time("stop and fail on action call", func() {
				stopBrokers(userBroker)

				Expect(func() {
					<-contactBroker.Call("user.update", longList)
				}).Should(Panic())
				Expect(func() {
					<-profileBroker.Call("user.update", longList)
				}).Should(Panic())
				// Expect(func() {
				// 	<-profileBroker.Call("profile.update", longList)
				// }).Should(Panic())

				stopBrokers(contactBroker)

				Expect(func() {
					<-profileBroker.Call("contact.update", longList)
				}).Should(Panic())

				stopBrokers(profileBroker)
				Expect(func() {
					<-profileBroker.Call("profile.update", longList)
				}).Should(Panic())
			})

			loopNumber++
			fmt.Println("\n\n**** One More Loop -> Total: ", loopNumber)

		}, numberOfLoops)

	})

	It("Should connect, subscribe, publish and disconnect", func() {
		params := map[string]string{
			"name":     "John",
			"lastName": "Snow",
		}

		actionName := "some.service.action"
		actionContext := contextA.ChildActionContext(actionName, payload.New(params))

		transporter := nats.CreateStanTransporter(options)
		<-transporter.Connect()

		received := make(chan bool)
		transporter.Subscribe("topicA", "node1", func(message moleculer.Payload) {

			contextMap := serializer.PayloadToContextMap(message)

			newContext := context.ActionContext(brokerDelegates, contextMap)
			Expect(newContext.ActionName()).Should(Equal(actionName))
			contextParams := newContext.Payload()
			Expect(contextParams.Get("name").String()).Should(Equal("John"))
			Expect(contextParams.Get("lastName").String()).Should(Equal("Snow"))

			received <- true
		})

		contextMap := actionContext.AsMap()
		contextMap["sender"] = "someone"

		msg, _ := serializer.MapToPayload(&contextMap)

		transporter.Publish("topicA", "node1", msg)

		Expect(<-received).Should(Equal(true))

		<-transporter.Disconnect()

	})

})
