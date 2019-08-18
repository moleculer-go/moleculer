package nats_test

import (
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

var StanTestHost = os.Getenv("STAN_HOST")
var _ = Describe("NATS Streaming Transit", func() {
	brokerDelegates := BrokerDelegates()
	contextA := context.BrokerContext(brokerDelegates)
	logger := contextA.Logger()
	var serializer serializer.Serializer = serializer.CreateJSONSerializer(logger)
	url := "stan://" + StanTestHost + ":4222"
	options := nats.StanOptions{
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
		numberOfLoops := 5
		loopNumber := 0
		Measure("Creation of multiple brokers with connect/disconnect cycles running on stan transporter.", func(bench Benchmarker) {

			var userBroker, contactBroker, profileBroker *broker.ServiceBroker
			bench.Time("brokers creation", func() {
				userBroker = broker.New(&moleculer.Config{
					LogLevel:    logLevel,
					Transporter: "STAN",
				})
				userBroker.Publish(userService())
				userBroker.Start()

				contactBroker = broker.New(&moleculer.Config{
					LogLevel:    logLevel,
					Transporter: "STAN",
				})
				contactBroker.Publish(contactService())
				contactBroker.Start()

				profileBroker = broker.New(&moleculer.Config{
					LogLevel:    logLevel,
					Transporter: "STAN",
				})
				profileBroker.Publish(profileService())
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

				Expect((<-contactBroker.Call("user.update", longList)).IsError()).Should(BeTrue())
				Expect((<-profileBroker.Call("user.update", longList)).IsError()).Should(BeTrue())

				stopBrokers(contactBroker)
				Expect((<-profileBroker.Call("contact.update", longList)).IsError()).Should(BeTrue())

				stopBrokers(profileBroker)
				Expect(func() {
					<-profileBroker.Call("profile.update", longList)
				}).Should(Panic())
			})

			loopNumber++

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
		transporter.SetPrefix("MOL")
		Expect(<-transporter.Connect()).Should(Succeed())

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

		Expect(<-transporter.Disconnect()).Should(Succeed())

	})

	It("Should fail to connect", func() {
		logger := contextA.Logger()
		options := nats.StanOptions{
			URL:        "some ivalid URL",
			ClientID:   "test-cluster",
			Logger:     logger,
			Serializer: serializer,
			ValidateMsg: func(msg moleculer.Payload) bool {
				return true
			},
		}
		transporter := nats.CreateStanTransporter(options)
		transporter.SetPrefix("MOL")
		Expect(<-transporter.Connect()).ShouldNot(Succeed())
	})

	It("Should not fail on double disconnect", func() {
		logger := contextA.Logger()
		options := nats.StanOptions{
			url,
			"test-cluster",
			"unit-test-client-id",
			logger,
			serializer,
			func(msg moleculer.Payload) bool {
				return true
			},
		}
		transporter := nats.CreateStanTransporter(options)
		transporter.SetPrefix("MOL")
		Expect(<-transporter.Connect()).Should(Succeed())
		Expect(<-transporter.Disconnect()).Should(Succeed())
		Expect(<-transporter.Disconnect()).Should(Succeed())
	})

	It("Should fail Subscribe() and Publish() when is not connected", func() {
		transporter := nats.CreateStanTransporter(nats.StanOptions{})
		Expect(func() { transporter.Subscribe("", "", func(moleculer.Payload) {}) }).Should(Panic())
		Expect(func() { transporter.Publish("", "", payload.Empty()) }).Should(Panic())
	})

})
