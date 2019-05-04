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

func natsTestHost() string {
	env := os.Getenv("NATS_HOST")
	if env == "" {
		return "localhost"
	}
	return env
}

var NatsTestHost = natsTestHost()

var _ = Describe("NATS Streaming Transit", func() {
	//log.SetLevel(log.TraceLevel)
	brokerDelegates := BrokerDelegates()
	contextA := context.BrokerContext(brokerDelegates)
	url := "nats://" + NatsTestHost + ":4222"

	stringSize := 50
	arraySize := 100
	var longList []interface{}
	for i := 0; i < arraySize; i++ {
		randomString := util.RandomString(stringSize)
		longList = append(longList, randomString)
	}

	Describe("Remote Calls", func() {
		logLevel := "fatal"
		transporter := "nats://" + NatsTestHost + ":4222"
		userBroker := broker.New(&moleculer.Config{
			LogLevel:    logLevel,
			Transporter: transporter,
		})
		addUserService(userBroker)
		userBroker.Start()

		contactBroker := broker.New(&moleculer.Config{
			LogLevel:    logLevel,
			Transporter: transporter,
		})
		addContactService(contactBroker)
		contactBroker.Start()

		It("should make a remove call from broker a to broker b", func() {
			result := <-userBroker.Call("contact.update", longList)
			fmt.Println("result: ", result)
			Expect(result.IsError()).Should(BeFalse())
			Expect(len(result.StringArray())).Should(Equal(arraySize + 1))
		})
	})

	Describe("Start / Stop Cycles.", func() {
		logLevel := "FATAL"
		numberOfLoops := 10
		loopNumber := 0
		Measure("Creation of multiple brokers with connect/disconnect cycles running on nats transporter.", func(bench Benchmarker) {
			transporter := "nats://" + NatsTestHost + ":4222"
			var userBroker, contactBroker, profileBroker *broker.ServiceBroker
			bench.Time("brokers creation", func() {
				userBroker = broker.New(&moleculer.Config{
					LogLevel:    logLevel,
					Transporter: transporter,
				})
				addUserService(userBroker)
				userBroker.Start()

				contactBroker = broker.New(&moleculer.Config{
					LogLevel:    logLevel,
					Transporter: transporter,
				})
				addContactService(contactBroker)
				contactBroker.Start()

				profileBroker = broker.New(&moleculer.Config{
					LogLevel:    logLevel,
					Transporter: transporter,
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
				fmt.Println("result: ", result)
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
			fmt.Println("\n\n**** One More Loop -> Total: ", loopNumber)

		}, numberOfLoops)

	})

	It("Should connect, subscribe, publish and disconnect", func() {
		logger := contextA.Logger()
		var serializer serializer.Serializer = serializer.CreateJSONSerializer(logger)
		options := nats.NATSOptions{
			URL:        url,
			Name:       "test-cluster",
			Logger:     logger,
			Serializer: serializer,
			ValidateMsg: func(msg moleculer.Payload) bool {
				return true
			},
		}

		params := map[string]string{
			"name":     "John",
			"lastName": "Snow",
		}

		actionName := "some.service.action"
		actionContext := contextA.ChildActionContext(actionName, payload.New(params))

		transporter := nats.CreateNatsTransporter(options)
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

})
