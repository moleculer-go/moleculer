package nats_test

import (
	"os"
	"time"

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

		var userBroker, profileBroker *broker.ServiceBroker
		BeforeEach(func() {

			userBroker = broker.New(&moleculer.Config{
				LogLevel:    logLevel,
				Transporter: transporter,
				DiscoverNodeID: func() string {
					return "user_broker"
				},
				RequestTimeout: time.Second,
			})
			userBroker.Publish(userService())

			profileBroker = broker.New(&moleculer.Config{
				LogLevel:    logLevel,
				Transporter: transporter,
				DiscoverNodeID: func() string {
					return "profile_broker"
				},
				RequestTimeout: time.Second,
			})
			profileBroker.Publish(profileService())

		})

		It("should make a remote call from profile broker a to user broker", func(done Done) {
			userBroker.Start()
			profileBroker.Start()
			profileBroker.WaitFor("user")
			result := <-profileBroker.Call("user.update", longList)
			Expect(result.Error()).Should(Succeed())
			Expect(len(result.StringArray())).Should(Equal(arraySize + 1))

			userBroker.Stop()
			profileBroker.Stop()
			close(done)
		})

		It("should fail after brokers are stopped", func(done Done) {
			userBroker.Start()
			profileBroker.Start()

			profileBroker.WaitFor("user")
			p := (<-profileBroker.Call("user.update", longList))
			Expect(p.Error()).Should(Succeed())
			userBroker.Stop()
			Expect((<-profileBroker.Call("user.update", longList)).IsError()).Should(BeTrue())

			profileBroker.Stop()
			close(done)
		})
	})

	Describe("Start / Stop Cycles.", func() {
		logLevel := "fatal"
		numberOfLoops := 5
		loopNumber := 0
		Measure("Creation of multiple brokers with connect/disconnect cycles running on nats transporter.", func(bench Benchmarker) {
			transporter := "nats://" + NatsTestHost + ":4222"
			var userBroker, contactBroker, profileBroker *broker.ServiceBroker
			bench.Time("brokers creation", func() {
				userBroker = broker.New(&moleculer.Config{
					LogLevel:       logLevel,
					Transporter:    transporter,
					RequestTimeout: time.Second,
				})
				userBroker.Publish(userService())
				userBroker.Start()

				contactBroker = broker.New(&moleculer.Config{
					LogLevel:       logLevel,
					Transporter:    transporter,
					RequestTimeout: time.Second,
				})
				contactBroker.Publish(contactService())
				contactBroker.Start()

				profileBroker = broker.New(&moleculer.Config{
					LogLevel:       logLevel,
					Transporter:    transporter,
					RequestTimeout: time.Second,
				})
				profileBroker.Publish(profileService())
				profileBroker.Start()

				userBroker.WaitFor("contact", "profile")
				contactBroker.WaitFor("user", "profile")
				profileBroker.WaitFor("contact", "user")
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

	It("Should fail to connect", func() {
		logger := contextA.Logger()
		var serializer serializer.Serializer = serializer.CreateJSONSerializer(logger)
		options := nats.NATSOptions{
			URL:        "some ivalid URL",
			Name:       "test-cluster",
			Logger:     logger,
			Serializer: serializer,
			ValidateMsg: func(msg moleculer.Payload) bool {
				return true
			},
		}
		transporter := nats.CreateNatsTransporter(options)
		transporter.SetPrefix("MOL")
		Expect(<-transporter.Connect()).ShouldNot(Succeed())
	})

	It("Should not fail on double disconnect", func() {
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
		transporter := nats.CreateNatsTransporter(options)
		transporter.SetPrefix("MOL")
		Expect(<-transporter.Connect()).Should(Succeed())
		Expect(<-transporter.Disconnect()).Should(Succeed())
		Expect(<-transporter.Disconnect()).Should(Succeed())
	})

	It("Should fail Subscribe() and Publish() when is not connected", func() {
		transporter := nats.CreateNatsTransporter(nats.NATSOptions{})
		Expect(func() { transporter.Subscribe("", "", func(moleculer.Payload) {}) }).Should(Panic())
		Expect(func() { transporter.Publish("", "", payload.Empty()) }).Should(Panic())
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
