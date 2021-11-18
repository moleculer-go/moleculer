package kafka_test

import (
	"os"
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
	"github.com/moleculer-go/moleculer/context"
	"github.com/moleculer-go/moleculer/payload"
	"github.com/moleculer-go/moleculer/serializer"
	"github.com/moleculer-go/moleculer/transit/kafka"
	"github.com/moleculer-go/moleculer/transit/nats"
	"github.com/moleculer-go/moleculer/util"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func kafkaTestHost() string {
	env := os.Getenv("KAFKA_HOST")
	if env == "" {
		return "127.0.0.1"
	}
	return env
}

var KafkaTestHost = kafkaTestHost()

var _ = Describe("Test Kafka Transit", func() {
	brokerDelegates := BrokerDelegates()
	contextA := context.BrokerContext(brokerDelegates)

	stringSize := 50
	arraySize := 100
	var longList []interface{}
	for i := 0; i < arraySize; i++ {
		randomString := util.RandomString(stringSize)
		longList = append(longList, randomString)
	}

	logLevel := "fatal"
	kafkaHost := KafkaTestHost + ":9092"
	kafkakUrl := "kafka://" + kafkaHost

	CreateCommonTopics(kafkaHost)

	time.Sleep(time.Second * 5)

	var userBroker, profileBroker *broker.ServiceBroker
	BeforeSuite(func() {

		userBroker = broker.New(&moleculer.Config{
			LogLevel:    logLevel,
			Transporter: kafkakUrl,
			DiscoverNodeID: func() string {
				return "user_node"
			},
			RequestTimeout: 5 * time.Second,
		})
		userBroker.Publish(userService())

		profileBroker = broker.New(&moleculer.Config{
			LogLevel:    logLevel,
			Transporter: kafkakUrl,
			DiscoverNodeID: func() string {
				return "profile_node"
			},
			RequestTimeout: 5 * time.Second,
		})
		profileBroker.Publish(profileService())

		userBroker.Start()
		profileBroker.Start()
		profileBroker.WaitFor("user")
		profileBroker.WaitForActions("user.update", "user.read")

		time.Sleep(time.Second * 5)
	})

	AfterSuite(func() {
		userBroker.Stop()
		profileBroker.Stop()
	})

	Describe("Remote Calls", func() {

		It("should make a remote call from profile broker a to user broker", func() {

			resultUpdate := <-profileBroker.Call("user.update", longList)
			Expect(resultUpdate.Error()).Should(Succeed())
			Expect(len(resultUpdate.StringArray())).Should(Equal(arraySize + 1))

			resultRead := <-profileBroker.Call("user.read", longList)
			Expect(resultRead.Error()).Should(Succeed())
			Expect(len(resultRead.StringArray())).Should(Equal(arraySize))
		})

		It("Should not fail on double disconnect", func() {
			logger := contextA.Logger()
			var serializer serializer.Serializer = serializer.CreateJSONSerializer(logger)
			options := kafka.KafkaOptions{
				Url:        kafkakUrl,
				Name:       "test-cluster",
				Logger:     logger,
				Serializer: serializer,
			}
			transporter := kafka.CreateKafkaTransporter(options)
			transporter.SetPrefix("MOL")
			Expect(<-transporter.Connect()).Should(Succeed())
			Expect(<-transporter.Disconnect()).Should(Succeed())
			Expect(<-transporter.Disconnect()).Should(Succeed())
		})

		It("Should fail Subscribe() and Publish() when is not connected", func() {
			transporter := kafka.CreateKafkaTransporter(kafka.KafkaOptions{})
			Expect(func() { transporter.Subscribe("", "", func(moleculer.Payload) {}) }).Should(Panic())
			Expect(func() { transporter.Publish("", "", payload.Empty()) }).Should(Panic())
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

		It("Should connect, subscribe, publish and disconnect", func() {
			logger := contextA.Logger()
			serializer := serializer.CreateJSONSerializer(logger)
			options := kafka.KafkaOptions{
				Url:        kafkakUrl,
				Name:       "test-cluster",
				Logger:     logger,
				Serializer: serializer,
			}

			params := map[string]string{
				"name":     "John",
				"lastName": "Snow",
			}

			actionName := "some.service.action"
			actionContext := contextA.ChildActionContext(actionName, payload.New(params))

			node := "node1"
			topicName := "REQ"

			transporter := kafka.CreateKafkaTransporter(options)
			transporter.SetSerializer(serializer)
			transporter.SetPrefix("MOL")
			transporter.SetNodeID(node)
			Expect(<-transporter.Connect()).Should(Succeed())

			received := make(chan bool)
			done := make(chan bool)

			transporter.Subscribe(topicName, node, func(message moleculer.Payload) {

				contextMap := serializer.PayloadToContextMap(message)

				newContext := context.ActionContext(brokerDelegates, contextMap)
				Expect(newContext.ActionName()).Should(Equal(actionName))
				contextParams := newContext.Payload()
				Expect(contextParams.Get("name").String()).Should(Equal("John"))
				Expect(contextParams.Get("lastName").String()).Should(Equal("Snow"))

				close(done)
				received <- true
			})

			contextMap := actionContext.AsMap()
			contextMap["sender"] = "someone"
			msg, _ := serializer.MapToPayload(&contextMap)

			transporter.Publish(topicName, node, msg)

			Eventually(done, "5s").Should(BeClosed())
			Expect(<-received).Should(Equal(true))

			Expect(<-transporter.Disconnect()).Should(Succeed())
		})
	})

})
