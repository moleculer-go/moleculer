package nats_test

import (
	"github.com/moleculer-go/moleculer/util"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
	"github.com/moleculer-go/moleculer/context"
	"github.com/moleculer-go/moleculer/serializer"
	"github.com/moleculer-go/moleculer/transit"
	"github.com/moleculer-go/moleculer/transit/nats"
)

func addUserService(bkr *broker.ServiceBroker) {
	bkr.AddService(moleculer.Service{
		Name: "user",
		Actions: []moleculer.Action{
			{
				Name: "update",
				Handler: func(context moleculer.Context, params moleculer.Params) interface{} {
					list := params.Value().([]interface{})
					//sort.Strings(list)

					list = append(list, "user update")
					list = append(list, "one more...")
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
				Handler: func(context moleculer.Context, params moleculer.Params) interface{} {
					list := params.Value().([]interface{})
					list = append(list, "contact update")
					list = append(list, "one more...")

					return <-context.Call("user.update", list)
				},
			},
		},
	})
}

func addProfileService(bkr *broker.ServiceBroker) {
	bkr.AddService(moleculer.Service{
		Name: "profile",
		Actions: []moleculer.Action{
			{
				Name: "update",
				Handler: func(context moleculer.Context, params moleculer.Params) interface{} {
					list := params.Value().([]string)
					list = append(list, "profile update")
					list = append(list, "one more...")

					return <-context.Call("contact.update", list)
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

var _ = Describe("Transit", func() {

	brokerDelegates := BrokerDelegates()
	contextA := context.BrokerContext(brokerDelegates)
	logger := contextA.Logger()
	var serializer serializer.Serializer = serializer.CreateJSONSerializer(logger)
	url := "stan://localhost:4222"
	options := nats.StanOptions{
		"MOL",
		url,
		"test-cluster",
		"unit-test-client-id",
		logger,
		serializer,
		func(msg transit.Message) bool {
			return true
		},
	}

	stringSize := 50
	arraySize := 100
	var longArrayOfStrings []string
	for i := 0; i < arraySize; i++ {
		randomString := util.RandomString(stringSize)
		longArrayOfStrings = append(longArrayOfStrings, randomString)
	}

	It("Should create multiple brokers, connect and disconnect and make sure stan resources are closed/released properly.", func() {

		brokersLoop := 100
		for i := 0; i < brokersLoop; i++ {
			brokr1 := broker.FromConfig(&moleculer.BrokerConfig{
				LogLevel:    "INFO",
				Transporter: "STAN",
			})
			addUserService(brokr1)
			brokr1.Start()

			brokr2 := broker.FromConfig(&moleculer.BrokerConfig{
				LogLevel:    "INFO",
				Transporter: "STAN",
			})
			addContactService(brokr2)
			brokr2.Start()

			brokr3 := broker.FromConfig(&moleculer.BrokerConfig{
				LogLevel:    "INFO",
				Transporter: "STAN",
			})
			addProfileService(brokr3)
			brokr3.Start()

			result := <-brokr3.Call("profile.update", longArrayOfStrings)
			newList := result.([]interface{})
			Expect(len(newList)).Should(Equal(arraySize + 6))

			stopBrokers(brokr1, brokr2, brokr3)
		}

	})

	It("Should connect, subscribe, publish and disconnect", func() {
		params := map[string]string{
			"name":     "John",
			"lastName": "Snow",
		}

		actionName := "some.service.action"
		actionContext := contextA.NewActionContext(actionName, params)

		transporter := nats.CreateStanTransporter(options)
		<-transporter.Connect()

		received := make(chan bool)
		transporter.Subscribe("topicA", "node1", func(message transit.Message) {

			contextMap := serializer.MessageToContextMap(message)
			newContext := context.RemoteActionContext(brokerDelegates, contextMap)
			Expect(newContext.ActionName()).Should(Equal(actionName))
			contextParams := newContext.Params()
			Expect(contextParams.String("name")).Should(Equal("John"))
			Expect(contextParams.String("lastName")).Should(Equal("Snow"))

			received <- true
		})

		contextMap := actionContext.AsMap()
		contextMap["sender"] = "someone"
		msg, _ := serializer.MapToMessage(&contextMap)
		transporter.Publish("topicA", "node1", msg)

		Expect(<-received).Should(Equal(true))

		<-transporter.Disconnect()

	})

})
