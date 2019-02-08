package serializer_test

import (
	"time"

	"github.com/moleculer-go/moleculer/context"
	"github.com/moleculer-go/moleculer/payload"
	"github.com/moleculer-go/moleculer/serializer"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	log "github.com/sirupsen/logrus"
)

var _ = Describe("JSON Serializer", func() {

	brokerDelegates := BrokerDelegates("test-node")
	contextA := context.BrokerContext(brokerDelegates)

	It("Should handle each return type", func() {
		logger := log.WithField("serializer", "JSON")
		serializer := serializer.CreateJSONSerializer(logger)

		json := []byte(`{"name":{"first":"Janet","last":"Prichard"},"age":47}`)
		message := serializer.BytesToMessage(&json)

		Expect(message.Get("name").IsMap()).Should(Equal(true))
		Expect(message.Get("name").Get("first").String()).Should(Equal("Janet"))
		Expect(message.Get("name").Get("last").String()).Should(Equal("Prichard"))

		Expect(message.Get("age").Int()).Should(Equal(47))
		Expect(message.Get("age").Int64()).Should(Equal(int64(47)))
		Expect(message.Get("age").Float32()).Should(Equal(float32(47)))
		Expect(message.Get("age").Float()).Should(Equal(float64(47)))
		Expect(message.Get("age").Uint()).Should(Equal(uint64(47)))

		json = []byte(`{"list":["first", "second", "third"]}`)
		message = serializer.BytesToMessage(&json)

		Expect(message.Get("list").IsArray()).Should(Equal(true))
		Expect(message.Get("list").StringArray()).Should(Equal([]string{"first", "second", "third"}))
		Expect(message.Get("list").ValueArray()).Should(Equal([]interface{}{"first", "second", "third"}))

		json = []byte(`{"list":[10, 40, 50],"times":["2006-01-02T15:04:05Z", "2007-01-02T15:04:05Z", "2008-01-02T15:04:05Z"]}`)
		message = serializer.BytesToMessage(&json)

		Expect(message.Get("list").IsArray()).Should(Equal(true))
		Expect(message.Get("list").IntArray()).Should(Equal([]int{10, 40, 50}))
		Expect(message.Get("list").Int64Array()).Should(Equal([]int64{10, 40, 50}))
		Expect(message.Get("list").FloatArray()).Should(Equal([]float64{10, 40, 50}))
		Expect(message.Get("list").Float32Array()).Should(Equal([]float32{10, 40, 50}))
		Expect(message.Get("list").UintArray()).Should(Equal([]uint64{10, 40, 50}))

		Expect(message.Get("times").IsArray()).Should(Equal(true))
		Expect(len(message.Get("times").Array())).Should(Equal(3))
		Expect(message.Get("times").Array()[0].Value()).Should(Equal("2006-01-02T15:04:05Z"))
		Expect(message.Get("times").Array()[1].Value()).Should(Equal("2007-01-02T15:04:05Z"))
		Expect(message.Get("times").Array()[2].Value()).Should(Equal("2008-01-02T15:04:05Z"))
		Expect(message.Get("times").ValueArray()).Should(Equal([]interface{}{"2006-01-02T15:04:05Z", "2007-01-02T15:04:05Z", "2008-01-02T15:04:05Z"}))
		times := make([]time.Time, 3)
		t, _ := time.Parse(time.RFC3339, "2006-01-02T15:04:05Z")
		times[0] = t
		t, _ = time.Parse(time.RFC3339, "2007-01-02T15:04:05Z")
		times[1] = t
		t, _ = time.Parse(time.RFC3339, "2008-01-02T15:04:05Z")
		times[2] = t
		Expect(message.Get("times").TimeArray()).Should(BeEquivalentTo(times))
		Expect(message.Get("times").TimeArray()[0].Year()).Should(Equal(2006))
		Expect(message.Get("times").TimeArray()[1].Year()).Should(Equal(2007))
		Expect(message.Get("times").TimeArray()[2].Year()).Should(Equal(2008))
	})

	It("Should convert between context and Transit Message", func() {
		logger := log.WithField("serializer", "JSON")
		serializer := serializer.CreateJSONSerializer(logger)

		actionName := "some.service.action"
		params := map[string]string{
			"name":     "John",
			"lastName": "Snow",
		}
		actionContext := contextA.NewActionContext(actionName, payload.Create(params))

		contextMap := actionContext.AsMap()
		contextMap["sender"] = "original_sender"
		message, _ := serializer.MapToMessage(&contextMap)

		Expect(message.Get("action").String()).Should(Equal(actionName))
		Expect(message.Get("params.name").String()).Should(Equal("John"))
		Expect(message.Get("params.lastName").String()).Should(Equal("Snow"))

		values := serializer.MessageToContextMap(message)
		contextAgain := context.RemoteActionContext(brokerDelegates, values)

		Expect(contextAgain.TargetNodeID()).Should(Equal("original_sender"))
		Expect(contextAgain.ActionName()).Should(Equal(actionName))
		Expect(contextAgain.Payload().Get("name").String()).Should(Equal("John"))
		Expect(contextAgain.Payload().Get("lastName").String()).Should(Equal("Snow"))

	})
})
