package serializer_test

import (
	"os"
	"time"

	"github.com/moleculer-go/cupaloy/v2"
	"github.com/moleculer-go/moleculer"

	"github.com/moleculer-go/moleculer/context"
	"github.com/moleculer-go/moleculer/payload"
	"github.com/moleculer-go/moleculer/serializer"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	log "github.com/sirupsen/logrus"
)

var snap = cupaloy.New(cupaloy.FailOnUpdate(os.Getenv("UPDATE_SNAPSHOTS") == "true"))

var _ = Describe("JSON Serializer", func() {

	brokerDelegates := BrokerDelegates("test-node")
	contextA := context.BrokerContext(brokerDelegates)

	It("Remove should remove fields from the payload", func() {

		serial := serializer.CreateJSONSerializer(log.WithField("unit", "test"))
		p, _ := serial.MapToPayload(&map[string]interface{}{
			"name":     "John",
			"lastname": "Snow",
			"faction":  "Stark",
			"Winter":   "is coming!",
		})

		Expect(snap.SnapshotMulti("Remove()", p.Remove("Winter", "name"))).ShouldNot(HaveOccurred())
	})

	It("Bson should return a bson map", func() {

		serial := serializer.CreateJSONSerializer(log.WithField("unit", "test"))
		p, _ := serial.MapToPayload(&map[string]interface{}{
			"name":     "John",
			"lastname": "Snow",
			"faction":  "Stark",
			"Winter":   "is coming!",
		})

		bs := p.Bson()

		Expect(snap.SnapshotMulti("Bson()", bs)).ShouldNot(HaveOccurred())
	})

	It("Add should add fields to payload", func() {

		serial := serializer.CreateJSONSerializer(log.WithField("unit", "test"))
		p, _ := serial.MapToPayload(&map[string]interface{}{
			"name":     "John",
			"lastname": "Snow",
			"faction":  "Stark",
			"Winter":   "is coming!",
		})

		m := p.AddMany(map[string]interface{}{
			"page":     1,
			"pageSize": 15,
		})

		Expect(snap.SnapshotMulti("Add()", m)).ShouldNot(HaveOccurred())
	})

	It("Should handle each return type", func() {
		logger := log.WithField("serializer", "JSON")
		serializer := serializer.CreateJSONSerializer(logger)

		json := []byte(`{"name":{"first":"Janet","last":"Prichard"},"age":47}`)
		message := serializer.BytesToPayload(&json)

		Expect(message.Get("name").IsMap()).Should(Equal(true))
		Expect(message.Get("name").Map()["first"].String()).Should(Equal("Janet"))
		Expect(message.Get("name").Get("first").String()).Should(Equal("Janet"))
		Expect(message.Get("name").Get("last").String()).Should(Equal("Prichard"))

		Expect(message.Get("age").Int()).Should(Equal(47))
		Expect(message.Get("age").Int64()).Should(Equal(int64(47)))
		Expect(message.Get("age").Float32()).Should(Equal(float32(47)))
		Expect(message.Get("age").Float()).Should(Equal(float64(47)))
		Expect(message.Get("age").Uint()).Should(Equal(uint64(47)))

		Expect(message.Get("age").StringArray()).Should(BeNil())
		Expect(message.Get("age").IntArray()).Should(BeNil())
		Expect(message.Get("age").Int64Array()).Should(BeNil())
		Expect(message.Get("age").Float32Array()).Should(BeNil())
		Expect(message.Get("age").FloatArray()).Should(BeNil())
		Expect(message.Get("age").UintArray()).Should(BeNil())

		json = []byte(`["first", "second", "third"]`)
		message = serializer.BytesToPayload(&json)

		Expect(message.IsArray()).Should(Equal(true))
		Expect(message.StringArray()).Should(Equal([]string{"first", "second", "third"}))
		Expect(message.ValueArray()).Should(Equal([]interface{}{"first", "second", "third"}))

		json = []byte(`{"list":["first", "second", "third"]}`)
		message = serializer.BytesToPayload(&json)

		Expect(message.Get("list").IsArray()).Should(Equal(true))
		Expect(message.Get("list").StringArray()).Should(Equal([]string{"first", "second", "third"}))
		Expect(message.Get("list").ValueArray()).Should(Equal([]interface{}{"first", "second", "third"}))
		Expect(message.Get("list").RawMap()).Should(BeNil())
		var items []string
		message.Get("list").ForEach(func(key interface{}, payload moleculer.Payload) bool {
			items = append(items, payload.String())
			return true
		})
		Expect(items).Should(Equal([]string{"first", "second", "third"}))

		items = make([]string, 0)
		message.Get("list").ForEach(func(key interface{}, payload moleculer.Payload) bool {
			items = append(items, payload.String())
			return false
		})
		Expect(items).Should(Equal([]string{"first"}))

		json = []byte(`10`)
		message = serializer.BytesToPayload(&json)
		Expect(message.IsArray()).Should(BeFalse())
		Expect(message.Int()).Should(Equal(10))
		Expect(message.Int64()).Should(Equal(int64(10)))
		Expect(message.Float()).Should(Equal(float64(10)))
		Expect(message.Float32()).Should(Equal(float32(10)))
		Expect(message.Uint()).Should(Equal(uint64(10)))

		json = []byte(`[10, 40, 50]`)
		message = serializer.BytesToPayload(&json)

		Expect(message.IsArray()).Should(Equal(true))
		Expect(message.IntArray()).Should(Equal([]int{10, 40, 50}))
		Expect(message.Int64Array()).Should(Equal([]int64{10, 40, 50}))
		Expect(message.FloatArray()).Should(Equal([]float64{10, 40, 50}))
		Expect(message.Float32Array()).Should(Equal([]float32{10, 40, 50}))
		Expect(message.UintArray()).Should(Equal([]uint64{10, 40, 50}))

		json = []byte(`["2006-01-02T15:04:05Z", "2007-01-02T15:04:05Z", "2008-01-02T15:04:05Z"]`)
		message = serializer.BytesToPayload(&json)
		Expect(message.IsArray()).Should(Equal(true))
		Expect(len(message.Array())).Should(Equal(3))
		Expect(message.Array()[0].Value()).Should(Equal("2006-01-02T15:04:05Z"))
		Expect(message.Array()[1].Value()).Should(Equal("2007-01-02T15:04:05Z"))
		Expect(message.Array()[2].Value()).Should(Equal("2008-01-02T15:04:05Z"))

		json = []byte(`{"list":[10, 40, 50],"times":["2006-01-02T15:04:05Z", "2007-01-02T15:04:05Z", "2008-01-02T15:04:05Z"]}`)
		message = serializer.BytesToPayload(&json)

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

		json = []byte(`{"list":[true, true, false],"verdade":true}`)
		message = serializer.BytesToPayload(&json)
		Expect(message.Get("list").BoolArray()).Should(BeEquivalentTo([]bool{true, true, false}))
		Expect(message.Get("verdade").Bool()).Should(Equal(true))

		json = []byte(`{"time":"2016-01-02T15:04:05Z"}`)
		message = serializer.BytesToPayload(&json)
		t, _ = time.Parse(time.RFC3339, "2016-01-02T15:04:05Z")
		Expect(message.Get("time").Time()).Should(BeEquivalentTo(t))
		Expect(message.IsError()).Should(Equal(false))
		Expect(message.Error()).Should(BeNil())

		json = []byte(`{"error":"shit happened!"}`)
		message = serializer.BytesToPayload(&json)
		Expect(message.IsError()).Should(Equal(true))
		Expect(message.Error().Error()).Should(BeEquivalentTo("shit happened!"))

		json = []byte(`{"rootMap":{"text":"shit happened!", "textList":["item1", "item2"], "objList":[{"subMap":{"prop1":"value"}}, {"name":"john", "list":[1,2,3]}] }`)
		message = serializer.BytesToPayload(&json)
		Expect(message.Error()).Should(BeNil())

		Expect(snap.SnapshotMulti("RawMap()", message.RawMap())).ShouldNot(HaveOccurred())
	})

	It("Should convert between context and Transit Message", func() {
		logger := log.WithField("serializer", "JSON")
		serializer := serializer.CreateJSONSerializer(logger)

		actionName := "some.service.action"
		params := map[string]string{
			"name":     "John",
			"lastName": "Snow",
		}
		actionContext := contextA.ChildActionContext(actionName, payload.New(params))

		contextMap := actionContext.AsMap()
		contextMap["sender"] = "original_sender"
		message, _ := serializer.MapToPayload(&contextMap)

		Expect(message.Get("action").String()).Should(Equal(actionName))
		Expect(message.Get("params.name").String()).Should(Equal("John"))
		Expect(message.Get("params.lastName").String()).Should(Equal("Snow"))

		values := serializer.PayloadToContextMap(message)
		contextAgain := context.ActionContext(brokerDelegates, values)

		Expect(contextAgain.TargetNodeID()).Should(Equal("original_sender"))
		Expect(contextAgain.ActionName()).Should(Equal(actionName))
		Expect(contextAgain.Payload().Get("name").String()).Should(Equal("John"))
		Expect(contextAgain.Payload().Get("lastName").String()).Should(Equal("Snow"))

	})
})
