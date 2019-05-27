package payload_test

import (
	"errors"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/moleculer-go/cupaloy/v2"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/moleculer-go/moleculer"
	. "github.com/moleculer-go/moleculer/payload"
)

var snap = cupaloy.New(cupaloy.FailOnUpdate(os.Getenv("UPDATE_SNAPSHOTS") == "true"))

var _ = Describe("Payload", func() {

	It("Remove should remove fields from the payload and return a new copy", func() {

		p := New(map[string]string{
			"name":     "John",
			"lastname": "Snow",
			"faction":  "Stark",
			"Winter":   "is coming!",
		})

		Expect(snap.SnapshotMulti("Remove()", p.Remove("Winter", "name"))).ShouldNot(HaveOccurred())
	})

	It("Bson should return a bson map", func() {

		p := New(map[string]string{
			"name":     "John",
			"lastname": "Snow",
			"faction":  "Stark",
			"Winter":   "is coming!",
		})

		bs := p.Bson()

		Expect(snap.SnapshotMulti("Bson()", bs)).ShouldNot(HaveOccurred())
	})

	It("Add should add fields to payload", func() {

		p := New(map[string]string{
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

	type customMap map[string]interface{}
	type customArray []map[string]interface{}

	It("should deal with custom map types", func() {
		p := New(customMap{
			"name":     "John",
			"lastname": "Snow",
			"sub": customMap{
				"level": 1,
				"sub": customMap{
					"level": 2,
				},
				"subList": customArray{
					customMap{
						"level": 3,
						"name":  "sub item inside custom array",
					},
				},
			},
		})
		Expect(snap.SnapshotMulti("CustomMap()-RawMap()", p.RawMap())).ShouldNot(HaveOccurred())
		Expect(snap.SnapshotMulti("CustomMap()-Bson()", p.Bson())).ShouldNot(HaveOccurred())
	})

	It("should deal with bson.M map types", func() {
		p := New(bson.M{
			"name":     "John",
			"lastname": "Snow",
			"sub": bson.M{
				"level": 1,
				"sub": bson.M{
					"level": 2,
				},
				"subList": []bson.M{
					bson.M{
						"level": 3,
						"name":  "sub item inside custom array",
					},
				},
			},
		})
		Expect(snap.SnapshotMulti("Bson-values", p.Bson())).ShouldNot(HaveOccurred())

	})

	It("Should convert numbers correctly", func() {

		Expect(New(true).Bool()).Should(BeTrue())
		Expect(New("true").Bool()).Should(BeTrue())

		Expect(New(false).Bool()).Should(BeFalse())
		Expect(New("false").Bool()).Should(BeFalse())
		Expect(New("anything else").Bool()).Should(BeFalse())
		Expect(New("").Bool()).Should(BeFalse())

		Expect(New("10").Int()).Should(Equal(int(10)))
		Expect(New("10").Int64()).Should(Equal(int64(10)))
		Expect(New("10").Float32()).Should(Equal(float32(10)))
		Expect(New("10").Float()).Should(Equal(float64(10)))
		Expect(New("10").Uint()).Should(Equal(uint64(10)))

		Expect(New(10).Int64()).Should(Equal(int64(10)))
		Expect(New(10).Float32()).Should(Equal(float32(10)))
		Expect(New(10).Float()).Should(Equal(float64(10)))
		Expect(New(10).Uint()).Should(Equal(uint64(10)))

		Expect(New(int64(30)).Int()).Should(Equal(30))
		Expect(New(int64(30)).Float32()).Should(Equal(float32(30)))
		Expect(New(int64(30)).Float()).Should(Equal(float64(30)))
		Expect(New(int64(30)).Uint()).Should(Equal(uint64(30)))

		Expect(New(float64(60)).Int()).Should(Equal(60))
		Expect(New(float64(60)).Float32()).Should(Equal(float32(60)))
		Expect(New(float64(60)).Int64()).Should(Equal(int64(60)))
		Expect(New(float64(60)).Uint()).Should(Equal(uint64(60)))

		Expect(New(float32(120)).Int()).Should(Equal(120))
		Expect(New(float32(120)).Float()).Should(Equal(float64(120)))
		Expect(New(float32(120)).Int64()).Should(Equal(int64(120)))
		Expect(New(float32(120)).Uint()).Should(Equal(uint64(120)))

		Expect(New(uint64(240)).Int()).Should(Equal(240))
		Expect(New(uint64(240)).Float()).Should(Equal(float64(240)))
		Expect(New(uint64(240)).Int64()).Should(Equal(int64(240)))
		Expect(New(uint64(240)).Float32()).Should(Equal(float32(240)))

	})

	It("Should create Payload with map and return values correctly", func() {

		var source interface{} = map[string]int{
			"height": 150,
			"width":  230,
		}
		params := New(source)
		Expect(params.Get("height").Int()).Should(Equal(150))
		Expect(params.Get("width").Int()).Should(Equal(230))

		source = map[string]string{
			"name": "John",
			"word": "Snow",
		}
		params = New(source)
		Expect(params.Get("name").Value()).Should(Equal("John"))
		Expect(params.Get("word").String()).Should(Equal("Snow"))

		var lHeight int64 = 345356436
		var lWidth int64 = 5623453254123
		source = map[string]int64{
			"height": lHeight,
			"width":  lWidth,
		}
		params = New(source)
		Expect(params.Get("height").Int64()).Should(Equal(lHeight))
		Expect(params.Get("width").Int64()).Should(Equal(lWidth))

		var f32Height float32 = 345356436.5623453254123
		var f32Width float32 = 5623453254123.345356436
		source = map[string]float32{
			"height": f32Height,
			"width":  f32Width,
		}
		params = New(source)
		Expect(params.Get("height").Float32()).Should(Equal(f32Height))
		Expect(params.Get("width").Float32()).Should(Equal(f32Width))

		var f64Height float64 = 345356436.5623453254123
		var f64Width float64 = 5623453254123.345356436
		source = map[string]float64{
			"height": f64Height,
			"width":  f64Width,
		}
		params = New(source)
		Expect(params.Get("height").Float()).Should(Equal(f64Height))
		Expect(params.Get("width").Float()).Should(Equal(f64Width))

		timeArray := []time.Time{time.Now(), time.Now().Local(), time.Now().UTC()}
		source = map[string]interface{}{
			"string":  "Hellow Night!",
			"int":     12345678910,
			"int64":   lHeight,
			"float32": f32Height,
			"float64": f64Height,
			"map": map[string]string{
				"sub1": "value-sub1",
				"sub2": "value-sub2",
			},
			"stringArray":  []string{"value1", "value2", "value3"},
			"intArray":     []int{10, 20, 30},
			"int64Array":   []int64{100, 200, 300},
			"float32Array": []float32{100.45, 200.56, 300.67},
			"float64Array": []float64{100.45, 200.56, 300.67},
			"uintArray":    []uint64{1000, 2000, 3000},
			"valueArray":   []interface{}{"value1", 20, 25.5},
			"timeArray":    timeArray,
			"boolArray":    []bool{true, false, true},
		}
		params = New(source)
		Expect(params.Get("notFound").Value()).Should(BeNil())
		Expect(params.Get("string").String()).Should(Equal("Hellow Night!"))

		rawMap := make(map[string]interface{})
		for key, value := range params.RawMap() {
			if key == "timeArray" {
				continue
			}
			rawMap[key] = value
		}
		Expect(snap.SnapshotMulti("RawMap()", rawMap)).ShouldNot(HaveOccurred())

		moreOfTheSame := New(params)
		Expect(moreOfTheSame.Get("notFound").Value()).Should(BeNil())
		Expect(moreOfTheSame.Get("string").String()).Should(Equal("Hellow Night!"))

		Expect(params.Get("stringArray").StringArray()).Should(Equal([]string{"value1", "value2", "value3"}))
		Expect(New([]string{"value1", "value2", "value3"}).StringArray()).Should(Equal([]string{"value1", "value2", "value3"}))
		Expect(New(map[string]string{"key1": "value1", "key2": "value2"}).RawMap()).Should(BeEquivalentTo(map[string]interface{}{"key1": "value1", "key2": "value2"}))

		Expect(params.Get("intArray").IntArray()).Should(BeEquivalentTo([]int{10, 20, 30}))
		Expect(New([]int{10, 20, 30}).IntArray()).Should(Equal([]int{10, 20, 30}))
		Expect(New(map[string]int{"key1": 1, "key2": 2}).RawMap()).Should(BeEquivalentTo(map[string]interface{}{"key1": 1, "key2": 2}))

		Expect(params.Get("boolArray").BoolArray()).Should(BeEquivalentTo([]bool{true, false, true}))
		Expect(New([]int{10, 20, 30}).IntArray()).Should(Equal([]int{10, 20, 30}))
		Expect(New([]int{10, 20, 30}).IsArray()).Should(Equal(true))

		Expect(params.Get("int64Array").Int64Array()).Should(BeEquivalentTo([]int64{100, 200, 300}))
		Expect(New([]int64{100, 200, 300}).Int64Array()).Should(Equal([]int64{100, 200, 300}))
		Expect(New(map[string]int64{"key1": 1, "key2": 2}).RawMap()).Should(BeEquivalentTo(map[string]interface{}{"key1": int64(1), "key2": int64(2)}))

		Expect(params.Get("float64Array").FloatArray()).Should(BeEquivalentTo([]float64{100.45, 200.56, 300.67}))
		Expect(New([]float64{100.45, 200.56, 300.67}).FloatArray()).Should(Equal([]float64{100.45, 200.56, 300.67}))
		Expect(New(map[string]float64{"key1": 100.45, "key2": 200.56}).RawMap()).Should(BeEquivalentTo(map[string]interface{}{"key1": float64(100.45), "key2": float64(200.56)}))

		Expect(params.Get("float32Array").Float32Array()).Should(BeEquivalentTo([]float32{100.45, 200.56, 300.67}))
		Expect(New([]float32{100.45, 200.56, 300.67}).Float32Array()).Should(Equal([]float32{100.45, 200.56, 300.67}))
		Expect(New(map[string]float32{"key1": 100.45, "key2": 200.56}).RawMap()).Should(BeEquivalentTo(map[string]interface{}{"key1": float32(100.45), "key2": float32(200.56)}))

		Expect(params.Get("uintArray").UintArray()).Should(BeEquivalentTo([]uint64{1000, 2000, 3000}))
		Expect(New([]uint64{1000, 2000, 3000}).UintArray()).Should(Equal([]uint64{1000, 2000, 3000}))
		Expect(New(map[string]uint64{"key1": 1, "key2": 2}).RawMap()).Should(BeEquivalentTo(map[string]interface{}{"key1": uint64(1), "key2": uint64(2)}))

		Expect(params.Get("valueArray").ValueArray()).Should(BeEquivalentTo([]interface{}{"value1", 20, 25.5}))
		Expect(params.Get("timeArray").TimeArray()).Should(BeEquivalentTo(timeArray))
		now := time.Now()
		Expect(New(map[string]time.Time{"key1": now, "key2": now}).RawMap()).Should(BeEquivalentTo(map[string]interface{}{"key1": now, "key2": now}))

		Expect(params.Get("int").Int()).Should(Equal(12345678910))
		Expect(params.Get("int64").Int64()).Should(Equal(lHeight))
		Expect(params.Get("float32").Float32()).Should(Equal(f32Height))
		Expect(params.Get("float64").Float()).Should(Equal(f64Height))
		Expect(params.Get("map").Map()["sub1"].String()).Should(Equal("value-sub1"))
		Expect(params.Get("map").Map()["sub2"].String()).Should(Equal("value-sub2"))

		var items []string
		params.Get("stringArray").ForEach(func(key interface{}, payload moleculer.Payload) bool {
			items = append(items, payload.String())
			return true
		})
		Expect(items).Should(Equal([]string{"value1", "value2", "value3"}))

		items = make([]string, 0)
		params.Get("stringArray").ForEach(func(key interface{}, payload moleculer.Payload) bool {
			items = append(items, payload.String())
			return false
		})
		Expect(items).Should(Equal([]string{"value1"}))

		mapItems := make(map[string]string)
		params.Get("map").ForEach(func(key interface{}, payload moleculer.Payload) bool {
			mapItems[key.(string)] = payload.String()
			return true
		})
		Expect(mapItems).Should(Equal(map[string]string{
			"sub1": "value-sub1",
			"sub2": "value-sub2",
		}))

		mapItems = make(map[string]string)
		params.Get("map").ForEach(func(key interface{}, payload moleculer.Payload) bool {
			mapItems[key.(string)] = payload.String()
			return false
		})
		Expect(len(mapItems)).Should(Equal(1))

		Expect(params.Error()).Should(BeNil())

		Expect(params.Get("string").StringArray()).Should(BeNil())
		Expect(params.Get("string").IntArray()).Should(BeNil())
		Expect(params.Get("string").Int64Array()).Should(BeNil())
		Expect(params.Get("string").FloatArray()).Should(BeNil())
		Expect(params.Get("string").Float32Array()).Should(BeNil())
		Expect(params.Get("string").ValueArray()).Should(BeNil())
		Expect(params.Get("string").UintArray()).Should(BeNil())
		Expect(params.Get("string").BoolArray()).Should(BeNil())

		Expect(params.Exists()).Should(Equal(true))
		Expect(New(nil).Exists()).Should(Equal(false))

		someErrror := errors.New("some error")
		params = New(someErrror)
		Expect(params.IsError()).Should(Equal(true))
		Expect(params.Error()).Should(Equal(someErrror))
	})

})
