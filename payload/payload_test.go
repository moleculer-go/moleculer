package payload_test

import (
	"errors"

	test "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/moleculer-go/moleculer/payload"
)

var _ = test.Describe("Payload", func() {
	test.It("Should create Payload with map and return values correctly", func() {

		var source interface{} = map[string]int{
			"height": 150,
			"width":  230,
		}
		params := Create(source)
		Expect(params.Get("height").Int()).Should(Equal(150))
		Expect(params.Get("width").Int()).Should(Equal(230))

		source = map[string]string{
			"name": "John",
			"word": "Snow",
		}
		params = Create(source)
		Expect(params.Get("name").Value()).Should(Equal("John"))
		Expect(params.Get("word").String()).Should(Equal("Snow"))

		var lHeight int64 = 345356436
		var lWidth int64 = 5623453254123
		source = map[string]int64{
			"height": lHeight,
			"width":  lWidth,
		}
		params = Create(source)
		Expect(params.Get("height").Int64()).Should(Equal(lHeight))
		Expect(params.Get("width").Int64()).Should(Equal(lWidth))

		var f32Height float32 = 345356436.5623453254123
		var f32Width float32 = 5623453254123.345356436
		source = map[string]float32{
			"height": f32Height,
			"width":  f32Width,
		}
		params = Create(source)
		Expect(params.Get("height").Float32()).Should(Equal(f32Height))
		Expect(params.Get("width").Float32()).Should(Equal(f32Width))

		var f64Height float64 = 345356436.5623453254123
		var f64Width float64 = 5623453254123.345356436
		source = map[string]float64{
			"height": f64Height,
			"width":  f64Width,
		}
		params = Create(source)
		Expect(params.Get("height").Float()).Should(Equal(f64Height))
		Expect(params.Get("width").Float()).Should(Equal(f64Width))

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
		}
		params = Create(source)
		Expect(params.Get("string").String()).Should(Equal("Hellow Night!"))
		Expect(params.Get("int").Int()).Should(Equal(12345678910))
		Expect(params.Get("int64").Int64()).Should(Equal(lHeight))
		Expect(params.Get("float32").Float32()).Should(Equal(f32Height))
		Expect(params.Get("float64").Float()).Should(Equal(f64Height))
		Expect(params.Get("map").Map()["sub1"].String()).Should(Equal("value-sub1"))
		Expect(params.Get("map").Map()["sub2"].String()).Should(Equal("value-sub2"))

		someErrror := errors.New("some error")
		params = Create(someErrror)
		Expect(params.IsError()).Should(Equal(true))
		Expect(params.Error()).Should(Equal(someErrror))
	})

})
