package params_test

import (
	test "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/moleculer-go/moleculer/params"
)

var _ = test.Describe("Params", func() {
	test.It("Should create Params with map and return values correctly", func() {

		var source interface{} = map[string]int{
			"height": 150,
			"width":  230,
		}
		params := CreateParams(&source)
		Expect(params.Int("height")).Should(Equal(150))
		Expect(params.Int("width")).Should(Equal(230))

		source = map[string]string{
			"name": "John",
			"word": "Snow",
		}
		params = CreateParams(&source)
		Expect(params.Get("name")).Should(Equal("John"))
		Expect(params.String("word")).Should(Equal("Snow"))

		var lHeight int64 = 345356436
		var lWidth int64 = 5623453254123
		source = map[string]int64{
			"height": lHeight,
			"width":  lWidth,
		}
		params = CreateParams(&source)
		Expect(params.Int64("height")).Should(Equal(lHeight))
		Expect(params.Int64("width")).Should(Equal(lWidth))

		var f32Height float32 = 345356436.5623453254123
		var f32Width float32 = 5623453254123.345356436
		source = map[string]float32{
			"height": f32Height,
			"width":  f32Width,
		}
		params = CreateParams(&source)
		Expect(params.Float("height")).Should(Equal(f32Height))
		Expect(params.Float("width")).Should(Equal(f32Width))

		var f64Height float64 = 345356436.5623453254123
		var f64Width float64 = 5623453254123.345356436
		source = map[string]float64{
			"height": f64Height,
			"width":  f64Width,
		}
		params = CreateParams(&source)
		Expect(params.Float64("height")).Should(Equal(f64Height))
		Expect(params.Float64("width")).Should(Equal(f64Width))

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
		params = CreateParams(&source)
		Expect(params.Get("string")).Should(Equal("Hellow Night!"))
		Expect(params.Int("int")).Should(Equal(12345678910))
		Expect(params.Int64("int64")).Should(Equal(lHeight))
		Expect(params.Float("float32")).Should(Equal(f32Height))
		Expect(params.Float64("float64")).Should(Equal(f64Height))
		Expect(params.Map("map").Get("sub1")).Should(Equal("value-sub1"))
		Expect(params.Map("map").Get("sub2")).Should(Equal("value-sub2"))
	})

})
