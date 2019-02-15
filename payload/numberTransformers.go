package payload

import (
	"fmt"
)

type toIntFunc func(source *interface{}) int
type toInt64Func func(source *interface{}) int64
type toFloat32Func func(source *interface{}) float32
type toFloat64Func func(source *interface{}) float64

type toUint64Func func(source *interface{}) uint64

type numberTransformer struct {
	name    string
	toInt   toIntFunc
	toInt64 toInt64Func

	toFloat32 toFloat32Func
	toFloat64 toFloat64Func

	toUint64 toUint64Func
}

var numberTransformers = []numberTransformer{
	numberTransformer{
		name: "int",
		toInt: func(source *interface{}) int {
			return (*source).(int)
		},
		toInt64: func(source *interface{}) int64 {
			return int64((*source).(int))
		},
		toFloat32: func(source *interface{}) float32 {
			return float32((*source).(int))
		},
		toFloat64: func(source *interface{}) float64 {
			return float64((*source).(int))
		},
		toUint64: func(source *interface{}) uint64 {
			return uint64((*source).(int))
		},
	},
	numberTransformer{
		name: "int64",
		toInt: func(source *interface{}) int {
			return int((*source).(int64))
		},
		toInt64: func(source *interface{}) int64 {
			return (*source).(int64)
		},
		toFloat32: func(source *interface{}) float32 {
			return float32((*source).(int64))
		},
		toFloat64: func(source *interface{}) float64 {
			return float64((*source).(int64))
		},
		toUint64: func(source *interface{}) uint64 {
			return uint64((*source).(int64))
		},
	},
	numberTransformer{
		name: "float32",
		toInt: func(source *interface{}) int {
			return int((*source).(float32))
		},
		toInt64: func(source *interface{}) int64 {
			return int64((*source).(float32))
		},
		toFloat32: func(source *interface{}) float32 {
			return (*source).(float32)
		},
		toFloat64: func(source *interface{}) float64 {
			return float64((*source).(float32))
		},
		toUint64: func(source *interface{}) uint64 {
			return uint64((*source).(float32))
		},
	},
	numberTransformer{
		name: "float64",
		toInt: func(source *interface{}) int {
			return int((*source).(float64))
		},
		toInt64: func(source *interface{}) int64 {
			return int64((*source).(float64))
		},
		toFloat32: func(source *interface{}) float32 {
			return float32((*source).(float64))
		},
		toFloat64: func(source *interface{}) float64 {
			return (*source).(float64)
		},
		toUint64: func(source *interface{}) uint64 {
			return uint64((*source).(float64))
		},
	},
	numberTransformer{
		name: "uint64",
		toInt: func(source *interface{}) int {
			return int((*source).(uint64))
		},
		toInt64: func(source *interface{}) int64 {
			return int64((*source).(uint64))
		},
		toFloat32: func(source *interface{}) float32 {
			return float32((*source).(uint64))
		},
		toFloat64: func(source *interface{}) float64 {
			return float64((*source).(uint64))
		},
		toUint64: func(source *interface{}) uint64 {
			return (*source).(uint64)
		},
	},
}

// getMapTransformer : return the map transformer for the specific map type
func getNumberTransformer(value *interface{}) *numberTransformer {
	valueType := getValueType(value)
	for _, transformer := range numberTransformers {
		if valueType == transformer.name {
			return &transformer
		}
	}
	fmt.Println("getNumberTransformer() no transformer for  valueType -> ", valueType)
	return nil
}
