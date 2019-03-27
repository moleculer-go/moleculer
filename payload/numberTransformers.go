package payload

import (
	"fmt"
	"strconv"
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

func stringToFloat64(value string) float64 {
	fvalue, err := strconv.ParseFloat(value, 64)
	if err != nil {
		panic(fmt.Sprint("Could not convert string: ", value, " to a number!"))
	}
	return fvalue
}

var numberTransformers = []numberTransformer{
	{
		name: "string",
		toInt: func(source *interface{}) int {
			return int(stringToFloat64((*source).(string)))
		},
		toInt64: func(source *interface{}) int64 {
			return int64(stringToFloat64((*source).(string)))
		},
		toFloat32: func(source *interface{}) float32 {
			return float32(stringToFloat64((*source).(string)))
		},
		toFloat64: func(source *interface{}) float64 {
			return float64(stringToFloat64((*source).(string)))
		},
		toUint64: func(source *interface{}) uint64 {
			return uint64(stringToFloat64((*source).(string)))
		},
	},
	{
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
	{
		name: "int32",
		toInt: func(source *interface{}) int {
			return int((*source).(int32))
		},
		toInt64: func(source *interface{}) int64 {
			return int64((*source).(int32))
		},
		toFloat32: func(source *interface{}) float32 {
			return float32((*source).(int32))
		},
		toFloat64: func(source *interface{}) float64 {
			return float64((*source).(int32))
		},
		toUint64: func(source *interface{}) uint64 {
			return uint64((*source).(int32))
		},
	},
	{
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
	{
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
	{
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
	{
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

// MapTransformer : return the map transformer for the specific map type
func getNumberTransformer(value *interface{}) *numberTransformer {
	valueType := GetValueType(value)
	for _, transformer := range numberTransformers {
		if valueType == transformer.name {
			return &transformer
		}
	}
	fmt.Println("[WARN] getNumberTransformer() no transformer for  valueType -> ", valueType)
	return nil
}
