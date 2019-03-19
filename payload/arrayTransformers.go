package payload

import (
	"time"

	"github.com/moleculer-go/moleculer"
)

type interfaceArrayFunc func(source *interface{}) []interface{}
type arrayLenFunc func(source *interface{}) int

type arrayTransformer struct {
	name           string
	InterfaceArray interfaceArrayFunc
	ArrayLen       arrayLenFunc
}

var arrayTransformers = []arrayTransformer{
	arrayTransformer{
		"[]interface {}",
		func(source *interface{}) []interface{} {
			return (*source).([]interface{})
		},
		func(source *interface{}) int {
			return len((*source).([]interface{}))
		},
	},
	arrayTransformer{
		"[]string",
		func(source *interface{}) []interface{} {
			sourceList := (*source).([]string)
			result := make([]interface{}, len(sourceList))
			for index, value := range sourceList {
				result[index] = value
			}
			return result
		},
		func(source *interface{}) int {
			return len((*source).([]string))
		},
	},
	arrayTransformer{
		"[]int",
		func(source *interface{}) []interface{} {
			sourceList := (*source).([]int)
			result := make([]interface{}, len(sourceList))
			for index, value := range sourceList {
				result[index] = value
			}
			return result
		},
		func(source *interface{}) int {
			return len((*source).([]int))
		},
	},
	arrayTransformer{
		"[]bool",
		func(source *interface{}) []interface{} {
			sourceList := (*source).([]bool)
			result := make([]interface{}, len(sourceList))
			for index, value := range sourceList {
				result[index] = value
			}
			return result
		},
		func(source *interface{}) int {
			return len((*source).([]bool))
		},
	},
	arrayTransformer{
		"[]int64",
		func(source *interface{}) []interface{} {
			sourceList := (*source).([]int64)
			result := make([]interface{}, len(sourceList))
			for index, value := range sourceList {
				result[index] = value
			}
			return result
		},
		func(source *interface{}) int {
			return len((*source).([]int64))
		},
	},
	arrayTransformer{
		"[]float32",
		func(source *interface{}) []interface{} {
			sourceList := (*source).([]float32)
			result := make([]interface{}, len(sourceList))
			for index, value := range sourceList {
				result[index] = value
			}
			return result
		},
		func(source *interface{}) int {
			return len((*source).([]float32))
		},
	},
	arrayTransformer{
		"[]float64",
		func(source *interface{}) []interface{} {
			sourceList := (*source).([]float64)
			result := make([]interface{}, len(sourceList))
			for index, value := range sourceList {
				result[index] = value
			}
			return result
		},
		func(source *interface{}) int {
			return len((*source).([]float64))
		},
	},
	arrayTransformer{
		"[]uint64",
		func(source *interface{}) []interface{} {
			sourceList := (*source).([]uint64)
			result := make([]interface{}, len(sourceList))
			for index, value := range sourceList {
				result[index] = value
			}
			return result
		},
		func(source *interface{}) int {
			return len((*source).([]uint64))
		},
	},
	arrayTransformer{
		"[]time.Time",
		func(source *interface{}) []interface{} {
			sourceList := (*source).([]time.Time)
			result := make([]interface{}, len(sourceList))
			for index, value := range sourceList {
				result[index] = value
			}
			return result
		},
		func(source *interface{}) int {
			return len((*source).([]time.Time))
		},
	},
	arrayTransformer{
		"[]map[string]interface {}",
		func(source *interface{}) []interface{} {
			sourceList := (*source).([]map[string]interface{})
			result := make([]interface{}, len(sourceList))
			for index, value := range sourceList {
				result[index] = value
			}
			return result
		},
		func(source *interface{}) int {
			return len((*source).([]map[string]interface{}))
		},
	},
	arrayTransformer{
		"[]moleculer.Payload",
		func(source *interface{}) []interface{} {
			sourceList := (*source).([]moleculer.Payload)
			result := make([]interface{}, len(sourceList))
			for index, value := range sourceList {
				result[index] = value.Value()
			}
			return result
		},
		func(source *interface{}) int {
			return len((*source).([]moleculer.Payload))
		},
	},
}

// MapTransformer : return the map transformer for the specific map type
func ArrayTransformer(value *interface{}) *arrayTransformer {
	valueType := GetValueType(value)
	for _, transformer := range arrayTransformers {
		if valueType == transformer.name {
			return &transformer
		}
	}
	//fmt.Println("ArrayTransformer() no transformer for  valueType -> ", valueType)
	return nil
}
