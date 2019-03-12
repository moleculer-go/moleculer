package payload

import (
	"time"
)

type interfaceArrayFunc func(source *interface{}) []interface{}

type arrayTransformer struct {
	name           string
	InterfaceArray interfaceArrayFunc
}

var arrayTransformers = []arrayTransformer{
	arrayTransformer{
		"[]interface {}",
		func(source *interface{}) []interface{} {
			return (*source).([]interface{})
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
