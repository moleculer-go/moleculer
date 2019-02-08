package payload

import "time"

type interfaceArrayFunc func(source *interface{}) []interface{}

type arrayTransformer struct {
	name           string
	interfaceArray interfaceArrayFunc
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
}

// getMapTransformer : return the map transformer for the specific map type
func getArrayTransformer(value *interface{}) *arrayTransformer {
	valueType := getValueType(value)
	for _, transformer := range arrayTransformers {
		if valueType == transformer.name {
			return &transformer
		}
	}
	return nil
}
