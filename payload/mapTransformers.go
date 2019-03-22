package payload

import (
	"fmt"
	"time"
)

type getFunc func(path string, source *interface{}) (interface{}, bool)
type asMapFunc func(source *interface{}) map[string]interface{}

type mapTransformer struct {
	name  string
	AsMap asMapFunc
}

func (transformer *mapTransformer) get(path string, source *interface{}) (interface{}, bool) {
	sourceAsMap := transformer.AsMap(source)
	value, found := sourceAsMap[path]
	return value, found
}

var mapTransformers = []mapTransformer{
	mapTransformer{
		"map[string]interface {}",
		func(source *interface{}) map[string]interface{} {
			return (*source).(map[string]interface{})
		},
	},
	mapTransformer{
		"map[string]string",
		func(source *interface{}) map[string]interface{} {
			result := make(map[string]interface{})
			for key, value := range (*source).(map[string]string) {
				result[key] = value
			}
			return result
		},
	},
	mapTransformer{
		"map[string]int",
		func(source *interface{}) map[string]interface{} {
			result := make(map[string]interface{})
			for key, value := range (*source).(map[string]int) {
				result[key] = value
			}
			return result
		},
	},
	mapTransformer{
		"map[string]int64",
		func(source *interface{}) map[string]interface{} {
			result := make(map[string]interface{})
			for key, value := range (*source).(map[string]int64) {
				result[key] = value
			}
			return result
		},
	},
	mapTransformer{
		"map[string]uint64",
		func(source *interface{}) map[string]interface{} {
			result := make(map[string]interface{})
			for key, value := range (*source).(map[string]uint64) {
				result[key] = value
			}
			return result
		},
	},
	mapTransformer{
		"map[string]float32",
		func(source *interface{}) map[string]interface{} {
			result := make(map[string]interface{})
			for key, value := range (*source).(map[string]float32) {
				result[key] = value
			}
			return result
		},
	},
	mapTransformer{
		"map[string]float64",
		func(source *interface{}) map[string]interface{} {
			result := make(map[string]interface{})
			for key, value := range (*source).(map[string]float64) {
				result[key] = value
			}
			return result
		},
	},
	mapTransformer{
		"map[string]time.Time",
		func(source *interface{}) map[string]interface{} {
			result := make(map[string]interface{})
			for key, value := range (*source).(map[string]time.Time) {
				result[key] = value
			}
			return result
		},
	},
}

// GetValueType : return a string that represents the map type.
// examples: map[string]int , map[string]string, map[string]float32 and etc
// there are a few possible implementations, Reflection is not very
// popular in GO.. so this uses mt.Sprintf .. but we need to
// test a second version of this with Reflect to check what is faster
func GetValueType(value *interface{}) string {
	return fmt.Sprintf("%T", (*value))
}

// MapTransformer : return the map transformer for the specific map type
func MapTransformer(value *interface{}) *mapTransformer {
	valueType := GetValueType(value)
	for _, transformer := range mapTransformers {
		if valueType == transformer.name {
			return &transformer
		}
	}
	return nil
}
