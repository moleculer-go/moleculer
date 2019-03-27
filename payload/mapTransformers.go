package payload

import (
	"fmt"
	"reflect"
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
			result := make(map[string]interface{}, len((*source).(map[string]string)))
			for key, value := range (*source).(map[string]string) {
				result[key] = value
			}
			return result
		},
	},
	mapTransformer{
		"map[string]int",
		func(source *interface{}) map[string]interface{} {
			result := make(map[string]interface{}, len((*source).(map[string]int)))
			for key, value := range (*source).(map[string]int) {
				result[key] = value
			}
			return result
		},
	},
	mapTransformer{
		"map[string]int64",
		func(source *interface{}) map[string]interface{} {
			result := make(map[string]interface{}, len((*source).(map[string]int64)))
			for key, value := range (*source).(map[string]int64) {
				result[key] = value
			}
			return result
		},
	},
	mapTransformer{
		"map[string]uint64",
		func(source *interface{}) map[string]interface{} {
			result := make(map[string]interface{}, len((*source).(map[string]uint64)))
			for key, value := range (*source).(map[string]uint64) {
				result[key] = value
			}
			return result
		},
	},
	mapTransformer{
		"map[string]float32",
		func(source *interface{}) map[string]interface{} {
			result := make(map[string]interface{}, len((*source).(map[string]float32)))
			for key, value := range (*source).(map[string]float32) {
				result[key] = value
			}
			return result
		},
	},
	mapTransformer{
		"map[string]float64",
		func(source *interface{}) map[string]interface{} {
			result := make(map[string]interface{}, len((*source).(map[string]float64)))
			for key, value := range (*source).(map[string]float64) {
				result[key] = value
			}
			return result
		},
	},
	mapTransformer{
		"map[string]time.Time",
		func(source *interface{}) map[string]interface{} {
			result := make(map[string]interface{}, len((*source).(map[string]time.Time)))
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

func rawPayloadMapTransformer(source *interface{}) map[string]interface{} {
	sourcePayload := (*source).(*RawPayload)
	return sourcePayload.RawMap()
}

// reflectionMapTransformer takes a value that is map like and transform into a generic map.
func reflectionMapTransformer(source *interface{}) map[string]interface{} {
	rv := reflect.ValueOf(*source)
	result := make(map[string]interface{}, rv.Len())
	for _, mkey := range rv.MapKeys() {
		item := rv.MapIndex(mkey)
		key := mkey.String()
		value := item.Interface()
		if item.Kind() == reflect.Map {
			mt := MapTransformer(&value)
			result[key] = mt.AsMap(&value)
		} else if item.Kind() == reflect.Array || item.Kind() == reflect.Slice {
			at := ArrayTransformer(&value)
			result[key] = at.InterfaceArray(&value)
		} else {
			result[key] = value
		}
	}
	return result
}

// MapTransformer : return the map transformer for the specific map type
func MapTransformer(value *interface{}) *mapTransformer {

	//try this
	// switch vt := (*value).(type) {
	// case map[string]interface{}:
	// 	//do something.
	// 	fmt.Println("worked vt: ", vt)
	// default:
	// 	//do something else
	// 	fmt.Println("worked also vt: ", vt)
	// }

	valueType := GetValueType(value)
	for _, transformer := range mapTransformers {
		if valueType == transformer.name {
			return &transformer
		}
	}
	if valueType == "*payload.RawPayload" {
		transformer := mapTransformer{
			"*payload.RawPayload",
			rawPayloadMapTransformer,
		}
		return &transformer
	}

	//try to use reflection
	rt := reflect.TypeOf(*value)
	if rt != nil && rt.Kind() == reflect.Map {
		//fmt.Println("MapTransformer - reflection transformer will be used for valueType: ", valueType)
		return &mapTransformer{"reflection", reflectionMapTransformer}
	}
	//fmt.Println("MapTransformer - transformer not found for type: ", valueType)
	return nil
}
