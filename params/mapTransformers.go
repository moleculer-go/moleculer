package params

import "fmt"

type copyMapFunction func(source *interface{}, target *map[string]interface{})
type mapTransformer struct {
	name    string
	copyMap copyMapFunction
}

var mapTransformers = []mapTransformer{
	mapTransformer{
		"map[string]interface {}",
		func(source *interface{}, target *map[string]interface{}) {
			sourceAsMap := (*source).(map[string]interface{})
			for key, value := range sourceAsMap {
				(*target)[key] = value
			}
		},
	},
	mapTransformer{
		"map[string]string",
		func(source *interface{}, target *map[string]interface{}) {
			sourceAsMap := (*source).(map[string]string)
			for key, value := range sourceAsMap {
				(*target)[key] = value
			}
		},
	},
	mapTransformer{
		"map[string]int",
		func(source *interface{}, target *map[string]interface{}) {
			sourceAsMap := (*source).(map[string]int)
			for key, value := range sourceAsMap {
				(*target)[key] = value
			}
		},
	},
	mapTransformer{
		"map[string]int64",
		func(source *interface{}, target *map[string]interface{}) {
			sourceAsMap := (*source).(map[string]int64)
			for key, value := range sourceAsMap {
				(*target)[key] = value
			}
		},
	},
	mapTransformer{
		"map[string]float32",
		func(source *interface{}, target *map[string]interface{}) {
			sourceAsMap := (*source).(map[string]float32)
			for key, value := range sourceAsMap {
				(*target)[key] = value
			}
		},
	},
	mapTransformer{
		"map[string]float64",
		func(source *interface{}, target *map[string]interface{}) {
			sourceAsMap := (*source).(map[string]float64)
			for key, value := range sourceAsMap {
				(*target)[key] = value
			}
		},
	},
}

// getValueType : return a string that represents the map type.
// examples: map[string]int , map[string]string, map[string]float32 and etc
// there are a few possible implementations, Reflection is not very
// popular in GO.. so this uses mt.Sprintf .. but we need to
// test a second version of this with Reflect to check what is faster
func getValueType(value *interface{}) string {
	return fmt.Sprintf("%T", (*value))
}

// getMapTransformer : return the map transformer for the specific map type
func getMapTransformer(value *interface{}) *mapTransformer {
	valueType := getValueType(value)
	for _, transformer := range mapTransformers {
		if valueType == transformer.name {
			return &transformer
		}
	}
	fmt.Println("ERROR getMapTransformer() did not find a transformer for type: ", valueType)
	return nil
}

// copyValues copy items form source into the target map
func copyValues(source *interface{}, target *map[string]interface{}) {
	if transformer := getMapTransformer(source); transformer != nil {
		transformer.copyMap(source, target)
	}
	//fmt.Println("ERROR copyValues() did not find a transformer")
}
