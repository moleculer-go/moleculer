package test

import (
	"fmt"
	"sort"
)

// OrderMapArray given an array of map[string]... this func will order based on the field name.
func OrderMapArray(list []map[string]interface{}, field string) []map[string]interface{} {
	result := make([]map[string]interface{}, len(list))
	keys := make([]string, len(list))
	positions := make(map[string]int)
	for index, item := range list {
		keys[index] = fmt.Sprint(item[field])
		positions[keys[index]] = index
	}
	sort.Strings(keys)
	for index, key := range keys {
		position := positions[key]
		result[index] = list[position]
	}
	return result
}
