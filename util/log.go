package util

import (
	"encoding/json"
	"fmt"
)

func PrettyPrintMap(data interface{}) string {
	prettyData, err := json.MarshalIndent(data, "", "    ")
	if err != nil {
		return fmt.Sprintf("Error while pretty printing: %v", err)
	}
	return string(prettyData)
}
