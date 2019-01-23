package params

import (
	. "github.com/moleculer-go/moleculer/common"
)

type ParamsImpl struct {
	source *interface{}
	values *map[string]interface{}
}

func (params ParamsImpl) Int(name string) int {
	return (*params.values)[name].(int)
}

func (params ParamsImpl) Int64(name string) int64 {
	return (*params.values)[name].(int64)
}

func (params ParamsImpl) Float(name string) float32 {
	return (*params.values)[name].(float32)
}

func (params ParamsImpl) Float64(name string) float64 {
	return (*params.values)[name].(float64)
}

func (params ParamsImpl) String(name string) string {
	return (*params.values)[name].(string)
}

func (params ParamsImpl) Get(name string) string {
	return params.String(name)
}

func CreateParams(source *interface{}) Params {
	//TODO parse source and feed into values

	values := make(map[string]interface{})
	return ParamsImpl{source, &values}
}
