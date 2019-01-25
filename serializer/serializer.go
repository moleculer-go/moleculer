package serializer

import (
	. "github.com/moleculer-go/moleculer/common"
)

type TransitMessage interface {
	AsMap() map[string]interface{}
	Exists() bool
	Value() interface{}
	Int() int64
	Float() float64
	String() string
	Get(path string) TransitMessage
	//TODO add the reminaing from Result type from GJSON (https://github.com/tidwall/gjson)
}
type Serializer interface {
	ContextToMessage(context Context) TransitMessage
	MessageToContext(TransitMessage) Context
}
