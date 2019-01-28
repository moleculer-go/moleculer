// common : contain common types
package common

import (
	"time"

	. "github.com/moleculer-go/goemitter"
	log "github.com/sirupsen/logrus"
)

type Params interface {
	Get(name string) string
	String(name string) string
	Int(name string) int
	Int64(name string) int64
	Float(name string) float32
	Float64(name string) float64
	Map(name string) Params
}

type Context interface {

	//Common used context methods:
	Call(actionName string, params interface{}, opts ...OptionsFunc) chan interface{}
	Emit(eventName string, params interface{}, groups ...string)
	Broadcast(eventName string, params interface{}, groups ...string)

	NewActionContext(actionName string, params interface{}, opts ...OptionsFunc) Context

	GetActionName() string
	GetParams() Params

	//export context info in a map[string]
	AsMap() map[string]interface{}

	InvokeAction(opts ...OptionsFunc) chan interface{}

	SetNode(node *Node)
	GetNode() *Node
	GetID() string
	GetMeta() map[string]interface{}

	GetLogger() *log.Entry
}

type Endpoint interface {
	InvokeAction(context *Context) chan interface{}
	GetNodeID() string
	IsLocal() bool
}

type Strategy interface {
	SelectEndpoint([]Endpoint) Endpoint
}

type OptionsFunc func(key string) interface{}

func GetStringOption(key string, opts []OptionsFunc) string {
	result := GetOption(key, opts)
	if result != nil {
		return result.(string)
	}
	return ""
}

func GetOption(key string, opts []OptionsFunc) interface{} {
	for _, opt := range opts {
		result := opt(key)
		if result != nil {
			return result
		}
	}
	return nil
}

func WrapOptions(opts []OptionsFunc) OptionsFunc {
	return func(key string) interface{} {
		return GetOption(key, opts)
	}
}

type Transit interface {
	IsReady() bool
	Request(*Context) chan interface{}
	Connect() chan bool
	Ready() chan bool
	DiscoverNode(nodeID string)
	SendHeartbeat()
}

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
	BytesToMessage(bytes *[]byte) TransitMessage
	ContextToMessage(context *Context) TransitMessage
	MessageToContext(*TransitMessage) Context
	MapToMessage(mapValue *map[string]interface{}) TransitMessage
}

type getLoggerFunction func(name string, value string) *log.Entry
type getLocalBusFunction func() *Emitter
type isStartedFunction func() bool
type getLocalNodeFunction func() *Node
type GetTransitFunction func() *Transit
type GetSerializerFunction func() *Serializer
type RegistryMessageHandlerFunction func(command string, message *TransitMessage)

type BrokerInfo struct {
	GetLocalNode           getLocalNodeFunction
	GetLogger              getLoggerFunction
	GetLocalBus            getLocalBusFunction
	GetTransit             GetTransitFunction
	IsStarted              isStartedFunction
	GetSerializer          GetSerializerFunction
	RegistryMessageHandler RegistryMessageHandlerFunction
}
type Node interface {
	GetID() string
	IncreaseSequence()
	ExportAsMap() map[string]interface{}
	IsAvailable() bool
	HeartBeat(heartbeat map[string]interface{})
	IsExpired(timeout time.Duration) bool
	Update(info map[string]interface{}) bool
}
