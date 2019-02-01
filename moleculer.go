package moleculer

import (
	"time"

	bus "github.com/moleculer-go/goemitter"

	log "github.com/sirupsen/logrus"
)

// Params is wraps the payload sent to an action.
// I has convinience methods to read action parameters by name with the right type.
type Params interface {
	Get(name string) string
	String(name string) string
	Int(name string) int
	Int64(name string) int64
	Float(name string) float32
	Float64(name string) float64
	Map(name string) Params
	Value() interface{}
}

// ParamsSchema is used by the validation engine to check if parameters sent to the action are valid.
type ParamsSchema struct {
}

type Action struct {
	Name    string
	Handler ActionHandler
	Params  ParamsSchema
}

type Event struct {
	Name    string
	Handler EventHandler
}

type Service struct {
	Name     string
	Version  string
	Settings map[string]interface{}
	Metadata map[string]interface{}
	Hooks    map[string]interface{}
	Mixins   []Mixin
	Actions  []Action
	Events   []Event
	Created  FuncType
	Started  FuncType
	Stopped  FuncType
}

type Mixin struct {
	Name     string
	Settings map[string]interface{}
	Metadata map[string]interface{}
	Hooks    map[string]interface{}
	Actions  []Action
	Events   []Event
	Created  FuncType
	Started  FuncType
	Stopped  FuncType
}

type BrokerConfig struct {
	LogLevel              string
	LogFormat             string
	DiscoverNodeID        func() string
	Transporter           string
	HeartbeatFrequency    time.Duration
	HeartbeatTimeout      time.Duration
	OfflineCheckFrequency time.Duration
}

type ActionHandler func(context Context, params Params) interface{}
type EventHandler func(context Context, params Params)
type FuncType func()

type LoggerFunc func(name string, value string) *log.Entry
type BusFunc func() *bus.Emitter
type isStartedFunc func() bool
type LocalNodeFunc func() Node
type ActionDelegateFunc func(context BrokerContext, opts ...OptionsFunc) chan interface{}
type EventDelegateFunc func(context BrokerContext, groups []string)

type OptionsFunc func(key string) interface{}

type Node interface {
	GetID() string
	IncreaseSequence()
	ExportAsMap() map[string]interface{}
	IsAvailable() bool
	HeartBeat(heartbeat map[string]interface{})
	IsExpired(timeout time.Duration) bool
	Update(info map[string]interface{}) bool
	AddService(service map[string]interface{})
}

type Context interface {
	//context methods used by services
	Call(actionName string, params interface{}, opts ...OptionsFunc) chan interface{}
	Emit(eventName string, params interface{}, groups ...string)
	Broadcast(eventName string, params interface{}, groups ...string)

	Logger() *log.Entry
}

type BrokerContext interface {
	NewActionContext(actionName string, params interface{}, opts ...OptionsFunc) BrokerContext

	ActionName() string
	Params() Params

	//export context info in a map[string]
	AsMap() map[string]interface{}

	SetTargetNodeID(targetNodeID string)
	TargetNodeID() string

	ID() string
	Meta() *map[string]interface{}

	Logger() *log.Entry
}

type BrokerDelegates struct {
	LocalNode         LocalNodeFunc
	Logger            LoggerFunc
	Bus               BusFunc
	IsStarted         isStartedFunc
	Config            BrokerConfig
	ActionDelegate    ActionDelegateFunc
	EventDelegate     EventDelegateFunc
	BroadcastDelegate EventDelegateFunc
}
