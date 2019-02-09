package moleculer

import (
	"time"

	bus "github.com/moleculer-go/goemitter"

	log "github.com/sirupsen/logrus"
)

type ForEachFunc func(iterator func(key interface{}, value Payload) bool)

// Payload contains the data sent/return to actions.
// I has convinience methods to read action parameters by name with the right type.
type Payload interface {
	RawMap() map[string]interface{}
	Map() map[string]Payload
	Exists() bool
	IsError() bool
	Error() error
	Value() interface{}
	ValueArray() []interface{}
	Int() int
	IntArray() []int
	Int64() int64
	Int64Array() []int64
	Uint() uint64
	UintArray() []uint64
	Float32() float32
	Float32Array() []float32
	Float() float64
	FloatArray() []float64
	String() string
	StringArray() []string
	Bool() bool
	BoolArray() []bool
	Time() time.Time
	TimeArray() []time.Time
	Array() []Payload
	Get(path string) Payload
	IsArray() bool
	IsMap() bool
	ForEach(iterator func(key interface{}, value Payload) bool)
}

// ParamsSchema is used by the validation engine to check if parameters sent to the action are valid.
type ParamsSchema struct {
}

type Action struct {
	Name    string
	Handler ActionHandler
	Payload ParamsSchema
}

type Event struct {
	Name    string
	Group   string
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
	LogLevel               string
	LogFormat              string
	DiscoverNodeID         func() string
	Transporter            string
	HeartbeatFrequency     time.Duration
	HeartbeatTimeout       time.Duration
	OfflineCheckFrequency  time.Duration
	NeighboursCheckTimeout time.Duration
}

type ActionHandler func(context Context, params Payload) interface{}
type EventHandler func(context Context, params Payload)
type FuncType func()

type LoggerFunc func(name string, value string) *log.Entry
type BusFunc func() *bus.Emitter
type isStartedFunc func() bool
type LocalNodeFunc func() Node
type ActionDelegateFunc func(context BrokerContext, opts ...OptionsFunc) chan Payload
type EmitEventFunc func(context BrokerContext)

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
	Call(actionName string, params interface{}, opts ...OptionsFunc) chan Payload
	Emit(eventName string, params interface{}, groups ...string)
	Broadcast(eventName string, params interface{}, groups ...string)
	Logger() *log.Entry
}

type BrokerContext interface {
	NewActionContext(actionName string, params Payload, opts ...OptionsFunc) BrokerContext
	EventContext(actionName string, params Payload, groups []string, broadcast bool) BrokerContext

	ActionName() string
	EventName() string
	Payload() Payload
	Groups() []string

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
	EmitEvent         EmitEventFunc
	BroadcastEvent    EmitEventFunc
	HandleRemoteEvent EmitEventFunc
}
