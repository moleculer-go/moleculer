package moleculer

import (
	"fmt"
	"time"

	bus "github.com/moleculer-go/goemitter"
	"github.com/moleculer-go/moleculer/util"

	log "github.com/sirupsen/logrus"
)

type ForEachFunc func(iterator func(key interface{}, value Payload) bool)

// Payload contains the data sent/return to actions.
// I has convinience methods to read action parameters by name with the right type.
type Payload interface {
	MapArray() []map[string]interface{}
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

// ActionSchema is used by the validation engine to check if parameters sent to the action are valid.
type ActionSchema interface {
}

type ObjectSchema struct {
	Source interface{}
}

type Action struct {
	Name        string
	Handler     ActionHandler
	Schema      ActionSchema
	Description string
}

type Event struct {
	Name    string
	Group   string
	Handler EventHandler
}

type Service struct {
	Name         string
	Version      string
	Dependencies []string
	Settings     map[string]interface{}
	Metadata     map[string]interface{}
	Hooks        map[string]interface{}
	Mixins       []Mixin
	Actions      []Action
	Events       []Event
	Created      LifecycleFunc
	Started      LifecycleFunc
	Stopped      LifecycleFunc
}

type Mixin struct {
	Name     string
	Settings map[string]interface{}
	Metadata map[string]interface{}
	Hooks    map[string]interface{}
	Actions  []Action
	Events   []Event
	Created  LifecycleFunc
	Started  LifecycleFunc
	Stopped  LifecycleFunc
}

type TransporterFactoryFunc func() interface{}

type BrokerConfig struct {
	LogLevel                   string
	LogFormat                  string
	DiscoverNodeID             func() string
	Transporter                string
	TransporterFactory         TransporterFactoryFunc
	HeartbeatFrequency         time.Duration
	HeartbeatTimeout           time.Duration
	OfflineCheckFrequency      time.Duration
	NeighboursCheckTimeout     time.Duration
	WaitForDependenciesTimeout time.Duration
	Middlewares                []Middlewares
	Namespace                  string
	RequestTimeout             time.Duration
	MCallTimeout               time.Duration
	RetryPolicy                RetryPolicy
	MaxCallLevel               int
	Metrics                    bool
	MetricsRate                float32
	DisableInternalServices    bool
	DisableInternalMiddlewares bool
	DontWaitForNeighbours      bool
	WaitForNeighboursInterval  time.Duration
	Created                    func()
	Started                    func()
	Stoped                     func()
}

var DefaultConfig = BrokerConfig{
	LogLevel:                   "INFO",
	LogFormat:                  "TEXT",
	DiscoverNodeID:             discoverNodeID,
	Transporter:                "MEMORY",
	HeartbeatFrequency:         15 * time.Second,
	HeartbeatTimeout:           30 * time.Second,
	OfflineCheckFrequency:      20 * time.Second,
	NeighboursCheckTimeout:     2 * time.Second,
	WaitForDependenciesTimeout: 2 * time.Second,
	Metrics:                    false,
	MetricsRate:                1,
	DisableInternalServices:    false,
	DisableInternalMiddlewares: false,
	Created:                    func() {},
	Started:                    func() {},
	Stoped:                     func() {},
	MaxCallLevel:               100,
	RetryPolicy: RetryPolicy{
		Enabled: false,
	},
	RequestTimeout:            0,
	MCallTimeout:              5 * time.Second,
	WaitForNeighboursInterval: 200 * time.Millisecond,
}

// discoverNodeID - should return the node id for this machine
func discoverNodeID() string {
	// return fmt.Sprint(strings.Replace(hostname, ".", "_", -1), "-", util.RandomString(12))
	return fmt.Sprint("Node_", util.RandomString(5))
}

type RetryPolicy struct {
	Enabled  bool
	Retries  int
	Delay    int
	MaxDelay int
	Factor   int
	Check    func(error) bool
}

type ActionHandler func(context Context, params Payload) interface{}
type EventHandler func(context Context, params Payload)
type LifecycleFunc func(service Service, logger *log.Entry)

type LoggerFunc func(name string, value string) *log.Entry
type BusFunc func() *bus.Emitter
type isStartedFunc func() bool
type LocalNodeFunc func() Node
type ActionDelegateFunc func(context BrokerContext, opts ...OptionsFunc) chan Payload
type EmitEventFunc func(context BrokerContext)
type ServiceForActionFunc func(string) *Service

type OptionsFunc func(key string) interface{}

type MiddlewareHandler func(params interface{}, next func(...interface{}))

type Middlewares map[string]MiddlewareHandler

type Middleware interface {
	CallHandlers(name string, params interface{}) interface{}
}
type Node interface {
	GetID() string
	ExportAsMap() map[string]interface{}
	IsAvailable() bool
	IsExpired(timeout time.Duration) bool
	Update(info map[string]interface{}) bool

	IncreaseSequence()
	HeartBeat(heartbeat map[string]interface{})
	AddService(service map[string]interface{})
}
type Context interface {
	//context methods used by services
	MCall(map[string]map[string]interface{}) chan map[string]interface{}
	Call(actionName string, params interface{}, opts ...OptionsFunc) chan Payload
	Emit(eventName string, params interface{}, groups ...string)
	Broadcast(eventName string, params interface{}, groups ...string)
	Logger() *log.Entry
}

type BrokerContext interface {
	ChildActionContext(actionName string, params Payload, opts ...OptionsFunc) BrokerContext
	ChildEventContext(eventName string, params Payload, groups []string, broadcast bool) BrokerContext

	ActionName() string
	EventName() string
	Payload() Payload
	Groups() []string
	IsBroadcast() bool

	//export context info in a map[string]
	AsMap() map[string]interface{}

	SetTargetNodeID(targetNodeID string)
	TargetNodeID() string

	ID() string
	Meta() *map[string]interface{}

	Logger() *log.Entry
}

//Needs Refactoring..2 broker interfaces.. one for regiwstry.. and for for all others.
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
	ServiceForAction  ServiceForActionFunc
}
