package moleculer

import (
	"fmt"
	"os"
	"time"

	bus "github.com/moleculer-go/goemitter"
	"github.com/moleculer-go/moleculer/util"
	"go.mongodb.org/mongo-driver/bson"

	log "github.com/sirupsen/logrus"
)

type ForEachFunc func(iterator func(key interface{}, value Payload) bool)

// Payload contains the data sent/return to actions.
// I has convinience methods to read action parameters by name with the right type.
type Payload interface {
	First() Payload
	Sort(field string) Payload
	Remove(fields ...string) Payload
	AddItem(value interface{}) Payload
	Add(field string, value interface{}) Payload
	AddMany(map[string]interface{}) Payload
	MapArray() []map[string]interface{}
	RawMap() map[string]interface{}
	Bson() bson.M
	BsonArray() bson.A
	Map() map[string]Payload
	Exists() bool
	IsError() bool
	Error() error
	ErrorPayload() Payload
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
	ByteArray() []byte
	Time() time.Time
	TimeArray() []time.Time
	Array() []Payload
	At(index int) Payload
	Len() int
	Get(path string, defaultValue ...interface{}) Payload
	//Only return a payload containing only the field specified
	Only(path string) Payload
	IsArray() bool
	IsMap() bool
	ForEach(iterator func(key interface{}, value Payload) bool)
	MapOver(tranform func(in Payload) Payload) Payload
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
	Settings    map[string]interface{}
	Description string
}

type Event struct {
	Name    string
	Group   string
	Handler EventHandler
}

type ServiceSchema struct {
	Name         string
	Version      string
	Dependencies []string
	Settings     map[string]interface{}
	Metadata     map[string]interface{}
	Hooks        map[string]interface{}
	Mixins       []Mixin
	Actions      []Action
	Events       []Event
	Created      CreatedFunc
	Started      LifecycleFunc
	Stopped      LifecycleFunc
}

type Mixin struct {
	Name         string
	Dependencies []string
	Settings     map[string]interface{}
	Metadata     map[string]interface{}
	Hooks        map[string]interface{}
	Actions      []Action
	Events       []Event
	Created      CreatedFunc
	Started      LifecycleFunc
	Stopped      LifecycleFunc
}

type TransporterFactoryFunc func() interface{}
type StrategyFactoryFunc func() interface{}

type Config struct {
	LogLevel                   string
	LogFormat                  string
	DiscoverNodeID             func() string
	Transporter                string
	TransporterFactory         TransporterFactoryFunc
	StrategyFactory            StrategyFactoryFunc
	UpdateNodeMetricsFrequency time.Duration
	HeartbeatFrequency         time.Duration
	HeartbeatTimeout           time.Duration
	OfflineCheckFrequency      time.Duration
	OfflineTimeout             time.Duration
	NeighboursCheckTimeout     time.Duration
	WaitForDependenciesTimeout time.Duration
	Middlewares                []Middlewares
	Namespace                  string
	RequestTimeout             time.Duration
	MCallTimeout               time.Duration
	RetryPolicy                *RetryPolicy
	MaxCallLevel               int
	Metrics                    bool
	MetricsRate                float32
	DisableInternalServices    bool
	DisableInternalMiddlewares bool
	DontWaitForNeighbours      bool
	WaitForNeighboursInterval  time.Duration
	Created                    func()
	Started                    func()
	Stopped                    func()

	Services map[string]interface{}

	// TCP transporter options (map-based for user flexibility)
	TCPOptions map[string]interface{}
}

// TCPConfig holds TCP transporter configuration options
type TCPConfig struct {
	// Enable UDP discovery
	UdpDiscovery bool
	// Reusing UDP server socket
	UdpReuseAddr bool

	// UDP port for listening and discovery (defaults to 4445 for compatibility)
	UdpPort int
	// UDP bind address (if null, bind on all interfaces)
	UdpBindAddress string
	// UDP sending period (seconds)
	UdpPeriod time.Duration

	UdpMaxDiscovery int

	// Memory management options
	// Worker pool size for connection handling (default: 20)
	WorkerPoolSize int
	// Connection timeout duration (default: 30 seconds)
	ConnectionTimeout time.Duration
	// Idle connection cleanup interval (default: 60 seconds)
	IdleConnectionTimeout time.Duration

	// Multicast address.
	UdpMulticast string
	// Multicast TTL setting
	UdpMulticastTTL int

	// Send broadcast (Boolean, String, Array<String>)
	UdpBroadcast      []string
	UdpBroadcastAddrs []string
	// TCP server port. 0 means random port
	Port int
	// Static remote nodes address list (when UDP discovery is not available)
	Urls []string
	// Use hostname as preffered connection address
	UseHostname bool

	// Gossip sending period in seconds
	GossipPeriod int
	// Maximum enabled outgoing connections. If reach, close the old connections
	MaxConnections int
	// Maximum TCP packet size
	MaxPacketSize int

	Prefix      string
	NodeId      string
	Namespace   string
	Logger      *log.Entry
	Serializer  interface{} // Will be set to the actual serializer type
	ValidateMsg interface{} // Will be set to the actual validate function type
}

var DefaultConfig = Config{
	LogLevel:                   "INFO",
	LogFormat:                  "TEXT",
	DiscoverNodeID:             discoverNodeID,
	Transporter:                "MEMORY",
	UpdateNodeMetricsFrequency: 5 * time.Second,
	HeartbeatFrequency:         5 * time.Second,
	HeartbeatTimeout:           15 * time.Second,
	OfflineCheckFrequency:      20 * time.Second,
	OfflineTimeout:             10 * time.Minute,
	DontWaitForNeighbours:      true,
	NeighboursCheckTimeout:     2 * time.Second,
	WaitForDependenciesTimeout: 2 * time.Second,
	Metrics:                    false,
	MetricsRate:                1,
	DisableInternalServices:    false,
	DisableInternalMiddlewares: false,
	Created:                    func() {},
	Started:                    func() {},
	Stopped:                    func() {},
	MaxCallLevel:               100,
	RetryPolicy: &RetryPolicy{
		Enabled: false,
	},
	RequestTimeout:            3 * time.Second,
	MCallTimeout:              5 * time.Second,
	WaitForNeighboursInterval: 200 * time.Millisecond,

	// Default TCP options (matching JavaScript defaults)
	TCPOptions: map[string]interface{}{
		"UdpDiscovery":          true,
		"UdpReuseAddr":          true,
		"UdpPort":               4445, // Default UDP listening and discovery port (matches JavaScript)
		"UdpBindAddress":        "",
		"UdpPeriod":             30 * time.Second,
		"UdpMaxDiscovery":       0,                // Unlimited
		"WorkerPoolSize":        20,               // Default worker pool size
		"ConnectionTimeout":     30 * time.Second, // Default connection timeout
		"IdleConnectionTimeout": 60 * time.Second, // Default idle connection timeout
		"UdpMulticast":          "239.0.0.0",
		"UdpMulticastTTL":       1,
		"UdpBroadcast":          []string{},
		"Port":                  0, // Random TCP port
		"Urls":                  []string{},
		"UseHostname":           true,
		"GossipPeriod":          2, // 2 seconds
		"MaxConnections":        32,
		"MaxPacketSize":         1024 * 1024, // 1MB
		"Prefix":                "",
		"NodeId":                "",
		"Namespace":             "",
		"Logger":                nil, // Will be set by broker
		"Serializer":            nil, // Will be set by broker
		"ValidateMsg":           nil, // Will be set by broker
	},
}

// discoverNodeID - should return the node id for this machine
func discoverNodeID() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "node-" + util.RandomString(2)
	}
	return fmt.Sprint(hostname, "-", util.RandomString(5))
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
type CreatedFunc func(ServiceSchema, *log.Entry)
type LifecycleFunc func(BrokerContext, ServiceSchema)

type LoggerFunc func(name string, value string) *log.Entry
type BusFunc func() *bus.Emitter
type isStartedFunc func() bool
type LocalNodeFunc func() Node
type InstanceIDFunc func() string
type ActionDelegateFunc func(context BrokerContext, opts ...Options) chan Payload
type EmitEventFunc func(context BrokerContext)
type ServiceForActionFunc func(string) []*ServiceSchema
type MultActionDelegateFunc func(callMaps map[string]map[string]interface{}) chan map[string]Payload
type BrokerContextFunc func() BrokerContext
type MiddlewareHandlerFunc func(name string, params interface{}) interface{}
type PublishFunc func(...interface{})
type WaitForFunc func(...string) error
type MiddlewareHandler func(params interface{}, next func(...interface{}))

type Middlewares map[string]MiddlewareHandler

type Middleware interface {
	CallHandlers(name string, params interface{}) interface{}
}
type Node interface {
	GetID() string
	GetHost() string
	ExportAsMap() map[string]interface{}
	IsAvailable() bool
	GetIpList() []string
	GetPort() int
	Available()
	Unavailable()
	IsExpired(timeout time.Duration) bool
	Update(id string, info map[string]interface{}) (bool, []map[string]interface{})
	UpdateInfo(info map[string]interface{}) []map[string]interface{}
	IncreaseSequence()
	HeartBeat(heartbeat map[string]interface{})
	Publish(service map[string]interface{})
	GetUdpAddress() string
	GetSequence() int64
	GetCpuSequence() int64
	GetCpu() int64
	IsLocal() bool
	UpdateMetrics()
	GetHostname() string
}

type Options struct {
	Meta   Payload
	NodeID string
}

type EventOptions struct {
	Meta   Payload
	Groups []string
}

type Context interface {
	//context methods used by services
	MCall(map[string]map[string]interface{}) chan map[string]Payload
	Call(actionName string, params interface{}, opts ...Options) chan Payload
	Emit(eventName string, params interface{}, opts ...EventOptions)
	Broadcast(eventName string, params interface{}, opts ...EventOptions)
	Logger() *log.Entry

	Payload() Payload
	Meta() Payload
}

type ForEachNodeFunc func(node Node) bool
type Registry interface {
	GetNodeByID(nodeID string) Node
	AddOfflineNode(nodeID, hostname, ipAddress string, port int) Node
	ForEachNode(ForEachNodeFunc)
	DisconnectNode(nodeID string)
	RemoteNodeInfoReceived(message Payload)
	GetLocalNode() Node
	GetNodeByAddress(host string) Node
}

type BrokerContext interface {
	Call(actionName string, params interface{}, opts ...Options) chan Payload
	Emit(eventName string, params interface{}, opts ...EventOptions)
	Broadcast(eventName string, params interface{}, opts ...EventOptions)

	ChildActionContext(actionName string, params Payload, opts ...Options) BrokerContext
	ChildEventContext(eventName string, params Payload, groups []string, broadcast bool) BrokerContext

	ActionName() string
	EventName() string
	Payload() Payload
	Groups() []string
	IsBroadcast() bool
	Caller() string

	//export context info in a map[string]
	AsMap() map[string]interface{}

	SetTargetNodeID(targetNodeID string)
	TargetNodeID() string

	ID() string
	RequestID() string
	Meta() Payload
	UpdateMeta(Payload)
	Logger() *log.Entry

	Publish(...interface{})
	WaitFor(services ...string) error
}

// Needs Refactoring..2 broker interfaces.. one for regiwstry.. and for for all others.
type BrokerDelegates struct {
	InstanceID         InstanceIDFunc
	LocalNode          LocalNodeFunc
	Logger             LoggerFunc
	Bus                BusFunc
	IsStarted          isStartedFunc
	Config             Config
	MultActionDelegate MultActionDelegateFunc
	ActionDelegate     ActionDelegateFunc
	EmitEvent          EmitEventFunc
	BroadcastEvent     EmitEventFunc
	HandleRemoteEvent  EmitEventFunc
	ServiceForAction   ServiceForActionFunc
	BrokerContext      BrokerContextFunc
	MiddlewareHandler  MiddlewareHandlerFunc
	Publish            PublishFunc
	WaitFor            WaitForFunc
}
