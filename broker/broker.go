package broker

import (
	"errors"
	"fmt"
	"strings"
	"time"

	bus "github.com/moleculer-go/goemitter"
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/cache"
	"github.com/moleculer-go/moleculer/context"
	"github.com/moleculer-go/moleculer/middleware"
	"github.com/moleculer-go/moleculer/options"
	"github.com/moleculer-go/moleculer/registry"
	"github.com/moleculer-go/moleculer/serializer"
	"github.com/moleculer-go/moleculer/service"
	"github.com/moleculer-go/moleculer/strategy"
	"github.com/moleculer-go/moleculer/util"

	log "github.com/sirupsen/logrus"
)

var defaultConfig = moleculer.BrokerConfig{
	LogLevel:               "INFO",
	LogFormat:              "TEXT",
	DiscoverNodeID:         DiscoverNodeID,
	Transporter:            "MEMORY",
	HeartbeatFrequency:     5 * time.Second,
	HeartbeatTimeout:       30 * time.Second,
	OfflineCheckFrequency:  20 * time.Second,
	NeighboursCheckTimeout: 2 * time.Second,
}

// DiscoverNodeID - should return the node id for this machine
func DiscoverNodeID() string {
	// TODO: Check moleculer JS algo for this..
	// hostname, err := os.Hostname()
	// if err != nil {
	// 	fmt.Errorf("Error trying to get the machine hostname - error: %s", err)
	// 	hostname = ""
	// }
	// return fmt.Sprint(strings.Replace(hostname, ".", "_", -1), "-", util.RandomString(12))
	return fmt.Sprint("Node_", util.RandomString(8))
}

func mergeConfigs(baseConfig moleculer.BrokerConfig, userConfig []*moleculer.BrokerConfig) moleculer.BrokerConfig {

	if len(userConfig) > 0 {
		config := userConfig[0]
		if config.LogLevel != "" {
			baseConfig.LogLevel = config.LogLevel
		}
		if config.LogFormat != "" {
			baseConfig.LogFormat = config.LogFormat
		}
		if config.DiscoverNodeID != nil {
			baseConfig.DiscoverNodeID = config.DiscoverNodeID
		}
		if config.Transporter != "" {
			baseConfig.Transporter = config.Transporter
		}
	}

	return baseConfig
}

type ServiceBroker struct {
	namespace string
	nodeID    string

	logger *log.Entry

	localBus *bus.Emitter

	registry *registry.ServiceRegistry

	middlewares *middleware.MiddlewareHandler

	cache cache.Cache

	serializer *serializer.Serializer

	services []*service.Service

	started bool

	rootContext moleculer.BrokerContext

	config moleculer.BrokerConfig

	strategy strategy.Strategy

	delegates moleculer.BrokerDelegates

	localNode moleculer.Node
}

// GetLocalBus : return the service broker local bus (Event Emitter)
func (broker *ServiceBroker) LocalBus() *bus.Emitter {
	return broker.localBus
}

// stopService stop the service.
func (broker *ServiceBroker) stopService(service *service.Service) {
	broker.middlewares.CallHandlers("serviceStoping", service)

	service.Stop()

	broker.middlewares.CallHandlers("serviceStoped", service)

}

// startService start a service.
func (broker *ServiceBroker) startService(service *service.Service) {

	broker.middlewares.CallHandlers("serviceStarting", service)

	waitForDependencies(service)

	service.Start()

	notifyServiceStarted(service)

	broker.registry.AddLocalService(service)

	broker.middlewares.CallHandlers("serviceStarted", service)
}

// wait for all service dependencies to load
func waitForDependencies(service *service.Service) {
	//TODO
}

// notify a service when it is started
func notifyServiceStarted(service *service.Service) {
	// if service.Started != nil {
	// 	service.Started()
	// }
	//TODO: notify mixins also.. that might have the started method
}

func (broker *ServiceBroker) broadcastLocal(eventName string, params ...interface{}) {
	//TODO
}

func (broker *ServiceBroker) createBrokerLogger() *log.Entry {

	if strings.ToUpper(broker.config.LogFormat) == "JSON" {
		log.SetFormatter(&log.JSONFormatter{})
	} else {
		log.SetFormatter(&log.TextFormatter{})
	}

	if strings.ToUpper(broker.config.LogLevel) == "WARN" {
		log.SetLevel(log.WarnLevel)
	} else if strings.ToUpper(broker.config.LogLevel) == "DEBUG" {
		log.SetLevel(log.DebugLevel)
	} else if strings.ToUpper(broker.config.LogLevel) == "TRACE" {
		log.SetLevel(log.TraceLevel)
	} else if strings.ToUpper(broker.config.LogLevel) == "ERROR" {
		log.SetLevel(log.ErrorLevel)
	} else if strings.ToUpper(broker.config.LogLevel) == "FATAL" {
		log.SetLevel(log.FatalLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	nodeID := broker.config.DiscoverNodeID()
	brokerLogger := log.WithFields(log.Fields{
		"broker": nodeID,
	})
	fmt.Println("Broker Log Setup -> Level", log.GetLevel(), " nodeID: ", nodeID)

	return brokerLogger
}

// AddService : for each service schema it will validate and create
// a service instance in the broker.
func (broker *ServiceBroker) AddService(schemas ...moleculer.Service) {
	for _, schema := range schemas {
		serviceInstance := service.FromSchema(schema)
		broker.services = append(broker.services, serviceInstance)
	}
}

func (broker *ServiceBroker) Stop() {
	broker.logger.Info("Broker -> Stoping...")

	broker.middlewares.CallHandlers("stoping", broker)

	for _, service := range broker.services {
		broker.stopService(service)
	}

	broker.registry.Stop()

	broker.started = false
	broker.broadcastLocal("$broker.stoped")

	broker.middlewares.CallHandlers("stoped", broker)
}

func (broker *ServiceBroker) Start() {
	broker.logger.Info("Broker -> Starting...")

	broker.started = false

	broker.middlewares.CallHandlers("starting", broker)

	for _, service := range broker.services {
		broker.startService(service)
	}

	broker.registry.Start()

	broker.logger.Debug("Broker -> registry started!")

	broker.started = true
	broker.broadcastLocal("$broker.started")

	broker.middlewares.CallHandlers("started", broker)

	broker.logger.Info("Broker -> Started!!!")
}

// Call :  invoke a service action and return a channel which will eventualy deliver the results ;)
func (broker *ServiceBroker) Call(actionName string, params interface{}, opts ...moleculer.OptionsFunc) chan interface{} {
	broker.logger.Trace("Broker - Call() actionName: ", actionName, " params: ", params, " opts: ", opts)
	if !broker.started {
		panic(errors.New("Broker must be started before making calls :("))
	}
	actionContext := broker.rootContext.NewActionContext(actionName, params, options.Wrap(opts))
	return broker.registry.DelegateCall(actionContext, options.Wrap(opts))
}

func (broker *ServiceBroker) Emit(event string, params interface{}) {
	broker.logger.Debug("Broker - emit !")
}

func (broker *ServiceBroker) IsStarted() bool {
	return broker.started
}

func (broker *ServiceBroker) GetLogger(name string, value string) *log.Entry {
	return broker.logger.WithField(name, value)
}

func (broker *ServiceBroker) LocalNode() moleculer.Node {
	return broker.localNode
}

func (broker *ServiceBroker) newLogger(name string, value string) *log.Entry {
	return broker.logger.WithField(name, value)
}

func (broker *ServiceBroker) init() {
	broker.logger = broker.createBrokerLogger()
	broker.strategy = strategy.RoundRobinStrategy{}
	broker.setupLocalBus()
	broker.localNode = registry.CreateNode(broker.config.DiscoverNodeID())

	broker.delegates = moleculer.BrokerDelegates{
		broker.LocalNode,
		broker.newLogger,
		broker.LocalBus,
		broker.IsStarted,
		broker.config,
		func(context moleculer.BrokerContext, opts ...moleculer.OptionsFunc) chan interface{} {
			if !broker.started {
				panic(errors.New("Broker must be started before making calls :("))
			}
			return broker.registry.DelegateCall(context, options.Wrap(opts))
		},
		func(context moleculer.BrokerContext, groups []string) {
			if !broker.started {
				panic(errors.New("Broker must be started before emiting events :("))
			}
			broker.registry.DelegateEvent(context, groups)
		},
		func(context moleculer.BrokerContext, groups []string) {
			if !broker.started {
				panic(errors.New("Broker must be started before broadcasting events :("))
			}
			broker.registry.DelegateBroadcast(context, groups)
		},
	}
	broker.registry = registry.CreateRegistry(broker.delegates)
	broker.rootContext = context.BrokerContext(broker.delegates)
}

func (broker *ServiceBroker) setupLocalBus() {
	broker.localBus = bus.Construct()

	broker.localBus.On("$registry.service.added", func(args ...interface{}) {
		//TODO check code from -> this.broker.servicesChanged(true)
	})
}

// FromConfig : returns a valid broker based on environment configuration
// this is usually called when creating a broker to starting the service(s)
func FromConfig(userConfig ...*moleculer.BrokerConfig) *ServiceBroker {

	config := mergeConfigs(defaultConfig, userConfig)
	broker := ServiceBroker{config: config}
	broker.init()

	broker.logger.Info("Broker - FromConfig() ")
	return &broker
}
