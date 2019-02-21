package broker

import (
	"errors"
	"strings"
	"time"

	bus "github.com/moleculer-go/goemitter"
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/cache"
	"github.com/moleculer-go/moleculer/context"
	"github.com/moleculer-go/moleculer/metrics"
	"github.com/moleculer-go/moleculer/middleware"
	"github.com/moleculer-go/moleculer/options"
	"github.com/moleculer-go/moleculer/payload"
	"github.com/moleculer-go/moleculer/registry"
	"github.com/moleculer-go/moleculer/serializer"
	"github.com/moleculer-go/moleculer/service"

	log "github.com/sirupsen/logrus"
)

func mergeConfigs(baseConfig moleculer.BrokerConfig, userConfig []*moleculer.BrokerConfig) moleculer.BrokerConfig {
	if len(userConfig) > 0 {
		for _, config := range userConfig {
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
			if config.TransporterFactory != nil {
				baseConfig.TransporterFactory = config.TransporterFactory
			}
			if config.DisableInternalMiddlewares {
				baseConfig.DisableInternalMiddlewares = config.DisableInternalMiddlewares
			}
			if config.DisableInternalServices {
				baseConfig.DisableInternalServices = config.DisableInternalServices
			}
			if config.Metrics {
				baseConfig.Metrics = config.Metrics
			}

			if config.MetricsRate > 0 {
				baseConfig.MetricsRate = config.MetricsRate
			}

			if config.DontWaitForNeighbours {
				baseConfig.DontWaitForNeighbours = config.DontWaitForNeighbours
			}

			if config.Middlewares != nil {
				baseConfig.Middlewares = config.Middlewares
			}
		}
	}
	return baseConfig
}

type ServiceBroker struct {
	namespace string

	logger *log.Entry

	localBus *bus.Emitter

	registry *registry.ServiceRegistry

	middlewares *middleware.Dispatch

	cache cache.Cache

	serializer *serializer.Serializer

	services []*service.Service

	started bool

	rootContext moleculer.BrokerContext

	config moleculer.BrokerConfig

	delegates moleculer.BrokerDelegates

	localNode moleculer.Node
}

// GetLocalBus : return the service broker local bus (Event Emitter)
func (broker *ServiceBroker) LocalBus() *bus.Emitter {
	return broker.localBus
}

// stopService stop the service.
func (broker *ServiceBroker) stopService(svc *service.Service) {
	broker.middlewares.CallHandlers("serviceStoping", svc)
	svc.Stop()
	broker.middlewares.CallHandlers("serviceStoped", svc)
}

// startService start a service.
func (broker *ServiceBroker) startService(svc *service.Service) {

	broker.middlewares.CallHandlers("serviceStarting", svc)

	broker.waitForDependencies(svc)

	svc.Start()

	broker.registry.AddLocalService(svc)

	broker.middlewares.CallHandlers("serviceStarted", svc)
}

// waitForDependencies wait for all services listed in the service dependencies to be discovered.
func (broker *ServiceBroker) waitForDependencies(service *service.Service) {
	if len(service.Dependencies()) == 0 {
		return
	}
	start := time.Now()
	for {
		if !broker.started {
			break
		}
		found := true
		for _, dependency := range service.Dependencies() {
			known := broker.registry.KnowService(dependency)
			if !known {
				found = false
				break
			}
		}
		if found {
			broker.logger.Debug("waitForDependencies() - All dependencies were found :) -> service: ", service.Name(), " wait For Dependencies: ", service.Dependencies())
			break
		}
		if time.Since(start) > broker.config.WaitForDependenciesTimeout {
			broker.logger.Warn("waitForDependencies() - Time out ! service: ", service.Name(), " wait For Dependencies: ", service.Dependencies())
			break
		}

		time.Sleep(100 * time.Millisecond)
	}
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
	//broker.logger.Debug("Broker Log Setup -> Level", log.GetLevel(), " nodeID: ", nodeID)
	return brokerLogger
}

// addService internal addService .. adds one service.Service instance to broker.services list.
func (broker *ServiceBroker) addService(svc *service.Service) {
	svc.SetNodeID(broker.localNode.GetID())
	broker.services = append(broker.services, svc)
	if broker.started {
		broker.startService(svc)
	}
}

// AddService : for each service schema it will validate and create
// a service instance in the broker.
func (broker *ServiceBroker) AddService(schemas ...moleculer.Service) {
	for _, schema := range schemas {
		broker.addService(service.FromSchema(schema, broker.GetLogger("service", schema.Name)))
	}
}

func (broker *ServiceBroker) Start() {
	if broker.IsStarted() {
		broker.logger.Warn("broker.Start() called on a broker that already started!")
		return
	}
	broker.logger.Info("Broker -> Starting...")

	broker.middlewares.CallHandlers("brokerStarting", broker.delegates)

	broker.registry.Start()

	internalServices := broker.registry.LocalServices()
	for _, service := range internalServices {
		broker.addService(service)
	}

	for _, service := range broker.services {
		broker.startService(service)
	}

	broker.logger.Debug("Broker -> registry started!")

	defer broker.broadcastLocal("$broker.started")
	defer broker.middlewares.CallHandlers("brokerStarted", broker.delegates)

	broker.started = true
	broker.logger.Info("Broker -> Started !!!")
}

func (broker *ServiceBroker) Stop() {
	broker.logger.Info("Broker -> Stoping...")

	broker.middlewares.CallHandlers("brokerStoping", broker.delegates)

	for _, service := range broker.services {
		broker.stopService(service)
	}

	broker.registry.Stop()

	broker.started = false
	broker.broadcastLocal("$broker.stoped")

	broker.middlewares.CallHandlers("brokerStoped", broker.delegates)
}

type callPair struct {
	label  string
	result moleculer.Payload
}

func (broker *ServiceBroker) invokeMCalls(callMaps map[string]map[string]interface{}, result chan map[string]moleculer.Payload) {
	if len(callMaps) == 0 {
		result <- make(map[string]moleculer.Payload)
		return
	}
	timeoutChan := make(chan bool, 1)
	go func(timeout time.Duration) {
		time.Sleep(timeout)
		timeoutChan <- true
	}(broker.config.MCallTimeout)

	callResults := make(chan callPair, len(callMaps))
	for label, content := range callMaps {
		go func(label, actionName string, params interface{}, callResults chan callPair) {
			result := <-broker.Call(actionName, params)
			callResults <- callPair{label, result}
		}(label, content["action"].(string), content["params"], callResults)
	}

	results := make(map[string]moleculer.Payload)
	for {
		select {
		case pair := <-callResults:
			results[pair.label] = pair.result
			if len(results) == len(callMaps) {
				result <- results
				return
			}
		case <-timeoutChan:
			timeoutError := errors.New("MCall timeout error.")
			broker.logger.Error(timeoutError)
			for label, _ := range callMaps {
				if _, exists := results[label]; !exists {
					results[label] = payload.Create(timeoutError)
				}
			}
			result <- results
			return
		}
	}
}

// MCall perform multiple calls and return all results together in a nice map indexed by name.
func (broker *ServiceBroker) MCall(callMaps map[string]map[string]interface{}) chan map[string]moleculer.Payload {
	result := make(chan map[string]moleculer.Payload, 1)
	go broker.invokeMCalls(callMaps, result)
	return result
}

// Call :  invoke a service action and return a channel which will eventualy deliver the results ;)
func (broker *ServiceBroker) Call(actionName string, params interface{}, opts ...moleculer.OptionsFunc) chan moleculer.Payload {
	broker.logger.Trace("Broker - Call() actionName: ", actionName, " params: ", params, " opts: ", opts)
	if !broker.IsStarted() {
		panic(errors.New("Broker must be started before making calls :("))
	}
	actionContext := broker.rootContext.ChildActionContext(actionName, payload.Create(params), options.Wrap(opts))
	return broker.registry.LoadBalanceCall(actionContext, options.Wrap(opts))
}

func (broker *ServiceBroker) Emit(event string, params interface{}, groups ...string) {
	broker.logger.Trace("Broker - Emit() event: ", event, " params: ", params, " groups: ", groups)
	if !broker.IsStarted() {
		panic(errors.New("Broker must be started before emiting events :("))
	}
	newContext := broker.rootContext.ChildEventContext(event, payload.Create(params), groups, false)
	broker.registry.LoadBalanceEvent(newContext)
}

func (broker *ServiceBroker) Broadcast(event string, params interface{}, groups ...string) {
	broker.logger.Trace("Broker - Broadcast() event: ", event, " params: ", params, " groups: ", groups)
	if !broker.IsStarted() {
		panic(errors.New("Broker must be started before broadcasting events :("))
	}
	newContext := broker.rootContext.ChildEventContext(event, payload.Create(params), groups, true)
	broker.registry.BroadcastEvent(newContext)
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

func (broker *ServiceBroker) setupLocalBus() {
	broker.localBus = bus.Construct()

	broker.localBus.On("$registry.service.added", func(args ...interface{}) {
		//TODO check code from -> this.broker.servicesChanged(true)
	})
}

func (broker *ServiceBroker) registerMiddlewares() {
	broker.middlewares = middleware.Dispatcher(broker.logger.WithField("middleware", "dispatcher"))
	for _, mware := range broker.config.Middlewares {
		broker.middlewares.Add(mware)
	}
	if !broker.config.DisableInternalMiddlewares {
		broker.registerInternalMiddlewares()
	}
}

func (broker *ServiceBroker) registerInternalMiddlewares() {
	broker.middlewares.Add(metrics.Middlewares())
}

func (broker *ServiceBroker) init() {
	broker.logger = broker.createBrokerLogger()
	broker.setupLocalBus()

	broker.registerMiddlewares()

	broker.logger.Debug("brokerConfig middleware before: \n", broker.config)
	broker.config = broker.middlewares.CallHandlers("brokerConfig", broker.config).(moleculer.BrokerConfig)
	broker.logger.Debug("brokerConfig middleware after: \n", broker.config)

	broker.delegates = broker.createDelegates()
	broker.registry = registry.CreateRegistry(broker.delegates)
	broker.localNode = broker.registry.LocalNode()
	broker.rootContext = context.BrokerContext(broker.delegates)

}

func (broker *ServiceBroker) createDelegates() moleculer.BrokerDelegates {
	return moleculer.BrokerDelegates{
		LocalNode: broker.LocalNode,
		Logger:    broker.newLogger,
		Bus:       broker.LocalBus,
		IsStarted: broker.IsStarted,
		Config:    broker.config,
		ActionDelegate: func(context moleculer.BrokerContext, opts ...moleculer.OptionsFunc) chan moleculer.Payload {
			return broker.registry.LoadBalanceCall(context, options.Wrap(opts))
		},
		EmitEvent: func(context moleculer.BrokerContext) {
			broker.registry.LoadBalanceEvent(context)
		},
		BroadcastEvent: func(context moleculer.BrokerContext) {
			broker.registry.BroadcastEvent(context)
		},
		HandleRemoteEvent: func(context moleculer.BrokerContext) {
			broker.registry.HandleRemoteEvent(context)
		},
		ServiceForAction: func(name string) *moleculer.Service {
			svc := broker.registry.ServiceForAction(name)
			if svc != nil {
				return svc.Schema()
			}
			return nil
		},
		MultActionDelegate: func(callMaps map[string]map[string]interface{}) chan map[string]moleculer.Payload {
			return broker.MCall(callMaps)
		},
	}
}

// FromConfig : returns a valid broker based on environment configuration
// this is usually called when creating a broker to starting the service(s)
func FromConfig(userConfig ...*moleculer.BrokerConfig) *ServiceBroker {
	config := mergeConfigs(moleculer.DefaultConfig, userConfig)
	broker := ServiceBroker{config: config}
	broker.init()
	broker.logger.Info("Broker - FromConfig() ")
	return &broker
}
