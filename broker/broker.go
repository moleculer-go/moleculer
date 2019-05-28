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
	"github.com/moleculer-go/moleculer/payload"
	"github.com/moleculer-go/moleculer/registry"
	"github.com/moleculer-go/moleculer/serializer"
	"github.com/moleculer-go/moleculer/service"

	log "github.com/sirupsen/logrus"
)

func mergeConfigs(baseConfig moleculer.Config, userConfig []*moleculer.Config) moleculer.Config {
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
			if config.RequestTimeout != 0 {
				baseConfig.RequestTimeout = config.RequestTimeout
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

	started  bool
	starting bool

	rootContext moleculer.BrokerContext

	config moleculer.Config

	delegates *moleculer.BrokerDelegates

	id string

	localNode moleculer.Node
}

// GetLocalBus : return the service broker local bus (Event Emitter)
func (broker *ServiceBroker) LocalBus() *bus.Emitter {
	return broker.localBus
}

// stopService stop the service.
func (broker *ServiceBroker) stopService(svc *service.Service) {
	broker.middlewares.CallHandlers("serviceStopping", svc)
	svc.Stop(broker.rootContext.ChildActionContext("service.stop", payload.Empty()))
	broker.middlewares.CallHandlers("serviceStopped", svc)
}

// startService start a service.
func (broker *ServiceBroker) startService(svc *service.Service) {

	broker.logger.Debug("Broker start service: ", svc.FullName())

	broker.middlewares.CallHandlers("serviceStarting", svc)

	broker.waitForDependencies(svc)

	broker.registry.AddLocalService(svc)

	broker.middlewares.CallHandlers("serviceStarted", svc)

	svc.Start(broker.rootContext.ChildActionContext("service.start", payload.Empty()))
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
		time.Sleep(time.Microsecond)
	}
}

func (broker *ServiceBroker) broadcastLocal(eventName string, params ...interface{}) {
	broker.LocalBus().EmitAsync(eventName, params)
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

	brokerLogger := log.WithFields(log.Fields{
		"broker": broker.id,
	})
	//broker.logger.Debug("Broker Log Setup -> Level", log.GetLevel(), " nodeID: ", nodeID)
	return brokerLogger
}

// addService internal addService .. adds one service.Service instance to broker.services list.
func (broker *ServiceBroker) addService(svc *service.Service) {
	svc.SetNodeID(broker.localNode.GetID())
	broker.services = append(broker.services, svc)
	if broker.started || broker.starting {
		broker.startService(svc)
	}
	broker.logger.Debug("Broker - addService() - fullname: ", svc.FullName(), " # actions: ", len(svc.Actions()), " # events: ", len(svc.Events()))
}

// createService create a new service instance, from a struct or a schema :)
func (broker *ServiceBroker) createService(svc interface{}) (*service.Service, error) {
	schema, isSchema := svc.(moleculer.ServiceSchema)
	if !isSchema {
		svc, err := service.FromObject(svc, broker.delegates)
		if err != nil {
			return nil, err
		}
		return svc, nil
	}
	return service.FromSchema(schema, broker.delegates), nil
}

// WaitFor : wait for all services to be available
func (broker *ServiceBroker) WaitFor(services ...string) error {
	for _, svc := range services {
		if err := broker.waitForService(svc); err != nil {
			return err
		}
	}
	return nil
}

// WaitFor : wait for all services to be available
func (broker *ServiceBroker) WaitForNodes(nodes ...string) error {
	for _, nodeID := range nodes {
		if err := broker.waitForNode(nodeID); err != nil {
			return err
		}
	}
	return nil
}

//waitForService wait for a service to be available
func (broker *ServiceBroker) waitForService(service string) error {
	start := time.Now()
	for {
		if broker.registry.KnowService(service) {
			break
		}
		if time.Since(start) > broker.config.WaitForDependenciesTimeout {
			err := errors.New("waitForService() - Timeout ! service: " + service)
			broker.logger.Error(err)
			return err
		}
		time.Sleep(time.Microsecond)
	}
	return nil
}

//waitForNode wait for a node to be available
func (broker *ServiceBroker) waitForNode(nodeID string) error {
	start := time.Now()
	for {
		if broker.registry.KnowNode(nodeID) {
			break
		}
		if time.Since(start) > broker.config.WaitForDependenciesTimeout {
			err := errors.New("waitForNode() - Timeout ! nodeID: " + nodeID)
			broker.logger.Error(err)
			return err
		}
		time.Sleep(time.Microsecond)
	}
	return nil
}

// Publish : for each service schema it will validate and create
// a service instance in the broker.
func (broker *ServiceBroker) Publish(services ...interface{}) {
	for _, item := range services {
		svc, err := broker.createService(item)
		if err != nil {
			panic(errors.New("Could not publish service - error: " + err.Error()))
		}
		broker.addService(svc)
	}
}

func (broker *ServiceBroker) Start() {
	if broker.IsStarted() {
		broker.logger.Warn("broker.Start() called on a broker that already started!")
		return
	}
	broker.starting = true
	broker.logger.Info("Moleculer is starting...")
	broker.logger.Info("Node ID: ", broker.localNode.GetID())

	broker.middlewares.CallHandlers("brokerStarting", broker.delegates)

	broker.registry.Start()

	internalServices := broker.registry.LocalServices()
	for _, service := range internalServices {
		service.SetNodeID(broker.localNode.GetID())
		broker.startService(service)
	}

	for _, service := range broker.services {
		broker.startService(service)
	}

	for _, service := range internalServices {
		broker.addService(service)
	}

	broker.logger.Debug("Broker -> registry started!")

	defer broker.broadcastLocal("$broker.started")
	defer broker.middlewares.CallHandlers("brokerStarted", broker.delegates)

	broker.started = true
	broker.starting = false
	broker.logger.Info("Service Broker with ", len(broker.services), " service(s) started successfully.")
}

func (broker *ServiceBroker) Stop() {
	if !broker.started {
		broker.logger.Info("Broker is not started!")
		return
	}
	broker.logger.Info("Service Broker is stopping...")

	broker.middlewares.CallHandlers("brokerStopping", broker.delegates)

	for _, service := range broker.services {
		broker.stopService(service)
	}

	broker.registry.Stop()

	broker.started = false
	broker.broadcastLocal("$broker.stopped")

	broker.middlewares.CallHandlers("brokerStopped", broker.delegates)
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

	resultChan := make(chan callPair)
	for label, content := range callMaps {
		go func(label, actionName string, params interface{}, results chan callPair) {
			result := <-broker.Call(actionName, params)
			results <- callPair{label, result}
		}(label, content["action"].(string), content["params"], resultChan)
	}

	timeoutChan := make(chan bool, 1)
	go func(timeout time.Duration) {
		time.Sleep(timeout)
		timeoutChan <- true
	}(broker.config.MCallTimeout)

	results := make(map[string]moleculer.Payload)
	for {
		select {
		case pair := <-resultChan:
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
					results[label] = payload.New(timeoutError)
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
func (broker *ServiceBroker) Call(actionName string, params interface{}, opts ...moleculer.Options) chan moleculer.Payload {
	broker.logger.Trace("Broker - Call() actionName: ", actionName, " params: ", params, " opts: ", opts)
	if !broker.IsStarted() {
		panic(errors.New("Broker must be started before making calls :("))
	}
	actionContext := broker.rootContext.ChildActionContext(actionName, payload.New(params), opts...)
	return broker.registry.LoadBalanceCall(actionContext, opts...)
}

func (broker *ServiceBroker) Emit(event string, params interface{}, groups ...string) {
	broker.logger.Trace("Broker - Emit() event: ", event, " params: ", params, " groups: ", groups)
	if !broker.IsStarted() {
		panic(errors.New("Broker must be started before emiting events :("))
	}
	newContext := broker.rootContext.ChildEventContext(event, payload.New(params), groups, false)
	broker.registry.LoadBalanceEvent(newContext)
}

func (broker *ServiceBroker) Broadcast(event string, params interface{}, groups ...string) {
	broker.logger.Trace("Broker - Broadcast() event: ", event, " params: ", params, " groups: ", groups)
	if !broker.IsStarted() {
		panic(errors.New("Broker must be started before broadcasting events :("))
	}
	newContext := broker.rootContext.ChildEventContext(event, payload.New(params), groups, true)
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
	broker.id = broker.config.DiscoverNodeID()
	broker.logger = broker.createBrokerLogger()
	broker.setupLocalBus()

	broker.registerMiddlewares()

	broker.config = broker.middlewares.CallHandlers("Config", broker.config).(moleculer.Config)

	broker.delegates = broker.createDelegates()
	broker.registry = registry.CreateRegistry(broker.id, broker.delegates)
	broker.localNode = broker.registry.LocalNode()
	broker.rootContext = context.BrokerContext(broker.delegates)

}

func (broker *ServiceBroker) createDelegates() *moleculer.BrokerDelegates {
	return &moleculer.BrokerDelegates{
		LocalNode: broker.LocalNode,
		Logger:    broker.newLogger,
		Bus:       broker.LocalBus,
		IsStarted: broker.IsStarted,
		Config:    broker.config,
		ActionDelegate: func(context moleculer.BrokerContext, opts ...moleculer.Options) chan moleculer.Payload {
			return broker.registry.LoadBalanceCall(context, opts...)
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
		ServiceForAction: func(name string) *moleculer.ServiceSchema {
			svc := broker.registry.ServiceForAction(name)
			if svc != nil {
				return svc.Schema()
			}
			return nil
		},
		MultActionDelegate: func(callMaps map[string]map[string]interface{}) chan map[string]moleculer.Payload {
			return broker.MCall(callMaps)
		},
		BrokerContext: func() moleculer.BrokerContext {
			return broker.rootContext
		},
		MiddlewareHandler: broker.middlewares.CallHandlers,
		Publish:           broker.Publish,
		WaitFor:           broker.WaitFor,
	}
}

// New : returns a valid broker based on environment configuration
// this is usually called when creating a broker to starting the service(s)
func New(userConfig ...*moleculer.Config) *ServiceBroker {
	config := mergeConfigs(moleculer.DefaultConfig, userConfig)
	broker := ServiceBroker{config: config}
	broker.init()
	return &broker
}
