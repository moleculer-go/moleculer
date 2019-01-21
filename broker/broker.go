package broker

import (
	"context"

	. "github.com/moleculer-go/goemitter"
	. "github.com/moleculer-go/moleculer/cacher"
	. "github.com/moleculer-go/moleculer/common"
	. "github.com/moleculer-go/moleculer/middleware"
	. "github.com/moleculer-go/moleculer/registry"
	. "github.com/moleculer-go/moleculer/serializer"
	. "github.com/moleculer-go/moleculer/service"
	. "github.com/moleculer-go/moleculer/strategy"
	. "github.com/moleculer-go/moleculer/transit"
	log "github.com/sirupsen/logrus"
)

type brokerConfig struct {
}

type ServiceBroker struct {
	context *context.Context

	namespace string
	nodeID    string

	logger *log.Entry

	localBus *Emitter

	registry *ServiceRegistry

	middlewares *MiddlewareHandler

	cacher *Cacher

	serializer *Serializer

	transit *Transit

	services []*Service

	started bool

	callContext context.Context

	contextBroker contextBroker

	config brokerConfig

	strategy Strategy

	info *BrokerInfo

	localNode Node
}

// GetLocalBus : return the service broker local bus (Event Emitter)
func (broker *ServiceBroker) GetLocalBus() *Emitter {
	return broker.localBus
}

// startService start a service within the provided broker
func startService(broker *ServiceBroker, service *Service) {

	broker.middlewares.CallHandlers("serviceStarting", service)

	waitForDependencies(service)

	service.Start()

	notifyServiceStarted(service)

	broker.registry.AddLocalService(service)

	broker.middlewares.CallHandlers("serviceStarted", service)
}

// func (broker *ServiceBroker) registerLocalService(service *Service) {
// 	//TODOv -> call registry
// }

// wait for all service dependencies to load
func waitForDependencies(service *Service) {
	//TODO
}

// notify a service when it is started
func notifyServiceStarted(service *Service) {
	// if service.Started != nil {
	// 	service.Started()
	// }
	//TODO: notify mixins also.. that might have the started method
}

func (broker *ServiceBroker) broadcastLocal(eventName string, params ...interface{}) {
	//TODO
}

func setupLogger() *log.Entry {
	//log.SetFormatter(&log.JSONFormatter{})
	brokerLogger := log.WithFields(log.Fields{
		"broker": "yes",
	})
	return brokerLogger
}

// AddService : for each service schema it will validate and create
// a service instance in the broker.
func (broker *ServiceBroker) AddService(schemas ...ServiceSchema) {
	for _, schema := range schemas {
		service := CreateService(schema)
		broker.services = append(broker.services, service)
	}
}

func (broker *ServiceBroker) Start() {
	broker.logger.Info("Broker - starting ...")

	broker.started = false

	broker.middlewares.CallHandlers("starting", broker)

	broker.transit.Connect()

	for _, service := range broker.services {
		startService(broker, service)
	}

	broker.started = true
	broker.broadcastLocal("$broker.started")

	broker.transit.Ready()

	broker.middlewares.CallHandlers("started", broker)

	broker.logger.Info("Broker - started !")
}

// func (broker *ServiceBroker) findNextActionEndpoint(actionName string, opts ...map[string]interface{}) (*Endpoint, error) {

// 	var endpoint *Endpoint
// 	if opts != nil && opts[0]["nodeID"] != nil {
// 		nodeID := opts[0]["nodeID"].(string)
// 		//direct call
// 		endpoint = broker.registry.GetEndpointByNodeId(actionName, nodeID)
// 		if endpoint == nil {
// 			broker.logger.Warnf("Service %s  is not found on %s node.", actionName, nodeID)
// 			return nil, errors.New(fmt.Sprintf("Service Not Found - actionName: %s - nodeID: %s", actionName, nodeID))
// 		}
// 	} else {
// 		// Get endpoint list by action name
// 		endpointList := broker.registry.GetEndpointList(actionName)
// 		if endpointList == nil {
// 			broker.logger.Warnf("Service %s is not registered.", actionName)
// 			return nil, errors.New(fmt.Sprintf("Service Not Registered - actionName: %s", actionName))
// 		}
// 		endpoint = endpointList.Next(broker.strategy)
// 		if endpoint == nil {
// 			errMsg := fmt.Sprintf("Service %s is not available.", actionName)
// 			broker.logger.Warn(errMsg)
// 			return nil, errors.New(errMsg)
// 		}
// 		return endpoint, nil
// 	}

// 	return endpoint, nil
// }

type contextKey int

const (
	ContextBroker contextKey = iota
	ContextAction
)

type contextAction struct {
	actionName string
	params     interface{}
}

type contextBroker struct {
	//TODO add relevante broker info here
}

// TODO -> move to context factory
func createContext(broker *ServiceBroker, actionName string, params interface{}) context.Context {
	parent := broker.callContext
	if parent == nil {
		parent = context.WithValue(context.Background(), ContextBroker, broker.contextBroker)
	}
	return context.WithValue(parent, ContextAction, contextAction{actionName, params})
}

// Call :  invoke a service action and return a channel which will eventualy deliver the results ;)
func (broker *ServiceBroker) Call(actionName string, params interface{}, opts ...OptionsFunc) chan interface{} {
	broker.logger.Info("Broker - calling actionName: ", actionName, " params: ", params, " opts: ", opts)

	endpoint := broker.registry.NextActionEndpoint(actionName, broker.strategy, opts)
	if endpoint == nil {
		//TODO error handling... could not find the action in the registry
	}

	actionContext := createContext(broker, actionName, params)
	return endpoint.InvokeAction(&actionContext)
}

func (broker *ServiceBroker) Emit(event string, params interface{}) {
	broker.logger.Debug("Broker - emit !")
}

func (broker *ServiceBroker) GetInfo() *BrokerInfo {
	return broker.info
}

func (broker *ServiceBroker) IsStarted() bool {
	return broker.started
}

func (broker *ServiceBroker) GetLogger(name string) *log.Entry {
	return broker.logger.WithField(name, true)
}

func (broker *ServiceBroker) GetLocalNode() *Node {
	return &broker.localNode
}

func (broker *ServiceBroker) init() {
	broker.logger = setupLogger()
	broker.contextBroker = contextBroker{}
	broker.strategy = RoundRobinStrategy{}
	broker.setupLocalBus()
	broker.localNode = CreateNode(DiscoverNodeID())
	broker.info = &BrokerInfo{broker.GetLocalNode, broker.GetLogger, broker.GetLocalBus, broker.IsStarted}
	broker.registry = CreateRegistry(broker.GetInfo())

}

func (broker *ServiceBroker) setupLocalBus() {
	broker.localBus = CreateEmitter()

	broker.localBus.On("$registry.service.added", func(args ...interface{}) {
		//TODO check code from -> this.broker.servicesChanged(true)
	})
}

// BrokerFromContext : returns a valid broker based on a passed context
// this is called from any action / event
func FromContext(ctx *context.Context) *ServiceBroker {
	broker := ServiceBroker{context: ctx}
	broker.init()

	broker.logger.Info("Broker - BrokerFromContext() ")

	return &broker
}

// BrokerFromConfig : returns a valid broker based on environment configuration
// this is usually called when creating a broker to starting the service(s)
func FromConfig() *ServiceBroker {
	broker := ServiceBroker{config: brokerConfig{}}
	broker.init()

	broker.logger.Info("Broker - brokerFromConfig() ")
	return &broker
}
