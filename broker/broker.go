package broker

import (
	"context"
	"errors"
	"fmt"

	log "github.com/sirupsen/logrus"

	. "github.com/moleculer-go/moleculer/cacher"
	. "github.com/moleculer-go/moleculer/endpoint"
	. "github.com/moleculer-go/moleculer/middleware"
	. "github.com/moleculer-go/moleculer/registry"
	. "github.com/moleculer-go/moleculer/serializer"
	. "github.com/moleculer-go/moleculer/service"
	. "github.com/moleculer-go/moleculer/strategy"
	. "github.com/moleculer-go/moleculer/transit"
)

type brokerConfig struct {
}

type ServiceBroker struct {
	context *context.Context

	namespace string
	nodeID    string

	logger *log.Entry

	registry *ServiceRegistry

	middlewares *MiddlewareHandler

	cacher *Cacher

	serializer *Serializer

	transit *Transit

	services []*ServiceSchema

	started bool

	callContext context.Context

	contextBroker contextBroker

	config brokerConfig

	strategy Strategy
}

func startService(broker *ServiceBroker, service *ServiceSchema) {

	broker.middlewares.CallHandlers("serviceStarting", service)

	waitForDependencies(service)

	notifyServiceStarted(service)

	broker.registerLocalService(service)

	broker.middlewares.CallHandlers("serviceStarted", service)
}

func (broker *ServiceBroker) registerLocalService(service *ServiceSchema) {
	//TODO
}

// wait for all service dependencies to load
func waitForDependencies(service *ServiceSchema) {
	//TODO
}

// notify a service when it is started
func notifyServiceStarted(service *ServiceSchema) {
	if service.Started != nil {
		service.Started()
	}
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

func (broker *ServiceBroker) Start(services ...*ServiceSchema) {
	broker.logger.Info("Broker - starting ...")

	broker.started = false
	broker.services = services

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

func (broker *ServiceBroker) findNextActionEndpoint(actionName string, opts ...map[string]interface{}) (*Endpoint, error) {

	var endpoint *Endpoint
	if opts != nil && opts[0]["nodeID"] != nil {
		nodeID := opts[0]["nodeID"].(string)
		//direct call
		endpoint = broker.registry.GetEndpointByNodeId(actionName, nodeID)
		if endpoint == nil {
			broker.logger.Warnf("Service %s  is not found on %s node.", actionName, nodeID)
			return nil, errors.New(fmt.Sprintf("Service Not Found - actionName: %s - nodeID: %s", actionName, nodeID))
		}
	} else {
		// Get endpoint list by action name
		endpointList := broker.registry.GetEndpointList(actionName)
		if endpointList == nil {
			broker.logger.Warnf("Service %s is not registered.", actionName)
			return nil, errors.New(fmt.Sprintf("Service Not Registered - actionName: %s", actionName))
		}
		endpoint = endpointList.Next(broker.strategy)
		if endpoint == nil {
			errMsg := fmt.Sprintf("Service %s is not available.", actionName)
			broker.logger.Warn(errMsg)
			return nil, errors.New(errMsg)
		}
		return endpoint, nil
	}

	return endpoint, nil
}

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

// Call :  call a service action
//
func (broker *ServiceBroker) Call(actionName string, params interface{}, opts ...map[string]interface{}) interface{} {
	broker.logger.Info("Broker - calling actionName: ", actionName, " params: ", params, " opts: ", opts)

	//TODO find a better way... this was suposed to be one line.. in both functions opts is ...
	var endpoint *Endpoint
	var err error
	if opts != nil {
		endpoint, err = broker.findNextActionEndpoint(actionName, opts[0])
	} else {
		endpoint, err = broker.findNextActionEndpoint(actionName)
	}
	if err != nil {
		panic(err) //TODO error handling...
	}

	actionContext := createContext(broker, actionName, params)

	//has to be async
	endpoint.ActionHandler(&actionContext)

	return 0
}

func (broker *ServiceBroker) Emit(event string, params interface{}) {
	broker.logger.Debug("Broker - emit !")
}

func (broker *ServiceBroker) init() {
	broker.logger = setupLogger()
	broker.contextBroker = contextBroker{}
	broker.strategy = RoundRobinStrategy{}
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
