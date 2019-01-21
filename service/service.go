package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/moleculer-go/moleculer/params"
)

type ActionSchema struct {
}

type ActionHandler func(ctx context.Context, params params.Params) chan interface{}

type EventHandler func(ctx context.Context, params params.Params)

type ServiceActionSchema struct {
	Name    string
	Handler ActionHandler
	Schema  ActionSchema
}

type ServiceAction struct {
	name     string
	fullname string
	handler  ActionHandler
	schema   ActionSchema
}

type ServiceEventSchema struct {
	Name    string
	Handler EventHandler
}

type ServiceEvent struct {
	name    string
	handler EventHandler
}

type FuncType func()

type ServiceSchema struct {
	Name     string
	Version  string
	Settings map[string]interface{}
	Metadata map[string]interface{}
	Mixins   []*ServiceSchema
	Actions  []ServiceActionSchema
	Events   []ServiceEventSchema
	Created  FuncType
	Started  FuncType
	Stopped  FuncType
}

type Service struct {
	name     string
	version  string
	settings map[string]interface{}
	metadata map[string]interface{}
	actions  []ServiceAction
	events   []ServiceEvent
	created  []FuncType
	started  []FuncType
	stopped  []FuncType
}

func (serviceAction *ServiceAction) ReplaceHandler(actionHandler ActionHandler) {
	serviceAction.handler = actionHandler
}

func (serviceAction *ServiceAction) GetHandler() ActionHandler {
	return serviceAction.handler
}

func (serviceAction *ServiceAction) GetFullName() string {
	return serviceAction.fullname
}

func (service *Service) GetName() string {
	return service.name
}

func (service *Service) GetVersion() string {
	return service.version
}

func (service *Service) GetActions() []ServiceAction {
	return service.actions
}

func (service *Service) Summary() map[string]string {
	return map[string]string{
		"name":    service.name,
		"version": service.version,
	}
}

func (service *Service) GetEvents() []ServiceEvent {
	return service.events
}

func mergeActions(service ServiceSchema, mixin *ServiceSchema) ServiceSchema {
	// for _, mixinAction := range mixin.Actions {
	// 	existing := filter(service.actions, func(item interface{}) bool {
	// 		action := item.(ServiceAction)
	// 		return action.Name == mixinAction.Name
	// 	})
	// }
	return service
}

func mergeEvents(service ServiceSchema, mixin *ServiceSchema) ServiceSchema {
	return service
}

func mergeSettings(service ServiceSchema, mixin *ServiceSchema) ServiceSchema {
	return service
}

func mergeMetadata(service ServiceSchema, mixin *ServiceSchema) ServiceSchema {
	return service
}

func mergeHooks(service ServiceSchema, mixin *ServiceSchema) ServiceSchema {
	return service
}

func applyMixins(service ServiceSchema) ServiceSchema {
	for _, mixin := range service.Mixins {
		service = mergeActions(service, mixin)
		service = mergeEvents(service, mixin)
		service = mergeSettings(service, mixin)
		service = mergeMetadata(service, mixin)
		service = mergeHooks(service, mixin)
	}
	return service
}

func joinVersionToName(name string, version string) string {
	if version != "" {
		return fmt.Sprintf("%s.%s", version, name)
	}
	return name
}

func CreateServiceAction(serviceName string, actionName string, handler ActionHandler, schema ActionSchema) ServiceAction {
	return ServiceAction{
		actionName,
		fmt.Sprintf("%s.%s", serviceName, actionName),
		handler,
		schema,
	}
}

func copyProperties(service *Service, schema *ServiceSchema) {
	service.name = joinVersionToName(schema.Name, schema.Version)
	service.version = schema.Version
	service.settings = schema.Settings
	service.metadata = schema.Metadata
	for _, actionSchema := range schema.Actions {
		service.actions = append(service.actions, CreateServiceAction(
			service.name,
			actionSchema.Name,
			actionSchema.Handler,
			actionSchema.Schema,
		))
	}

	for _, eventSchema := range schema.Events {
		service.events = append(service.events, ServiceEvent{
			eventSchema.Name,
			eventSchema.Handler,
		})
	}

	if schema.Created != nil {
		service.created = append(service.created, schema.Created)
	}
	if schema.Started != nil {
		service.started = append(service.started, schema.Started)
	}
	if schema.Stopped != nil {
		service.stopped = append(service.stopped, schema.Stopped)
	}
}

func CreateService(schema ServiceSchema) *Service {
	if len(schema.Mixins) > 0 {
		schema = applyMixins(schema)
	}
	service := &Service{}
	copyProperties(service, &schema)
	if service.name == "" {
		panic(errors.New("Service name can't be empty! Maybe it is not a valid Service schema."))
	}
	return service
}

func (service *Service) Start() {

}

type filterPredicate func(item interface{}) bool

func filter(list []interface{}, predicate filterPredicate) []interface{} {
	var result []interface{}
	for _, item := range list {
		if predicate(item) {
			result = append(result, item)
		}
	}
	return result
}
