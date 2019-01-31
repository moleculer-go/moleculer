package service

import (
	"errors"
	"fmt"

	. "github.com/moleculer-go/moleculer/common"
)

type ActionSchema struct {
}

type ActionHandler func(ctx Context, params Params) interface{}

type EventHandler func(ctx Context, params Params)

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
	fullname string
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

func (serviceAction *ServiceAction) GetName() string {
	return serviceAction.name
}

func (serviceAction *ServiceAction) GetFullName() string {
	return serviceAction.fullname
}

func (service *Service) GetName() string {
	return service.name
}

func (service *Service) GetFullName() string {
	return service.fullname
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

func (service *Service) AsMap() map[string]interface{} {
	serviceInfo := make(map[string]interface{})

	serviceInfo["name"] = service.name
	serviceInfo["version"] = service.version

	serviceInfo["settings"] = service.settings
	serviceInfo["metadata"] = service.metadata

	actions := make([]map[string]interface{}, len(service.actions))
	for index, serviceAction := range service.actions {
		actionInfo := make(map[string]interface{})
		actionInfo["name"] = serviceAction.name
		actionInfo["schema"] = actionSchemaAsMap(&serviceAction.schema)
		actions[index] = actionInfo
	}
	serviceInfo["actions"] = actions

	events := make([]map[string]interface{}, len(service.events))
	for index, serviceEvent := range service.events {
		eventInfo := make(map[string]interface{})
		eventInfo["name"] = serviceEvent.name
		events[index] = eventInfo
	}
	serviceInfo["events"] = events

	return serviceInfo
}

func actionSchemaFromMap(schemaInfo map[string]interface{}) ActionSchema {
	//TODO
	return ActionSchema{}
}

func actionSchemaAsMap(actionSchema *ActionSchema) map[string]interface{} {
	//TODO
	schema := make(map[string]interface{})
	return schema
}

func (service *Service) AddActionMap(actionInfo map[string]interface{}) *ServiceAction {
	action := CreateServiceAction(
		service.fullname,
		actionInfo["name"].(string),
		nil,
		actionSchemaFromMap(actionInfo["schema"].(map[string]interface{})),
	)
	service.actions = append(service.actions, action)
	return &action
}

func (service *Service) RemoveAction(fullname string) {
	var newActions []ServiceAction
	for _, action := range service.actions {
		if action.fullname != fullname {
			newActions = append(newActions, action)
		}
	}
	service.actions = newActions
}

func (service *Service) AddEventMap(eventInfo map[string]interface{}) *ServiceEvent {
	serviceEvent := ServiceEvent{
		eventInfo["name"].(string),
		nil,
	}
	service.events = append(service.events, serviceEvent)
	return &serviceEvent
}

func (service *Service) UpdateFromMap(serviceInfo map[string]interface{}) {
	service.settings = serviceInfo["settings"].(map[string]interface{})
	service.metadata = serviceInfo["metadata"].(map[string]interface{})
}

// populateFromMap populate a service with data from a map[string]interface{}.
func populateFromMap(service *Service, serviceInfo map[string]interface{}) {
	service.version = serviceInfo["version"].(string)
	service.name = serviceInfo["name"].(string)
	service.fullname = joinVersionToName(
		service.name,
		service.version)

	service.settings = serviceInfo["settings"].(map[string]interface{})
	service.metadata = serviceInfo["metadata"].(map[string]interface{})
	actions := serviceInfo["actions"].([]interface{})
	for _, item := range actions {
		actionInfo := item.(map[string]interface{})
		service.AddActionMap(actionInfo)
	}

	events := serviceInfo["events"].([]interface{})
	for _, item := range events {
		eventInfo := item.(map[string]interface{})
		service.AddEventMap(eventInfo)
	}
}

// populateFromSchema populate a service with data from a ServiceSchema.
func populateFromSchema(service *Service, schema *ServiceSchema) {
	service.name = schema.Name
	service.version = schema.Version
	service.fullname = joinVersionToName(service.name, service.version)

	service.settings = schema.Settings
	if service.settings == nil {
		service.settings = make(map[string]interface{})
	}
	service.metadata = schema.Metadata
	if service.metadata == nil {
		service.metadata = make(map[string]interface{})
	}

	service.actions = make([]ServiceAction, len(schema.Actions))
	for index, actionSchema := range schema.Actions {
		service.actions[index] = CreateServiceAction(
			service.fullname,
			actionSchema.Name,
			actionSchema.Handler,
			actionSchema.Schema,
		)
	}

	service.events = make([]ServiceEvent, len(schema.Events))
	for index, eventSchema := range schema.Events {
		service.events[index] = ServiceEvent{
			eventSchema.Name,
			eventSchema.Handler,
		}
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
	populateFromSchema(service, &schema)
	if service.name == "" {
		panic(errors.New("Service name can't be empty! Maybe it is not a valid Service schema."))
	}
	return service
}

func CreateServiceFromMap(serviceInfo map[string]interface{}) *Service {
	service := &Service{}
	populateFromMap(service, serviceInfo)
	if service.name == "" {
		panic(errors.New("Service name can't be empty! Maybe it is not a valid Service schema."))
	}
	return service
}

// Start called by the broker when the service is starting.
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
