package service

import (
	"errors"
	"fmt"

	"github.com/moleculer-go/moleculer"
)

type Action struct {
	name     string
	fullname string
	handler  moleculer.ActionHandler
	params   moleculer.ParamsSchema
}

type Event struct {
	name        string
	serviceName string
	group       string
	handler     moleculer.EventHandler
}

func (event *Event) Handler() moleculer.EventHandler {
	return event.handler
}

func (event *Event) Name() string {
	return event.name
}

func (event *Event) ServiceName() string {
	return event.serviceName
}

func (event *Event) Group() string {
	return event.group
}

type Service struct {
	fullname     string
	name         string
	version      string
	dependencies []string
	settings     map[string]interface{}
	metadata     map[string]interface{}
	actions      []Action
	events       []Event
	created      []moleculer.FuncType
	started      []moleculer.FuncType
	stopped      []moleculer.FuncType
	schema       *moleculer.Service
}

func (service *Service) Schema() *moleculer.Service {
	return service.schema
}

func (service *Service) Dependencies() []string {
	return service.dependencies
}

func (serviceAction *Action) Handler() moleculer.ActionHandler {
	return serviceAction.handler
}

func (serviceAction *Action) Name() string {
	return serviceAction.name
}

func (serviceAction *Action) FullName() string {
	return serviceAction.fullname
}

func (service *Service) Name() string {
	return service.name
}

func (service *Service) FullName() string {
	return service.fullname
}

func (service *Service) Version() string {
	return service.version
}

func (service *Service) Actions() []Action {
	return service.actions
}

func (service *Service) Summary() map[string]string {
	return map[string]string{
		"name":    service.name,
		"version": service.version,
	}
}

func (service *Service) Events() []Event {
	return service.events
}

// extendActions merges the actions from the base service with the mixin schema.
func extendActions(service moleculer.Service, mixin *moleculer.Mixin) moleculer.Service {
	for _, mixinAction := range mixin.Actions {
		for _, serviceAction := range service.Actions {
			if serviceAction.Name != mixinAction.Name {
				service.Actions = append(service.Actions, mixinAction)
			}
		}
	}
	return service
}

func concatenateEvents(service moleculer.Service, mixin *moleculer.Mixin) moleculer.Service {
	for _, mixinEvent := range mixin.Events {
		service.Events = append(service.Events, mixinEvent)
	}
	return service
}

func extendSettings(service moleculer.Service, mixin *moleculer.Mixin) moleculer.Service {
	settings := make(map[string]interface{})
	for index, setting := range service.Settings {
		if _, ok := service.Settings[index]; ok {
			settings[index] = setting
		}
	}

	for index, setting := range mixin.Settings {
		if _, ok := mixin.Settings[index]; ok {
			settings[index] = setting
		}
	}
	service.Settings = settings
	return service
}

func extendMetadata(service moleculer.Service, mixin *moleculer.Mixin) moleculer.Service {
	metadata := make(map[string]interface{})
	for index, value := range service.Metadata {
		if _, ok := service.Metadata[index]; ok {
			metadata[index] = value
		}
	}

	for index, value := range mixin.Metadata {
		if _, ok := mixin.Metadata[index]; ok {
			metadata[index] = value
		}
	}
	service.Metadata = metadata
	return service
}

func extendHooks(service moleculer.Service, mixin *moleculer.Mixin) moleculer.Service {
	hooks := make(map[string]interface{})
	for index, hook := range service.Hooks {
		if _, ok := service.Hooks[index]; ok {
			hooks[index] = hook
		}
	}

	for index, hook := range mixin.Hooks {
		if _, ok := mixin.Hooks[index]; ok {
			hooks[index] = hook
		}
	}
	service.Hooks = hooks
	return service
}

func mergeNames(service moleculer.Service, mixin *moleculer.Mixin) moleculer.Service { return service }
func mergeVersions(service moleculer.Service, mixin *moleculer.Mixin) moleculer.Service {
	return service
}
func mergeMethods(service moleculer.Service, mixin *moleculer.Mixin) moleculer.Service { return service }
func mergeMixins(service moleculer.Service, mixin *moleculer.Mixin) moleculer.Service  { return service }
func mergeDependencies(service moleculer.Service, mixin *moleculer.Mixin) moleculer.Service {
	return service
}
func concatenateCreated(service moleculer.Service, mixin *moleculer.Mixin) moleculer.Service {
	return service
}
func concatenateStarted(service moleculer.Service, mixin *moleculer.Mixin) moleculer.Service {
	return service
}
func concatenateStopped(service moleculer.Service, mixin *moleculer.Mixin) moleculer.Service {
	return service
}

/*
Mixin Strategy:
(done)settings:      	Extend with defaultsDeep.
(done)metadata:   	Extend with defaultsDeep.
(broken)actions:    	Extend with defaultsDeep. You can disable an action from mixin if you set to false in your service.
(done)hooks:      	Extend with defaultsDeep.
(broken)events:     	Concatenate listeners.
TODO:
name:           Merge & overwrite.
version:    	Merge & overwrite.
methods:       	Merge & overwrite.
mixins:	        Merge & overwrite.
dependencies:   Merge & overwrite.
created:    	Concatenate listeners.
started:    	Concatenate listeners.
stopped:    	Concatenate listeners.
*/

func applyMixins(service moleculer.Service) moleculer.Service {
	for _, mixin := range service.Mixins {
		service = extendActions(service, &mixin)
		service = concatenateEvents(service, &mixin)
		service = extendSettings(service, &mixin)
		service = extendMetadata(service, &mixin)
		service = extendHooks(service, &mixin)
		service = mergeNames(service, &mixin)
		service = mergeVersions(service, &mixin)
		service = mergeMethods(service, &mixin)
		service = mergeMixins(service, &mixin)
		service = mergeDependencies(service, &mixin)
		service = concatenateCreated(service, &mixin)
		service = concatenateStarted(service, &mixin)
		service = concatenateStopped(service, &mixin)
	}
	return service
}

func joinVersionToName(name string, version string) string {
	if version != "" {
		return fmt.Sprintf("%s.%s", version, name)
	}
	return name
}

func CreateServiceEvent(eventName, serviceName, group string, handler moleculer.EventHandler) Event {
	return Event{
		eventName,
		serviceName,
		group,
		handler,
	}
}

func CreateServiceAction(serviceName string, actionName string, handler moleculer.ActionHandler, params moleculer.ParamsSchema) Action {
	return Action{
		actionName,
		fmt.Sprintf("%s.%s", serviceName, actionName),
		handler,
		params,
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
		actionInfo["params"] = paramsAsMap(&serviceAction.params)
		actions[index] = actionInfo
	}
	serviceInfo["actions"] = actions

	events := make([]map[string]interface{}, len(service.events))
	for index, serviceEvent := range service.events {
		eventInfo := make(map[string]interface{})
		eventInfo["name"] = serviceEvent.name
		eventInfo["serviceName"] = serviceEvent.serviceName
		eventInfo["group"] = serviceEvent.group
		events[index] = eventInfo
	}
	serviceInfo["events"] = events

	return serviceInfo
}

func paramsFromMap(schema interface{}) moleculer.ParamsSchema {
	// if schema != nil {
	//mapValues = schema.(map[string]interface{})
	//TODO
	// }
	return moleculer.ParamsSchema{}
}

// moleculer.ParamsAsMap converts params schema into a map.
func paramsAsMap(params *moleculer.ParamsSchema) map[string]interface{} {
	//TODO
	schema := make(map[string]interface{})
	return schema
}

func (service *Service) AddActionMap(actionInfo map[string]interface{}) *Action {
	action := CreateServiceAction(
		service.fullname,
		actionInfo["name"].(string),
		nil,
		paramsFromMap(actionInfo["schema"]),
	)
	service.actions = append(service.actions, action)
	return &action
}

func (service *Service) RemoteEvent(name string) {
	var newEvents []Event
	for _, event := range service.events {
		if event.name != name {
			newEvents = append(newEvents, event)
		}
	}
	service.events = newEvents
}

func (service *Service) RemoveAction(fullname string) {
	var newActions []Action
	for _, action := range service.actions {
		if action.fullname != fullname {
			newActions = append(newActions, action)
		}
	}
	service.actions = newActions
}

func (service *Service) AddEventMap(eventInfo map[string]interface{}) *Event {
	serviceEvent := Event{
		name:        eventInfo["name"].(string),
		serviceName: eventInfo["serviceName"].(string),
		group:       eventInfo["group"].(string),
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

// populateFromSchema populate a service with data from a moleculer.Service.
func (service *Service) populateFromSchema() {
	schema := service.schema
	service.name = schema.Name
	service.version = schema.Version
	service.fullname = joinVersionToName(service.name, service.version)
	service.dependencies = schema.Dependencies
	service.settings = schema.Settings
	if service.settings == nil {
		service.settings = make(map[string]interface{})
	}
	service.metadata = schema.Metadata
	if service.metadata == nil {
		service.metadata = make(map[string]interface{})
	}

	service.actions = make([]Action, len(schema.Actions))
	for index, actionSchema := range schema.Actions {
		service.actions[index] = CreateServiceAction(
			service.fullname,
			actionSchema.Name,
			actionSchema.Handler,
			actionSchema.Payload,
		)
	}

	service.events = make([]Event, len(schema.Events))
	for index, eventSchema := range schema.Events {
		group := eventSchema.Group
		if group == "" {
			group = service.Name()
		}
		service.events[index] = Event{
			name:        eventSchema.Name,
			serviceName: service.Name(),
			group:       group,
			handler:     eventSchema.Handler,
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

func FromSchema(schema moleculer.Service) *Service {
	if len(schema.Mixins) > 0 {
		schema = applyMixins(schema)
	}
	service := &Service{schema: &schema}
	service.populateFromSchema()
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
	//TODO implement service lifecycle
}

// Stop called by the broker when the service is stoping.
func (service *Service) Stop() {
	//TODO implement service lifecycle
}
