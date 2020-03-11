package service

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/payload"
	log "github.com/sirupsen/logrus"
)

type Action struct {
	name     string
	fullname string
	handler  moleculer.ActionHandler
	params   moleculer.ActionSchema
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

type HasName interface {
	Name() string
}

type HasVersion interface {
	Version() string
}

type HasDependencies interface {
	Dependencies() []string
}

type HasSettings interface {
	Settings() map[string]interface{}
}

type HasMetadata interface {
	Metadata() map[string]interface{}
}

type HasMixins interface {
	Mixins() []moleculer.Mixin
}

type HasEvents interface {
	Events() []moleculer.Event
}

func ParseVersion(iver interface{}) string {

	v, ok := iver.(string)
	if ok {
		return v
	}

	f, ok := iver.(float64)
	if ok {
		return fmt.Sprintf("%g", f)
	}

	i, ok := iver.(int64)
	if ok {
		return fmt.Sprintf("%d", i)
	}

	return fmt.Sprintf("%v", iver)
}

type Service struct {
	nodeID       string
	fullname     string
	name         string
	version      string
	dependencies []string
	settings     map[string]interface{}
	metadata     map[string]interface{}
	actions      []Action
	events       []Event
	created      moleculer.CreatedFunc
	started      moleculer.LifecycleFunc
	stopped      moleculer.LifecycleFunc
	schema       *moleculer.ServiceSchema
	logger       *log.Entry
}

func (service *Service) Schema() *moleculer.ServiceSchema {
	return service.schema
}

func (service *Service) NodeID() string {
	return service.nodeID
}

func (service *Service) Settings() map[string]interface{} {
	return service.settings
}

func (service *Service) SetNodeID(nodeID string) {
	service.nodeID = nodeID
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
		"nodeID":  service.nodeID,
	}
}

func (service *Service) Events() []Event {
	return service.events
}

func findAction(name string, actions []moleculer.Action) bool {
	for _, a := range actions {

		if a.Name == name {
			return true
		}

	}
	return false
}

// extendActions merges the actions from the base service with the mixin schema.
func extendActions(service moleculer.ServiceSchema, mixin *moleculer.Mixin) moleculer.ServiceSchema {
	for _, ma := range mixin.Actions {
		if !findAction(ma.Name, service.Actions) {
			service.Actions = append(service.Actions, ma)
		}
	}
	return service
}

func mergeDependencies(service moleculer.ServiceSchema, mixin *moleculer.Mixin) moleculer.ServiceSchema {
	list := []string{}
	for _, item := range mixin.Dependencies {
		list = append(list, item)
	}
	for _, item := range service.Dependencies {
		list = append(list, item)
	}
	service.Dependencies = list
	return service
}

func concatenateEvents(service moleculer.ServiceSchema, mixin *moleculer.Mixin) moleculer.ServiceSchema {
	for _, mixinEvent := range mixin.Events {
		for _, serviceEvent := range service.Events {
			if serviceEvent.Name != mixinEvent.Name {
				service.Events = append(service.Events, mixinEvent)
			}
		}
	}
	return service
}

func MergeSettings(settings ...map[string]interface{}) map[string]interface{} {
	result := map[string]interface{}{}
	for _, set := range settings {
		if set != nil {
			for key, value := range set {
				result[key] = value
			}
		}
	}
	return result
}

func extendSettings(service moleculer.ServiceSchema, mixin *moleculer.Mixin) moleculer.ServiceSchema {
	service.Settings = MergeSettings(mixin.Settings, service.Settings)
	return service
}

func extendMetadata(service moleculer.ServiceSchema, mixin *moleculer.Mixin) moleculer.ServiceSchema {
	service.Metadata = MergeSettings(mixin.Metadata, service.Metadata)
	return service
}

func extendHooks(service moleculer.ServiceSchema, mixin *moleculer.Mixin) moleculer.ServiceSchema {
	service.Hooks = MergeSettings(mixin.Hooks, service.Hooks)
	return service
}

// chainCreated chain the Created hook of services and mixins
// the service.Created handler is called after all of the mixins Created
// handlers are called. so all initialization that your service need and is done by plugins
// will be done by the time your service created is called.
func chainCreated(service moleculer.ServiceSchema, mixin *moleculer.Mixin) moleculer.ServiceSchema {
	if mixin.Created != nil {
		svcHook := service.Created
		mixinHook := mixin.Created
		service.Created = func(svc moleculer.ServiceSchema, log *log.Entry) {
			mixinHook(svc, log)
			if svcHook != nil {
				svcHook(svc, log)
			}
		}
	}
	return service
}

// chainStarted chain the Started hook of services and mixins
// the service.Started handler is called after all of the mixins Started
// handlers are called. so all initialization that your service need and is done by plugins
// will be done by the time your service Started is called.
func chainStarted(service moleculer.ServiceSchema, mixin *moleculer.Mixin) moleculer.ServiceSchema {
	if mixin.Started != nil {
		svcHook := service.Started
		mixinHook := mixin.Started
		service.Started = func(ctx moleculer.BrokerContext, svc moleculer.ServiceSchema) {
			mixinHook(ctx, svc)
			if svcHook != nil {
				svcHook(ctx, svc)
			}
		}
	}
	return service
}

// chainStopped chain the Stope hook of services and mixins
// the service.Stopped handler is called after all of the mixins Stopped
// handlers are called. so all clean up is done by plugins before calling
// your service Stopped function.
func chainStopped(service moleculer.ServiceSchema, mixin *moleculer.Mixin) moleculer.ServiceSchema {
	if mixin.Stopped != nil {
		svcHook := service.Stopped
		mixinHook := mixin.Stopped
		service.Stopped = func(ctx moleculer.BrokerContext, svc moleculer.ServiceSchema) {
			mixinHook(ctx, svc)
			if svcHook != nil {
				svcHook(ctx, svc)
			}
		}
	}
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

func applyMixins(service moleculer.ServiceSchema) moleculer.ServiceSchema {
	for _, mixin := range service.Mixins {
		service = extendActions(service, &mixin)
		service = mergeDependencies(service, &mixin)
		service = concatenateEvents(service, &mixin)
		service = extendSettings(service, &mixin)
		service = extendMetadata(service, &mixin)
		service = extendHooks(service, &mixin)
		service = chainCreated(service, &mixin)
		service = chainStarted(service, &mixin)
		service = chainStopped(service, &mixin)
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

func CreateServiceAction(serviceName string, actionName string, handler moleculer.ActionHandler, params moleculer.ActionSchema) Action {
	return Action{
		actionName,
		fmt.Sprintf("%s.%s", serviceName, actionName),
		handler,
		params,
	}
}

// AsMap export the service info in a map containing: name, version, settings, metadata, nodeID, actions and events.
// The events list does not contain internal events (events that starts with $) like $node.disconnected.
func (service *Service) AsMap() map[string]interface{} {
	serviceInfo := make(map[string]interface{})

	serviceInfo["name"] = service.name
	serviceInfo["version"] = service.version

	serviceInfo["settings"] = service.settings
	serviceInfo["metadata"] = service.metadata
	serviceInfo["nodeID"] = service.nodeID

	if service.nodeID == "" {
		panic("no service.nodeID")
	}

	actions := map[string]map[string]interface{}{}
	for _, serviceAction := range service.actions {
		if !isInternalAction(serviceAction) {
			actionInfo := make(map[string]interface{})
			actionInfo["name"] = serviceAction.fullname
			actionInfo["rawName"] = serviceAction.name
			actionInfo["params"] = paramsAsMap(&serviceAction.params)
			actions[serviceAction.name] = actionInfo
		}
	}
	serviceInfo["actions"] = actions

	events := map[string]map[string]interface{}{}
	for _, serviceEvent := range service.events {
		if !isInternalEvent(serviceEvent) {
			eventInfo := make(map[string]interface{})
			eventInfo["name"] = serviceEvent.name
			eventInfo["group"] = serviceEvent.group
			events[serviceEvent.name] = eventInfo
		}
	}
	serviceInfo["events"] = events
	return serviceInfo
}

func isInternalAction(action Action) bool {
	return strings.Index(action.Name(), "$") == 0
}

func isInternalEvent(event Event) bool {
	return strings.Index(event.Name(), "$") == 0
}

func paramsFromMap(schema interface{}) moleculer.ActionSchema {
	// if schema != nil {
	//mapValues = schema.(map[string]interface{})
	//TODO
	// }
	return moleculer.ObjectSchema{nil}
}

// moleculer.ParamsAsMap converts params schema into a map.
func paramsAsMap(params *moleculer.ActionSchema) map[string]interface{} {
	//TODO
	schema := make(map[string]interface{})
	return schema
}

func (service *Service) AddActionMap(actionInfo map[string]interface{}) *Action {
	action := CreateServiceAction(
		service.fullname,
		actionInfo["rawName"].(string),
		nil,
		paramsFromMap(actionInfo["schema"]),
	)
	service.actions = append(service.actions, action)
	return &action
}

func (service *Service) RemoveEvent(name string) {
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
	group, exists := eventInfo["group"]
	if !exists {
		group = service.name
	}
	serviceEvent := Event{
		name:        eventInfo["name"].(string),
		serviceName: service.name,
		group:       group.(string),
	}
	service.events = append(service.events, serviceEvent)
	return &serviceEvent
}

//UpdateFromMap update the service metadata and settings from a serviceInfo map
func (service *Service) UpdateFromMap(serviceInfo map[string]interface{}) {
	service.settings = serviceInfo["settings"].(map[string]interface{})
	service.metadata = serviceInfo["metadata"].(map[string]interface{})
}

// AddSettings add settings to the service. it will be merged with the
// existing service settings
func (service *Service) AddSettings(settings map[string]interface{}) {
	service.settings = MergeSettings(service.settings, settings)
}

// AddMetadata add metadata to the service. it will be merged with existing service metadata.
func (service *Service) AddMetadata(metadata map[string]interface{}) {
	service.metadata = MergeSettings(service.metadata, metadata)
}

// populateFromMap populate a service with data from a map[string]interface{}.
func populateFromMap(service *Service, serviceInfo map[string]interface{}) {
	if nodeID, ok := serviceInfo["nodeID"]; ok {
		service.nodeID = nodeID.(string)
	}
	service.version = ParseVersion(serviceInfo["version"])
	service.name = serviceInfo["name"].(string)
	service.fullname = joinVersionToName(
		service.name,
		service.version)

	service.settings = serviceInfo["settings"].(map[string]interface{})
	service.metadata = serviceInfo["metadata"].(map[string]interface{})
	actions := serviceInfo["actions"].(map[string]interface{})
	for _, item := range actions {
		actionInfo := item.(map[string]interface{})
		service.AddActionMap(actionInfo)
	}

	events := serviceInfo["events"].(map[string]interface{})
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
			actionSchema.Schema,
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

	service.created = schema.Created
	service.started = schema.Started
	service.stopped = schema.Stopped
}

func copyVersion(obj interface{}, schema moleculer.ServiceSchema) moleculer.ServiceSchema {
	versioner, hasIt := obj.(HasVersion)
	if hasIt {
		schema.Version = versioner.Version()
	}
	return schema
}

func copyDependencies(obj interface{}, schema moleculer.ServiceSchema) moleculer.ServiceSchema {
	del, hasIt := obj.(HasDependencies)
	if hasIt {
		schema.Dependencies = del.Dependencies()
	}
	return schema
}

func copyEvents(obj interface{}, schema moleculer.ServiceSchema) moleculer.ServiceSchema {
	del, hasIt := obj.(HasEvents)
	if hasIt {
		schema.Events = del.Events()
	}
	return schema
}

func copyMetadata(obj interface{}, schema moleculer.ServiceSchema) moleculer.ServiceSchema {
	del, hasIt := obj.(HasMetadata)
	if hasIt {
		schema.Metadata = del.Metadata()
	}
	return schema
}

func copyMixins(obj interface{}, schema moleculer.ServiceSchema) moleculer.ServiceSchema {
	del, hasIt := obj.(HasMixins)
	if hasIt {
		schema.Mixins = del.Mixins()
	}
	return schema
}

func copySettings(obj interface{}, schema moleculer.ServiceSchema) moleculer.ServiceSchema {
	del, hasIt := obj.(HasSettings)
	if hasIt {
		schema.Settings = del.Settings()
	}
	return schema
}

var invalid = []string{
	"Name", "Version", "Dependencies", "Settings",
	"Metadata", "Mixins", "Events", "Created", "Started", "Stopped",
}

// validActionName checks if a given merhod (reflect.Value) is a valid action name.
func validActionName(name string) bool {
	for _, item := range invalid {
		if item == name {
			return false
		}
	}
	return true
}

// actionName given a method (reflect.Type) format the action name
// using camel case. Example: SetLogRate = setLogRate
func actionName(name string) string {
	if len(name) < 2 {
		return strings.ToLower(name)
	}
	return strings.ToLower(name[:1]) + name[1:len(name)]
}

type aHandlerTemplate struct {
	match func(interface{}) bool
	wrap  func(reflect.Value, interface{}) moleculer.ActionHandler
}

// handlerTemplate return an action hanler that is based on a template.
func handlerTemplate(m reflect.Value) moleculer.ActionHandler {
	obj := m.Interface()
	for _, t := range actionHandlerTemplates {
		if t.match(obj) {
			return t.wrap(m, obj)
		}
	}
	return nil
}

// getParamTypes return a list with the type of each arguments
func getParamTypes(m reflect.Value) []string {
	t := m.Type()
	result := make([]string, t.NumIn())
	for i := 0; i < t.NumIn(); i++ {
		result[i] = t.In(i).Name()
	}
	return result
}

// payloadToValue converts a payload to value considering the type.
func payloadToValue(t string, p moleculer.Payload) reflect.Value {
	if t == "Payload" {
		return reflect.ValueOf(p)
	}
	return reflect.ValueOf(p.Value())
}

func buildArgs(ptypes []string, p moleculer.Payload) []reflect.Value {
	args := []reflect.Value{}
	if p.IsArray() {
		list := p.Array()
		for i, t := range ptypes {
			v := payloadToValue(t, list[i])
			args = append(args, v)
		}
	} else if p.Exists() {
		v := payloadToValue(ptypes[0], p)
		args = append(args, v)
	}
	return args
}

// validateArgs check if param is an array and that the lenght matches with the expected form the handler function.
func validateArgs(ptypes []string, p moleculer.Payload) error {
	if !p.IsArray() && len(ptypes) > 1 {
		return errors.New(fmt.Sprint("This action requires arguments to be sent in an array. #", len(ptypes), " arguments - types: ", ptypes))
	}
	if p.Len() != len(ptypes) && len(ptypes) > 1 {
		return errors.New(fmt.Sprint("This action requires #", len(ptypes), " arguments - types: ", ptypes))
	}
	return nil
}

func isError(v interface{}) bool {
	_, is := v.(error)
	return is
}

func checkReturn(in []reflect.Value) interface{} {
	if in == nil || len(in) == 0 {
		return nil
	}
	if len(in) == 1 {
		return in[0].Interface()
	}
	if isError(in[len(in)-1].Interface()) {
		return in[len(in)-1].Interface()
	}
	return valuesToPayload(in)
}

// valuesToPayload convert a list (2 or more) of reflect.values to a payload obj.
func valuesToPayload(vs []reflect.Value) moleculer.Payload {
	list := make([]interface{}, len(vs))
	for i, v := range vs {
		list[i] = v.Interface()
	}
	return payload.New(list)
}

// variableArgsHandler creates an action hanler that deals with variable number of arguments.
func variableArgsHandler(m reflect.Value) moleculer.ActionHandler {
	ptypes := getParamTypes(m)
	return func(ctx moleculer.Context, p moleculer.Payload) interface{} {
		err := validateArgs(ptypes, p)
		if err != nil {
			return err
		}
		args := buildArgs(ptypes, p)
		return checkReturn(m.Call(args))
	}
}

// wrapAction creates an action that invokes the given a method (reclect.Value).
func wrapAction(m reflect.Method, v reflect.Value) moleculer.Action {
	handler := handlerTemplate(v)
	if handler == nil {
		handler = variableArgsHandler(v)
	}
	return moleculer.Action{
		Name:    actionName(m.Name),
		Handler: handler,
	}
}

// extractActions uses reflection to get all public methods of the object.
// from a list of methods decided which ones match the criteria to be an action.
func extractActions(obj interface{}) []moleculer.Action {
	actions := []moleculer.Action{}
	value := reflect.ValueOf(obj)
	tp := value.Type()
	for i := 0; i < tp.NumMethod(); i++ {
		m := tp.Method(i)
		if validActionName(m.Name) {
			actions = append(actions, wrapAction(m, value.Method(i)))
		}
	}
	return actions
}

type HasCreated interface {
	Created(moleculer.ServiceSchema, *log.Entry)
}
type HasCreatedNoParams interface {
	Created()
}

type HasStarted interface {
	Started(moleculer.BrokerContext, moleculer.ServiceSchema)
}
type HasStartedNoParams interface {
	Started()
}

type HasStopped interface {
	Stopped(moleculer.BrokerContext, moleculer.ServiceSchema)
}
type HasStoppedNoParams interface {
	Stopped()
}

func extractCreated(obj interface{}) moleculer.CreatedFunc {
	creator, hasIt := obj.(HasCreated)
	if hasIt {
		return creator.Created
	}
	creator2, hasIt2 := obj.(HasCreatedNoParams)
	if hasIt2 {
		return func(moleculer.ServiceSchema, *log.Entry) {
			creator2.Created()
		}
	}
	return nil
}

func extractStarted(obj interface{}) moleculer.LifecycleFunc {
	starter, hasIt := obj.(HasStarted)
	if hasIt {
		return starter.Started
	}
	starter2, hasIt2 := obj.(HasStartedNoParams)
	if hasIt2 {
		return func(moleculer.BrokerContext, moleculer.ServiceSchema) {
			starter2.Started()
		}
	}
	return nil
}

func extractStopped(obj interface{}) moleculer.LifecycleFunc {
	stopper, hasIt := obj.(HasStopped)
	if hasIt {
		return stopper.Stopped
	}
	stopper2, hasIt2 := obj.(HasStoppedNoParams)
	if hasIt2 {
		return func(moleculer.BrokerContext, moleculer.ServiceSchema) {
			stopper2.Stopped()
		}
	}
	return nil
}

func getName(obj interface{}) (string, error) {
	namer, hasName := obj.(HasName)
	var p interface{} = &obj
	pnamer, hasPName := p.(HasName)
	if !hasName && !hasPName {
		return "", errors.New("Service instance must have a non pointer method [ Name() string ]")
	}
	if hasName {
		return namer.Name(), nil
	}
	return pnamer.Name(), nil
}

// objToSchema create a service schema based on a object.
//checks if
func objToSchema(obj interface{}) (moleculer.ServiceSchema, error) {
	schema := moleculer.ServiceSchema{}
	name, err := getName(obj)
	if err != nil {
		return schema, err
	}
	schema.Name = name
	schema = copyVersion(obj, schema)
	schema = copyDependencies(obj, schema)
	schema = copyEvents(obj, schema)
	schema = copyMetadata(obj, schema)
	schema = copyMixins(obj, schema)
	schema = copySettings(obj, schema)
	schema.Actions = mergeActions(extractActions(obj), extractActions(&obj))
	schema.Created = extractCreated(obj)
	schema.Started = extractStarted(obj)
	schema.Stopped = extractStopped(obj)
	return schema, nil
}

func mergeActions(actions ...[]moleculer.Action) []moleculer.Action {
	r := []moleculer.Action{}
	for _, list := range actions {
		for _, a := range list {
			r = append(r, a)
		}
	}
	return r
}

// FromObject creates a service based on an object.
func FromObject(obj interface{}, bkr *moleculer.BrokerDelegates) (*Service, error) {
	schema, err := objToSchema(obj)
	if err != nil {
		return nil, err
	}
	return FromSchema(schema, bkr), nil
}

func serviceLogger(bkr *moleculer.BrokerDelegates, schema moleculer.ServiceSchema) *log.Entry {
	return bkr.Logger("service", schema.Name)
}

func FromSchema(schema moleculer.ServiceSchema, bkr *moleculer.BrokerDelegates) *Service {
	if len(schema.Mixins) > 0 {
		schema = applyMixins(schema)
	}
	logger := serviceLogger(bkr, schema)
	service := &Service{schema: &schema, logger: logger}
	service.populateFromSchema()
	if service.name == "" {
		panic(errors.New("Service name can't be empty! Maybe it is not a valid Service schema."))
	}
	if service.created != nil {
		go service.created((*service.schema), service.logger)
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
func (service *Service) Start(context moleculer.BrokerContext) {
	if service.started != nil {
		service.schema.Settings = service.settings
		service.schema.Metadata = service.metadata
		service.started(context, (*service.schema))
	}
}

// Stop called by the broker when the service is stopping.
func (service *Service) Stop(context moleculer.BrokerContext) {
	if service.stopped != nil {
		service.stopped(context, (*service.schema))
	}
}

var actionHandlerTemplates = []aHandlerTemplate{
	//Complete action
	{
		match: func(obj interface{}) bool {
			_, valid := obj.(func(moleculer.Context, moleculer.Payload) interface{})
			return valid
		},
		wrap: func(m reflect.Value, obj interface{}) moleculer.ActionHandler {
			return obj.(func(moleculer.Context, moleculer.Payload) interface{})
		},
	},
	{
		match: func(obj interface{}) bool {
			_, valid := obj.(func(moleculer.Context, moleculer.Payload) moleculer.Payload)
			return valid
		},
		wrap: func(m reflect.Value, obj interface{}) moleculer.ActionHandler {
			ah := obj.(func(moleculer.Context, moleculer.Payload) moleculer.Payload)
			return func(ctx moleculer.Context, p moleculer.Payload) interface{} {
				return ah(ctx, p)
			}
		},
	},
	//Context, params NO return
	{
		match: func(obj interface{}) bool {
			_, valid := obj.(func(moleculer.Context, moleculer.Payload))
			return valid
		},
		wrap: func(m reflect.Value, obj interface{}) moleculer.ActionHandler {
			ah := obj.(func(moleculer.Context, moleculer.Payload))
			return func(ctx moleculer.Context, p moleculer.Payload) interface{} {
				ah(ctx, p)
				return nil
			}
		},
	},
	//Just context
	{
		match: func(obj interface{}) bool {
			_, valid := obj.(func(moleculer.Context) interface{})
			return valid
		},
		wrap: func(m reflect.Value, obj interface{}) moleculer.ActionHandler {
			ah := obj.(func(moleculer.Context) interface{})
			return func(ctx moleculer.Context, p moleculer.Payload) interface{} {
				return ah(ctx)
			}
		},
	},
	//Just context, NO return
	{
		match: func(obj interface{}) bool {
			_, valid := obj.(func(moleculer.Context))
			return valid
		},
		wrap: func(m reflect.Value, obj interface{}) moleculer.ActionHandler {
			ah := obj.(func(moleculer.Context))
			return func(ctx moleculer.Context, p moleculer.Payload) interface{} {
				ah(ctx)
				return nil
			}
		},
	},

	//Just params
	{
		match: func(obj interface{}) bool {
			_, valid := obj.(func(moleculer.Payload) interface{})
			return valid
		},
		wrap: func(m reflect.Value, obj interface{}) moleculer.ActionHandler {
			ah := obj.(func(moleculer.Payload) interface{})
			return func(ctx moleculer.Context, p moleculer.Payload) interface{} {
				return ah(p)
			}
		},
	},
	//Just params, NO return
	{
		match: func(obj interface{}) bool {
			_, valid := obj.(func(moleculer.Payload))
			return valid
		},
		wrap: func(m reflect.Value, obj interface{}) moleculer.ActionHandler {
			ah := obj.(func(moleculer.Payload))
			return func(ctx moleculer.Context, p moleculer.Payload) interface{} {
				ah(p)
				return nil
			}
		},
	},

	//no args
	{
		match: func(obj interface{}) bool {
			_, valid := obj.(func() interface{})
			return valid
		},
		wrap: func(m reflect.Value, obj interface{}) moleculer.ActionHandler {
			ah := obj.(func() interface{})
			return func(ctx moleculer.Context, p moleculer.Payload) interface{} {
				return ah()
			}
		},
	},
	//no args, no return
	{
		match: func(obj interface{}) bool {
			_, valid := obj.(func())
			return valid
		},
		wrap: func(m reflect.Value, obj interface{}) moleculer.ActionHandler {
			ah := obj.(func())
			return func(ctx moleculer.Context, p moleculer.Payload) interface{} {
				ah()
				return nil
			}
		},
	},
}
