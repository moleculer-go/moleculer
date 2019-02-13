package context

import (
	"errors"
	"fmt"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/options"
	"github.com/moleculer-go/moleculer/payload"
	"github.com/moleculer-go/moleculer/util"

	log "github.com/sirupsen/logrus"
)

type Context struct {
	id           string
	broker       moleculer.BrokerDelegates
	targetNodeID string
	parentID     string
	actionName   string
	eventName    string
	groups       []string
	broadcast    bool
	params       moleculer.Payload
	meta         *map[string]interface{}
	timeout      int
	level        int
}

func BrokerContext(broker moleculer.BrokerDelegates) moleculer.BrokerContext {
	localNodeID := broker.LocalNode().GetID()
	id := fmt.Sprint("rootContext-broker-", localNodeID, "-", util.RandomString(12))
	context := Context{
		id:       id,
		broker:   broker,
		level:    1,
		parentID: "ImGroot;)",
	}
	return &context
}

// ChildEventContext : create a child context for a specific event call.
func (context *Context) ChildEventContext(eventName string, params moleculer.Payload, groups []string, broadcast bool) moleculer.BrokerContext {
	parentContext := context
	meta := parentContext.meta
	if meta == nil {
		metaMap := make(map[string]interface{})
		meta = &metaMap
	}
	if context.broker.Config.Metrics {
		(*meta)["metrics"] = true
	}
	eventContext := Context{
		id:        util.RandomString(12),
		broker:    parentContext.broker,
		eventName: eventName,
		groups:    groups,
		params:    params,
		broadcast: broadcast,
		level:     parentContext.level + 1,
		meta:      meta,
		parentID:  parentContext.id,
	}
	return &eventContext
}

// ChildActionContext : create a chiold context for a specific action call.
func (context *Context) ChildActionContext(actionName string, params moleculer.Payload, opts ...moleculer.OptionsFunc) moleculer.BrokerContext {
	parentContext := context
	meta := parentContext.meta
	if meta == nil {
		metaMap := make(map[string]interface{})
		meta = &metaMap
	}
	if context.broker.Config.Metrics {
		(*meta)["metrics"] = true
	}
	actionContext := Context{
		id:         util.RandomString(12),
		broker:     parentContext.broker,
		actionName: actionName,
		params:     params,
		level:      parentContext.level + 1,
		meta:       meta,
		parentID:   parentContext.id,
	}
	return &actionContext
}

// Max calling level check to avoid calling loops
func checkMaxCalls(context *Context) {

}

// ActionContext create an action context for remote call.
func ActionContext(broker moleculer.BrokerDelegates, values map[string]interface{}) moleculer.BrokerContext {
	var level int
	var parentID string
	var timeout int
	var meta map[string]interface{}

	sourceNodeID := values["sender"].(string)
	id := values["id"].(string)
	actionName, isAction := values["action"]
	if !isAction {
		panic(errors.New("Can't create an action context, you need a action field!"))
	}
	level = values["level"].(int)
	parentID = values["parentID"].(string)
	params := payload.Create(values["params"])

	if values["timeout"] != nil {
		timeout = values["timeout"].(int)
	}
	if values["meta"] != nil {
		meta = values["meta"].(map[string]interface{})
	}

	newContext := Context{
		broker:       broker,
		targetNodeID: sourceNodeID,
		id:           id,
		actionName:   actionName.(string),
		parentID:     parentID,
		params:       params,
		meta:         &meta,
		timeout:      timeout,
		level:        level,
	}

	return &newContext
}

// ChildEventContext create an event context for a remote call.
func ChildEventContext(broker moleculer.BrokerDelegates, values map[string]interface{}) moleculer.BrokerContext {
	var level int
	var parentID string
	var timeout int
	var meta map[string]interface{}

	sourceNodeID := values["sender"].(string)
	id := values["id"].(string)
	eventName, isEvent := values["event"]
	if !isEvent {
		panic(errors.New("Can't create an event context, you need an event field!"))
	}
	params := payload.Create(values["params"])

	newContext := Context{
		broker:       broker,
		targetNodeID: sourceNodeID,
		id:           id,
		eventName:    eventName.(string),
		broadcast:    values["broadcast"].(bool),
		parentID:     parentID,
		params:       params,
		meta:         &meta,
		timeout:      timeout,
		level:        level,
	}
	if values["groups"] != nil {
		newContext.groups = values["groups"].([]string)
	}
	return &newContext
}

func (context *Context) IsBroadcast() bool {
	return context.broadcast
}

// AsMap : export context info in a map[string]
func (context *Context) AsMap() map[string]interface{} {
	mapResult := make(map[string]interface{})

	var metrics interface{}
	if context.meta != nil {
		metrics = (*context.meta)["metrics"]
	}

	mapResult["id"] = context.id
	mapResult["params"] = context.params.Value()
	if context.actionName != "" {
		mapResult["action"] = context.actionName
		mapResult["level"] = context.level
		mapResult["metrics"] = metrics
		mapResult["parentID"] = context.parentID
		mapResult["meta"] = context.meta
		mapResult["timeout"] = context.timeout
	}
	if context.eventName != "" {
		mapResult["event"] = context.eventName
		mapResult["groups"] = context.groups
		mapResult["broadcast"] = context.broadcast
	}

	//TODO : check how to support streaming params in go
	mapResult["stream"] = false

	return mapResult
}

// Call : main entry point to call actions.
// chained action invocation
func (context *Context) Call(actionName string, params interface{}, opts ...moleculer.OptionsFunc) chan moleculer.Payload {
	actionContext := context.ChildActionContext(actionName, payload.Create(params), options.Wrap(opts))
	return context.broker.ActionDelegate(actionContext, options.Wrap(opts))
}

// Emit : Emit an event (grouped & balanced global event)
func (context *Context) Emit(eventName string, params interface{}, groups ...string) {
	context.Logger().Debug("Context Emit() eventName: ", eventName)
	newContext := context.ChildEventContext(eventName, payload.Create(params), groups, false)
	context.broker.EmitEvent(newContext)
}

// Broadcast : Broadcast an event for all local & remote services
func (context *Context) Broadcast(eventName string, params interface{}, groups ...string) {
	newContext := context.ChildEventContext(eventName, payload.Create(params), groups, true)
	context.broker.EmitEvent(newContext)
}

func (context *Context) ActionName() string {
	return context.actionName
}

func (context *Context) EventName() string {
	return context.eventName
}

func (context *Context) Groups() []string {
	return context.groups
}

func (context *Context) Payload() moleculer.Payload {
	return context.params
}

func (context *Context) SetTargetNodeID(targetNodeID string) {
	context.Logger().Debug("context factory SetTargetNodeID() targetNodeID: ", targetNodeID)
	context.targetNodeID = targetNodeID
}

func (context *Context) TargetNodeID() string {
	return context.targetNodeID
}

func (context *Context) ID() string {
	return context.id
}

func (context *Context) Meta() *map[string]interface{} {
	return context.meta
}

func (context *Context) Logger() *log.Entry {
	if context.actionName != "" {
		return context.broker.Logger("action", context.actionName)
	}
	return context.broker.Logger("context", "<root>")
}
