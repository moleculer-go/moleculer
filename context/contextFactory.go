package context

import (
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
	sendMetrics  bool
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

// EventContext : create a new context for a specific event call.
func (context *Context) EventContext(eventName string, params moleculer.Payload, groups []string, broadcast bool) moleculer.BrokerContext {
	parentContext := context
	eventContext := Context{
		id:          util.RandomString(12),
		broker:      parentContext.broker,
		eventName:   eventName,
		groups:      groups,
		params:      params,
		broadcast:   broadcast,
		level:       parentContext.level + 1,
		sendMetrics: parentContext.sendMetrics,
		parentID:    parentContext.id,
	}
	return &eventContext
}

// NewActionContext : create a new context for a specific action call.
func (context *Context) NewActionContext(actionName string, params moleculer.Payload, opts ...moleculer.OptionsFunc) moleculer.BrokerContext {
	parentContext := context
	actionContext := Context{
		id:          util.RandomString(12),
		broker:      parentContext.broker,
		actionName:  actionName,
		params:      params,
		level:       parentContext.level + 1,
		sendMetrics: parentContext.sendMetrics,
		parentID:    parentContext.id,
	}
	return &actionContext
}

// Max calling level check to avoid calling loops
func checkMaxCalls(context *Context) {

}

// RemoteContext create a context for a remote call
func RemoteContext(broker moleculer.BrokerDelegates, values map[string]interface{}) moleculer.BrokerContext {
	var level int
	var sendMetrics bool
	var parentID string
	var timeout int
	var meta map[string]interface{}

	sourceNodeID := values["sender"].(string)
	id := values["id"].(string)
	actionName, isAction := values["action"]
	eventName, isEvent := values["event"]

	if isAction {
		level = values["level"].(int)
		sendMetrics = values["metrics"].(bool)
		parentID = values["parentID"].(string)
	}
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
		parentID:     parentID,
		params:       params,
		meta:         &meta,
		timeout:      timeout,
		level:        level,
		sendMetrics:  sendMetrics,
	}

	if isAction {
		newContext.actionName = actionName.(string)
	} else if isEvent {
		newContext.eventName = eventName.(string)
		newContext.groups = values["groups"].([]string)
		newContext.broadcast = values["broadcast"].(bool)
	}
	return &newContext
}

// AsMap : export context info in a map[string]
func (context *Context) AsMap() map[string]interface{} {
	mapResult := make(map[string]interface{})

	mapResult["id"] = context.id
	mapResult["params"] = context.params.Value()
	if context.actionName != "" {
		mapResult["action"] = context.actionName
		mapResult["level"] = context.level
		mapResult["metrics"] = context.sendMetrics
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
	actionContext := context.NewActionContext(actionName, payload.Create(params), options.Wrap(opts))
	return context.broker.ActionDelegate(actionContext, options.Wrap(opts))
}

// Emit : Emit an event (grouped & balanced global event)
func (context *Context) Emit(eventName string, params interface{}, groups ...string) {
	newContext := context.EventContext(eventName, payload.Create(params), groups, false)
	context.broker.EventDelegate(newContext)
}

// Broadcast : Broadcast an event for all local & remote services
func (context *Context) Broadcast(eventName string, params interface{}, groups ...string) {
	newContext := context.EventContext(eventName, payload.Create(params), groups, true)
	context.broker.EventDelegate(newContext)
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
