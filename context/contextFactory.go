package context

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/options"
	"github.com/moleculer-go/moleculer/payload"
	"github.com/moleculer-go/moleculer/util"

	log "github.com/sirupsen/logrus"
)

type Context struct {
	id           string
	requestID    string
	broker       moleculer.BrokerDelegates
	targetNodeID string
	sourceNodeID string
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
	id := util.RandomString(12)
	var requestID string
	if parentContext.requestID != "" {
		requestID = parentContext.requestID
	} else {
		requestID = id
	}
	eventContext := Context{
		id:        id,
		requestID: requestID,
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

// Config return the broker config attached to this context.
func (context *Context) BrokerDelegates() moleculer.BrokerDelegates {
	return context.broker
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
	id := util.RandomString(12)
	var requestID string
	if parentContext.requestID != "" {
		requestID = parentContext.requestID
	} else {
		requestID = id
	}
	actionContext := Context{
		id:         id,
		requestID:  requestID,
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
		sourceNodeID: sourceNodeID,
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

// EventContext create an event context for a remote call.
func EventContext(broker moleculer.BrokerDelegates, values map[string]interface{}) moleculer.BrokerContext {
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
		sourceNodeID: sourceNodeID,
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

func (context *Context) RequestID() string {
	return context.requestID
}

// AsMap : export context info in a map[string]
func (context *Context) AsMap() map[string]interface{} {
	mapResult := make(map[string]interface{})

	var metrics interface{}
	if context.meta != nil {
		metrics = (*context.meta)["metrics"]
	}

	mapResult["id"] = context.id
	mapResult["requestID"] = context.requestID

	mapResult["params"] = context.params.Value()
	mapResult["level"] = context.level
	if context.actionName != "" {
		mapResult["action"] = context.actionName
		mapResult["metrics"] = metrics
		mapResult["parentID"] = context.parentID
		mapResult["meta"] = (*context.meta)
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

func (context *Context) MCall(callMaps map[string]map[string]interface{}) chan map[string]interface{} {
	resultChan := make(chan map[string]interface{})
	results := make(map[string]interface{})
	mutex := sync.Mutex{}
	for callName, details := range callMaps {
		actionName := details["action"].(string)
		params := details["params"]
		go func() {
			mutex.Lock()
			results[callName] = <-context.Call(actionName, params)
			mutex.Unlock()
		}()
	}
	start := time.Now()
	go func() {
		for {
			if len(results) == len(callMaps) {
				resultChan <- results
				break
			} else if time.Since(start) > context.broker.Config.MCallTimeout {
				timeoutError := errors.New("MCall timeout error.")
				context.Logger().Error(timeoutError)

				mutex.Lock()
				for callName, details := range callMaps {
					if results[callName] == nil {
						results[callName] = timeoutError
						context.Logger().Error("MCall timeout error -> actionName: ", details["action"], " params: ", details["params"])
					}
				}
				mutex.Unlock()
				resultChan <- results
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	return resultChan
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

func (context *Context) SourceNodeID() string {
	return context.sourceNodeID
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
