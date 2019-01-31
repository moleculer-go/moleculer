package middleware

import (
	"fmt"

	. "github.com/moleculer-go/moleculer/common"
	. "github.com/moleculer-go/moleculer/params"
	. "github.com/moleculer-go/moleculer/util"

	log "github.com/sirupsen/logrus"
)

type ContextImpl struct {
	self              *ContextImpl
	id                string
	actionDelegate    ActionDelegateFunc
	emitDelegate      EventDelegateFunc
	broadcastDelegate EventDelegateFunc
	localNodeID       string
	targetNodeID      string
	parentID          string
	actionName        string
	eventName         string
	getLogger         GetLoggerFunction
	params            interface{}
	meta              map[string]interface{}
	timeout           int
	level             int
	sendMetrics       bool
}

func CreateBrokerContext(actionDelegate ActionDelegateFunc, emitDelegate EventDelegateFunc, broadcastDelegate EventDelegateFunc, getLogger GetLoggerFunction, localNodeID string) Context {
	id := fmt.Sprint("rootContext-broker-", localNodeID, "-", RandomString(12))
	context := ContextImpl{
		id:                id,
		actionDelegate:    actionDelegate,
		emitDelegate:      emitDelegate,
		broadcastDelegate: broadcastDelegate,
		getLogger:         getLogger,
		localNodeID:       localNodeID,
		level:             1,
		parentID:          "ImGroot;)",
	}
	context.self = &context
	return context
}

// NewActionContext : create a new context for a specific action call
func (context ContextImpl) NewActionContext(actionName string, params interface{}, opts ...OptionsFunc) Context {
	parentContext := context.self
	actionContext := ContextImpl{
		id:                RandomString(12),
		actionDelegate:    parentContext.actionDelegate,
		emitDelegate:      parentContext.emitDelegate,
		broadcastDelegate: parentContext.broadcastDelegate,
		getLogger:         parentContext.getLogger,
		localNodeID:       parentContext.localNodeID,
		actionName:        actionName,
		params:            params,
		level:             parentContext.level + 1,
		sendMetrics:       parentContext.sendMetrics,
		parentID:          parentContext.id,
	}
	actionContext.self = &actionContext
	return actionContext
}

// Max calling level check to avoid calling loops
func checkMaxCalls(context *ContextImpl) {

}

func CreateContext(broker *BrokerInfo, values map[string]interface{}) Context {

	actionDelegate, emitDelegate, broadcastDelegate := broker.GetDelegates()

	//TODO check on moleculer JS if in the request the sender is sent.
	sourceNodeID := values["sender"].(string)
	id := values["id"].(string)
	actionName := values["action"].(string)
	params := values["params"]
	level := values["level"].(int)
	sendMetrics := values["metrics"].(bool)
	parentID := values["parentID"].(string)

	var timeout int
	var meta map[string]interface{}
	if values["timeout"] != nil {
		timeout = values["timeout"].(int)
	}
	if values["meta"] != nil {
		meta = values["meta"].(map[string]interface{})
	}

	localNodeID := (*broker.GetLocalNode()).GetID()

	actionContext := ContextImpl{
		actionDelegate:    actionDelegate,
		emitDelegate:      emitDelegate,
		broadcastDelegate: broadcastDelegate,
		getLogger:         broker.GetLogger,
		localNodeID:       localNodeID,
		targetNodeID:      sourceNodeID,
		id:                id,
		parentID:          parentID,
		actionName:        actionName,
		params:            params,
		meta:              meta,
		timeout:           timeout,
		level:             level,
		sendMetrics:       sendMetrics,
	}

	actionContext.self = &actionContext
	return actionContext
}

// AsMap : export context info in a map[string]
func (context ContextImpl) AsMap() map[string]interface{} {
	mapResult := make(map[string]interface{})

	mapResult["id"] = context.self.id
	mapResult["action"] = context.self.actionName
	mapResult["params"] = context.self.params
	mapResult["meta"] = context.self.meta
	mapResult["timeout"] = context.self.timeout
	mapResult["level"] = context.self.level
	mapResult["metrics"] = context.self.sendMetrics
	mapResult["parentID"] = context.self.parentID

	//TODO : check how to support streaming params in go
	mapResult["stream"] = false

	return mapResult
}

// InvokeAction : check max calls and call broker action delegate
func (context ContextImpl) InvokeAction(opts ...OptionsFunc) chan interface{} {
	checkMaxCalls(&context)
	var contextInterface Context = (*context.self)
	return context.self.actionDelegate(&contextInterface, WrapOptions(opts))
}

// Call : main entry point to call actions.
// chained action invocation
func (context ContextImpl) Call(actionName string, params interface{}, opts ...OptionsFunc) chan interface{} {
	actionContext := context.self.NewActionContext(actionName, params, WrapOptions(opts))
	return actionContext.InvokeAction(WrapOptions(opts))
}

// Emit : Emit an event (grouped & balanced global event)
func (context ContextImpl) Emit(eventName string, params interface{}, groups ...string) {

}

// Broadcast : Broadcast an event for all local & remote services
func (context ContextImpl) Broadcast(eventName string, params interface{}, groups ...string) {

}

func (context ContextImpl) GetActionName() string {
	return context.self.actionName
}

func (context ContextImpl) GetParams() Params {
	return CreateParams(&context.self.params)
}

func (context ContextImpl) SetTargetNodeID(targetNodeID string) {
	fmt.Println("context factory SetTargetNodeID() targetNodeID: ", targetNodeID)
	context.self.targetNodeID = targetNodeID
}

func (context ContextImpl) GetTargetNodeID() string {
	return context.self.targetNodeID
}

func (context ContextImpl) GetID() string {
	return context.self.id
}

func (context ContextImpl) GetMeta() map[string]interface{} {
	return context.self.meta
}

func (context ContextImpl) GetLogger() *log.Entry {
	if context.self.actionName != "" {
		return context.self.getLogger("action", context.self.actionName)
	}
	return context.self.getLogger("context", "<root>")
}
