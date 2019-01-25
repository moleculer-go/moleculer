package middleware

import (
	. "github.com/moleculer-go/moleculer/common"
	. "github.com/moleculer-go/moleculer/params"

	log "github.com/sirupsen/logrus"
)

type actionDelegateFunc func(context *Context, opts ...OptionsFunc) chan interface{}
type eventDelegateFunc func(context *Context, groups ...string)
type getLoggerFunction func(name string, value string) *log.Entry

type ContextImpl struct {
	self              *ContextImpl
	actionDelegate    actionDelegateFunc
	emitDelegate      eventDelegateFunc
	broadcastDelegate eventDelegateFunc
	node              *Node
	parent            *ContextImpl
	actionName        string
	eventName         string
	getLogger         getLoggerFunction
	params            interface{}
	id                string
	meta              map[string]interface{}
	timeout           int
	level             int
	sendMetrics       bool
	requestID         string
}

func CreateBrokerContext(actionDelegate actionDelegateFunc, emitDelegate eventDelegateFunc, broadcastDelegate eventDelegateFunc, getLogger getLoggerFunction, node *Node) Context {
	context := ContextImpl{
		actionDelegate:    actionDelegate,
		emitDelegate:      emitDelegate,
		broadcastDelegate: broadcastDelegate,
		getLogger:         getLogger,
		node:              node,
		level:             1,
		requestID:         "",
	}
	context.self = &context
	return context
}

// NewActionContext : create a new context for a specific action call
func (context ContextImpl) NewActionContext(actionName string, params interface{}, opts ...OptionsFunc) Context {
	parentContext := context
	actionContext := ContextImpl{
		actionDelegate:    parentContext.actionDelegate,
		emitDelegate:      parentContext.emitDelegate,
		broadcastDelegate: parentContext.broadcastDelegate,
		getLogger:         parentContext.getLogger,
		node:              parentContext.node,
		actionName:        actionName,
		params:            params,
		level:             parentContext.level + 1,
		sendMetrics:       parentContext.sendMetrics,
	}
	if parentContext.requestID != "" {
		actionContext.requestID = parentContext.requestID
	}
	actionContext.self = &actionContext
	actionContext.parent = &parentContext
	return actionContext
}

// Max calling level check to avoid calling loops
func checkMaxCalls(context *ContextImpl) {

}

func CreateContext(id, actionName string, params interface{}, meta map[string]interface{}, level int, sendMetrics bool, timeout int, parentID, requestID string) Context {

	parentContext := ContextImpl{
		id: parentID,
	}

	actionContext := ContextImpl{
		// actionDelegate:    parentContext.actionDelegate,
		// emitDelegate:      parentContext.emitDelegate,
		// broadcastDelegate: parentContext.broadcastDelegate,
		// getLogger:         parentContext.getLogger,
		// node:              parentContext.node,

		id:          id,
		actionName:  actionName,
		params:      params,
		meta:        meta,
		timeout:     timeout,
		level:       level,
		sendMetrics: sendMetrics,
		requestID:   requestID,
	}

	actionContext.self = &actionContext
	actionContext.parent = &parentContext
	return actionContext
}

// AsMap : export context info in a map[string]
func (context ContextImpl) AsMap() map[string]interface{} {
	mapResult := make(map[string]interface{})

	mapResult["id"] = context.id
	mapResult["action"] = context.actionName
	mapResult["params"] = context.params
	mapResult["meta"] = context.meta
	mapResult["timeout"] = context.timeout
	mapResult["level"] = context.level
	mapResult["metrics"] = context.sendMetrics
	if context.parent != nil {
		mapResult["parentID"] = context.parent.id
	}
	if context.requestID != "" {
		mapResult["requestID"] = context.requestID
	}
	//TODO : check how to support streaming params in go
	mapResult["stream"] = false

	return mapResult
}

// InvokeAction : check max calls and call broker action delegate
func (context ContextImpl) InvokeAction(opts ...OptionsFunc) chan interface{} {
	checkMaxCalls(&context)
	var contextInterface Context = context
	return context.actionDelegate(&contextInterface, WrapOptions(opts))
}

// Call : main entry point to call actions.
// chained action invocation
func (context ContextImpl) Call(actionName string, params interface{}, opts ...OptionsFunc) chan interface{} {
	actionContext := context.NewActionContext(actionName, params, WrapOptions(opts))
	return actionContext.InvokeAction(WrapOptions(opts))
}

// Emit : Emit an event (grouped & balanced global event)
func (context ContextImpl) Emit(eventName string, params interface{}, groups ...string) {

}

// Broadcast : Broadcast an event for all local & remote services
func (context ContextImpl) Broadcast(eventName string, params interface{}, groups ...string) {

}

func (context ContextImpl) GetActionName() string {
	return context.actionName
}

func (context ContextImpl) GetParams() Params {
	return CreateParams(&context.params)
}

func (context ContextImpl) GetLogger() *log.Entry {
	if context.actionName != "" {
		return context.getLogger("action", context.actionName)
	}
	return context.getLogger("context", "<root>")
}
