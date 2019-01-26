package middleware

import (
	. "github.com/moleculer-go/moleculer/common"
	. "github.com/moleculer-go/moleculer/params"
	. "github.com/moleculer-go/moleculer/util"

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
	parentContext := context.self
	actionContext := ContextImpl{
		id:                RandomString(12),
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
	actionContext.parent = parentContext
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

	mapResult["id"] = context.self.id
	mapResult["action"] = context.self.actionName
	mapResult["params"] = context.self.params
	mapResult["meta"] = context.self.meta
	mapResult["timeout"] = context.self.timeout
	mapResult["level"] = context.self.level
	mapResult["metrics"] = context.self.sendMetrics
	if context.self.parent != nil {
		mapResult["parentID"] = context.self.parent.id
	}
	if context.self.requestID != "" {
		mapResult["requestID"] = context.self.requestID
	}
	//TODO : check how to support streaming params in go
	mapResult["stream"] = false

	return mapResult
}

// InvokeAction : check max calls and call broker action delegate
func (context ContextImpl) InvokeAction(opts ...OptionsFunc) chan interface{} {
	checkMaxCalls(&context)
	var contextInterface Context = context
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

func (context ContextImpl) SetNode(node *Node) {
	context.self.node = node
}

func (context ContextImpl) GetNode() *Node {
	return context.self.node
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
