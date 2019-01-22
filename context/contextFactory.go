package middleware

import (
	. "github.com/moleculer-go/moleculer/common"

	log "github.com/sirupsen/logrus"
)

type actionDelegateFunc func(context *Context, opts ...OptionsFunc) chan interface{}
type eventDelegateFunc func(context *Context, groups ...string)
type getLoggerFunction func(name string, value string) *log.Entry

type ContextImpl struct {
	actionDelegate    actionDelegateFunc
	emitDelegate      eventDelegateFunc
	broadcastDelegate eventDelegateFunc
	node              *Node
	parent            *ContextImpl
	actionName        string
	eventName         string
	getLogger         getLoggerFunction
	params            interface{}
}

func CreateContext(actionDelegate actionDelegateFunc, emitDelegate eventDelegateFunc, broadcastDelegate eventDelegateFunc, getLogger getLoggerFunction, node *Node) Context {
	context := ContextImpl{
		actionDelegate:    actionDelegate,
		emitDelegate:      emitDelegate,
		broadcastDelegate: broadcastDelegate,
		getLogger:         getLogger,
		node:              node,
	}
	return context
}

// NewActionContext : create a new context for a specific action
func (context ContextImpl) NewActionContext(actionName string, params interface{}, opts ...OptionsFunc) Context {
	actionContext := ContextImpl{
		actionDelegate:    context.actionDelegate,
		emitDelegate:      context.emitDelegate,
		broadcastDelegate: context.broadcastDelegate,
		actionName:        actionName,
		params:            params,
	}
	actionContext.parent = &context

	return actionContext
}

// Max calling level check to avoid calling loops
func checkMaxCalls(context *ContextImpl) {

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

func (context ContextImpl) GetParams() interface{} {
	return context.params
}
