package middleware

import (
	. "github.com/moleculer-go/moleculer/common"
	. "github.com/moleculer-go/moleculer/params"
	. "github.com/moleculer-go/moleculer/service"
)

type MiddlewareHandler struct {
}

func (middlewareHandler *MiddlewareHandler) CallHandlers(name string, broker interface{}) {
	//called from broker start
}

func (middlewareHandler *MiddlewareHandler) WrapHandler(name string, actionHandler ActionHandler, serviceAction *ServiceAction) ActionHandler {
	return func(ctx Context, params Params) interface{} {
		//TODO : call middle wares
		return actionHandler(ctx, params)
	}
}
