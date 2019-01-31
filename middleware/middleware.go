package middleware

import (
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/service"
)

type MiddlewareHandler struct {
}

func (middlewareHandler *MiddlewareHandler) CallHandlers(name string, broker interface{}) {
	//called from broker start
}

func (middlewareHandler *MiddlewareHandler) WrapHandler(name string, actionHandler moleculer.ActionHandler, serviceAction *service.Action) moleculer.ActionHandler {
	return func(ctx moleculer.Context, params moleculer.Params) interface{} {
		//TODO : call middle wares
		return actionHandler(ctx, params)
	}
}
