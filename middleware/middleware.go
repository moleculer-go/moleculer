package middleware

import (
	"context"

	"github.com/moleculer-go/moleculer/params"
	. "github.com/moleculer-go/moleculer/service"
)

type MiddlewareHandler struct {
}

func (middlewareHandler *MiddlewareHandler) CallHandlers(name string, broker interface{}) {
	//called from broker start
}

func (middlewareHandler *MiddlewareHandler) WrapHandler(name string, actionHandler ActionHandler, serviceAction *ServiceAction) ActionHandler {
	return func(ctx context.Context, params params.Params) interface{} {
		//TODO : call middle wares
		return actionHandler(ctx, params)
	}
}
