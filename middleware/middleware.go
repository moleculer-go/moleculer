package middleware

type MiddlewareHandler struct {
}

func (middlewareHandler *MiddlewareHandler) CallHandlers(name string, broker interface{}) {
	//called from broker start
}
