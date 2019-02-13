package metrics

import (
	"github.com/moleculer-go/moleculer"
)

func metricStart(context moleculer.BrokerContext) {

}

func metricEnd(context moleculer.BrokerContext) {

}

func beforeLocalAction(params interface{}, next func(...interface{})) {
	context := params.(moleculer.BrokerContext)
	metricStart(context)
	next()
}

func afterLocalAction(params interface{}, next func(...interface{})) {
	context := params.(moleculer.BrokerContext)
	metricEnd(context)
	next()
}

// Middleware create a metrics middleware
func Middlewares() moleculer.Middlewares {
	return map[string]moleculer.MiddlewareHandler{
		"afterLocalAction":  afterLocalAction,
		"beforeLocalAction": beforeLocalAction,
	}
}
