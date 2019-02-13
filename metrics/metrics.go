package metrics

import (
	"github.com/moleculer-go/moleculer"
)

func metricStart(context moleculer.BrokerContext) {

}

func metricEnd(context moleculer.BrokerContext) {
	//context.
}

func shouldMetric(context moleculer.BrokerContext) bool {
	//context.
}

// Middleware create a metrics middleware
func Middlewares() moleculer.Middlewares {
	return map[string]moleculer.MiddlewareHandler{
		"afterLocalAction": func(params interface{}, next func(...interface{})) {
			context := params.(moleculer.BrokerContext)
			if shouldMetric(context) {
				metricEnd(context)
			}
			next()
		},
		"beforeLocalAction": func(params interface{}, next func(...interface{})) {
			context := params.(moleculer.BrokerContext)
			if shouldMetric(context) {
				metricStart(context)
			}
			next()
		},
	}
}
