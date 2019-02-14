package metrics

import (
	"github.com/moleculer-go/moleculer"
)

func metricStart(context moleculer.BrokerContext) {

}

func metricEnd(context moleculer.BrokerContext) {
	//context.
}

var callsCount = 0
var brokerConfig = moleculer.BrokerConfig{}

// shouldMetric check if it should metric for this context.
func shouldMetric(context moleculer.BrokerContext) bool {
	if context.Meta() != nil && (*context.Meta())["metrics"] != nil && (*context.Meta())["metrics"].(bool) {
		callsCount++
		if float32(callsCount)*brokerConfig.MetricsRate >= 1.0 {
			callsCount = 0
			return true
		}
	}
	return false
}

// Middleware create a metrics middleware
func Middlewares() moleculer.Middlewares {
	return map[string]moleculer.MiddlewareHandler{
		// store the broker config
		"brokerConfig": func(params interface{}, next func(...interface{})) {
			brokerConfig = params.(moleculer.BrokerConfig)
			next()
		},
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
