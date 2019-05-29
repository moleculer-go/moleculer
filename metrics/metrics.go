package metrics

import (
	"fmt"
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/context"
	"github.com/moleculer-go/moleculer/middleware"
)

func metricEnd(brokerContext moleculer.BrokerContext, result moleculer.Payload) {
	ctx := brokerContext.(*context.Context)
	if !ctx.Meta().Get("startTime").Exists() {
		return
	}

	startTime := ctx.Meta().Get("startTime").Time()
	payload := metricsPayload(brokerContext)

	mlseconds := float64(time.Since(startTime).Nanoseconds()) / 1000000
	payload["duration"] = mlseconds
	payload["endTime"] = time.Now().Format(time.RFC3339)
	if result.IsError() {
		payload["error"] = map[string]string{
			"message": fmt.Sprintf("%s", result.Error()),
		}
	}
	ctx.Emit("metrics.trace.span.finish", payload)
}

func metricStart(context moleculer.BrokerContext) {
	meta := context.Meta().Add("startTime", time.Now()).Add("duration", 0)
	context.UpdateMeta(meta)
	context.Emit("metrics.trace.span.start", metricsPayload(context))
}

// metricsPayload generate the payload for the metrics event
func metricsPayload(brokerContext moleculer.BrokerContext) map[string]interface{} {
	rawContext := brokerContext.(*context.Context)
	contextMap := brokerContext.AsMap()
	if rawContext.Meta().Get("startTime").Exists() {
		contextMap["startTime"] = rawContext.Meta().Get("startTime").Time().Format(time.RFC3339)
	}
	nodeID := rawContext.BrokerDelegates().LocalNode().GetID()
	contextMap["nodeID"] = nodeID
	if rawContext.SourceNodeID() == nodeID {
		contextMap["remoteCall"] = false
	} else {
		contextMap["remoteCall"] = true
		contextMap["callerNodeID"] = rawContext.SourceNodeID()
	}
	_, isAction := contextMap["action"]
	if isAction {
		action := contextMap["action"].(string)
		svcs := rawContext.BrokerDelegates().ServiceForAction(action)
		contextMap["action"] = map[string]string{"name": action}
		contextMap["service"] = map[string]string{"name": svcs[0].Name, "version": svcs[0].Version}
	}
	return contextMap
}

// shouldMetric check if it should metric for this context.
func createShouldMetric(Config moleculer.Config) func(context moleculer.BrokerContext) bool {
	var callsCount float32 = 0
	return func(context moleculer.BrokerContext) bool {
		if context.Meta().Get("metrics").Bool() {
			callsCount++
			if callsCount*Config.MetricsRate >= 1.0 {

				callsCount = 0
				return true
			}
		}
		return false
	}
}

// Middleware create a metrics middleware
func Middlewares() moleculer.Middlewares {
	var Config = moleculer.DefaultConfig
	shouldMetric := createShouldMetric(Config)
	return map[string]moleculer.MiddlewareHandler{
		// store the broker config
		"Config": func(params interface{}, next func(...interface{})) {
			Config = params.(moleculer.Config)
			shouldMetric = createShouldMetric(Config)
			next()
		},
		"afterLocalAction": func(params interface{}, next func(...interface{})) {
			payload := params.(middleware.AfterActionParams)
			context := payload.BrokerContext
			result := payload.Result
			if shouldMetric(context) {
				metricEnd(context, result)
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
