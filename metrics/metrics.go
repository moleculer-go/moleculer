package metrics

import (
	"fmt"
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/context"
	"github.com/moleculer-go/moleculer/middleware"
)

func metricEnd(brokerContext moleculer.BrokerContext, result moleculer.Payload) {
	rawContext := brokerContext.(*context.Context)
	if rawContext.Meta() == nil || (*rawContext.Meta())["startTime"] == nil {
		return
	}

	startTime := (*rawContext.Meta())["startTime"].(time.Time)
	payload := metricsPayload(brokerContext)

	mlseconds := float64(time.Since(startTime).Nanoseconds()) / 1000000
	payload["duration"] = mlseconds
	payload["endTime"] = time.Now().Format(time.RFC3339)
	if result.IsError() {
		payload["error"] = map[string]string{
			"message": fmt.Sprintf("%s", result.Error()),
		}
	}
	rawContext.Emit("metrics.trace.span.finish", payload)
}

func metricStart(context moleculer.BrokerContext) {
	(*context.Meta())["startTime"] = time.Now()
	(*context.Meta())["duration"] = 0
	context.(moleculer.Context).Emit("metrics.trace.span.start", metricsPayload(context))
}

// metricsPayload generate the payload for the metrics event
func metricsPayload(brokerContext moleculer.BrokerContext) map[string]interface{} {
	rawContext := brokerContext.(*context.Context)
	contextMap := brokerContext.AsMap()
	if rawContext.Meta() != nil && (*rawContext.Meta())["startTime"] != nil {
		contextMap["startTime"] = (*rawContext.Meta())["startTime"].(time.Time).Format(time.RFC3339)
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
		svc := rawContext.BrokerDelegates().ServiceForAction(action)
		contextMap["action"] = map[string]string{"name": action}
		contextMap["service"] = map[string]string{"name": svc.Name, "version": svc.Version}
	}
	return contextMap
}

// shouldMetric check if it should metric for this context.
func createShouldMetric(brokerConfig moleculer.BrokerConfig) func(context moleculer.BrokerContext) bool {
	var callsCount float32 = 0
	return func(context moleculer.BrokerContext) bool {
		if context.Meta() != nil && (*context.Meta())["metrics"] != nil && (*context.Meta())["metrics"].(bool) {
			//fmt.Println("createShouldMetric() metrics is on inside meta!")
			callsCount++
			if callsCount*brokerConfig.MetricsRate >= 1.0 {
				//fmt.Println("createShouldMetric() rate is good!")

				callsCount = 0
				return true
			}
		}
		// fmt.Println("createShouldMetric() metris is NOT ON :( ")
		// fmt.Println("createShouldMetric() context.Meta() -> ", context.Meta())
		return false
	}
}

// Middleware create a metrics middleware
func Middlewares() moleculer.Middlewares {
	var brokerConfig = moleculer.DefaultConfig
	shouldMetric := createShouldMetric(brokerConfig)
	return map[string]moleculer.MiddlewareHandler{
		// store the broker config
		"brokerConfig": func(params interface{}, next func(...interface{})) {
			brokerConfig = params.(moleculer.BrokerConfig)
			shouldMetric = createShouldMetric(brokerConfig)
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
			//fmt.Println("metrics... beforeLocalAction...")
			context := params.(moleculer.BrokerContext)
			if shouldMetric(context) {
				metricStart(context)
			}
			next()
		},
	}
}
