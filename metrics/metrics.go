package metrics

import (
	"github.com/moleculer-go/moleculer"
)

/**
 * Start metrics & send metric event.
 *
 * @param {Context} ctx
 *
 * @private
 */
//  function metricStart(ctx) {
// 	ctx.startTime = Date.now();
// 	ctx.startHrTime = process.hrtime();
// 	ctx.duration = 0;

// 	if (ctx.metrics) {
// 		const payload = generateMetricPayload(ctx);
// 		ctx.broker.emit("metrics.trace.span.start", payload);
// 	}
// }

// /**
//  * Generate metrics payload
//  *
//  * @param {Context} ctx
//  * @returns {Object}
//  */
// function generateMetricPayload(ctx) {
// 	let payload = {
// 		id: ctx.id,
// 		requestID: ctx.requestID,
// 		level: ctx.level,
// 		startTime: ctx.startTime,
// 		remoteCall: ctx.nodeID !== ctx.broker.nodeID
// 	};

// 	// Process extra metrics
// 	processExtraMetrics(ctx, payload);

// 	if (ctx.action) {
// 		payload.action = {
// 			name: ctx.action.name
// 		};
// 	}
// 	if (ctx.service) {
// 		payload.service = {
// 			name: ctx.service.name,
// 			version: ctx.service.version
// 		};
// 	}

// 	if (ctx.parentID)
// 		payload.parent = ctx.parentID;

// 	payload.nodeID = ctx.broker.nodeID;
// 	if (payload.remoteCall)
// 		payload.callerNodeID = ctx.nodeID;

// 	return payload;
// }

// /**
//  * Stop metrics & send finish metric event.
//  *
//  * @param {Context} ctx
//  * @param {Error} error
//  *
//  * @private
//  */
// function metricFinish(ctx, error) {
// 	if (ctx.startHrTime) {
// 		let diff = process.hrtime(ctx.startHrTime);
// 		ctx.duration = (diff[0] * 1e3) + (diff[1] / 1e6); // milliseconds
// 	}
// 	ctx.stopTime = ctx.startTime + ctx.duration;

// 	if (ctx.metrics) {
// 		const payload = generateMetricPayload(ctx);
// 		payload.endTime = ctx.stopTime;
// 		payload.duration = ctx.duration;
// 		payload.fromCache = ctx.cachedResult;

// 		if (error) {
// 			payload.error = {
// 				name: error.name,
// 				code: error.code,
// 				type: error.type,
// 				message: error.message
// 			};
// 		}

// 		ctx.broker.emit("metrics.trace.span.finish", payload);
// 	}
// }

func metricStart(context moleculer.BrokerContext) {

}

func metricEnd(context moleculer.BrokerContext) {
	//context.
}

var callsCount = 0
var brokerConfig = moleculer.DefaultConfig

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
