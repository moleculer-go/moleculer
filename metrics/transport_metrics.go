package metrics

import (
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/transit"
)

// TransportMetricsCollector collects and emits transport-specific metrics
type TransportMetricsCollector struct {
	broker    *moleculer.BrokerDelegates
	transport transit.Transport
	lastStats map[string]interface{}
	startTime time.Time
}

// NewTransportMetricsCollector creates a new transport metrics collector
func NewTransportMetricsCollector(broker *moleculer.BrokerDelegates, transport transit.Transport) *TransportMetricsCollector {
	return &TransportMetricsCollector{
		broker:    broker,
		transport: transport,
		lastStats: make(map[string]interface{}),
		startTime: time.Now(),
	}
}

// EmitTransportMetrics emits transport metrics as events
func (tmc *TransportMetricsCollector) EmitTransportMetrics() {
	if tmc.transport == nil {
		return
	}

	// Get current metrics from transport
	currentStats := tmc.transport.GetMetrics()
	if currentStats == nil {
		return
	}

	// Calculate deltas for rate metrics
	now := time.Now()
	uptime := now.Sub(tmc.startTime).Seconds()

	// Create transport metrics payload
	metricsPayload := map[string]interface{}{
		"timestamp":  now.Format(time.RFC3339),
		"uptime":     uptime,
		"transport":  currentStats,
		"nodeID":     tmc.broker.LocalNode().GetID(),
		"instanceID": tmc.broker.InstanceID(),
	}

	// Emit transport metrics event
	tmc.broker.Bus().EmitAsync("metrics.transport.stats", []interface{}{metricsPayload})

	// Store current stats for next calculation
	tmc.lastStats = currentStats
}

// StartPeriodicCollection starts periodic collection of transport metrics
func (tmc *TransportMetricsCollector) StartPeriodicCollection(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				tmc.EmitTransportMetrics()
			}
		}
	}()
}

// EmitConnectionEvent emits connection-related events
func (tmc *TransportMetricsCollector) EmitConnectionEvent(eventType string, nodeID string, details map[string]interface{}) {
	payload := map[string]interface{}{
		"timestamp":   time.Now().Format(time.RFC3339),
		"eventType":   eventType,
		"nodeID":      nodeID,
		"transport":   tmc.getTransportType(),
		"localNodeID": tmc.broker.LocalNode().GetID(),
		"instanceID":  tmc.broker.InstanceID(),
	}

	// Add details if provided
	for k, v := range details {
		payload[k] = v
	}

	tmc.broker.Bus().EmitAsync("metrics.transport.connection", []interface{}{payload})
}

// EmitBufferPoolEvent emits buffer pool related events
func (tmc *TransportMetricsCollector) EmitBufferPoolEvent(eventType string, size int, details map[string]interface{}) {
	payload := map[string]interface{}{
		"timestamp":  time.Now().Format(time.RFC3339),
		"eventType":  eventType,
		"size":       size,
		"transport":  tmc.getTransportType(),
		"nodeID":     tmc.broker.LocalNode().GetID(),
		"instanceID": tmc.broker.InstanceID(),
	}

	// Add details if provided
	for k, v := range details {
		payload[k] = v
	}

	tmc.broker.Bus().EmitAsync("metrics.transport.buffer", []interface{}{payload})
}

// getTransportType returns the transport type from metrics
func (tmc *TransportMetricsCollector) getTransportType() string {
	if tmc.transport == nil {
		return "unknown"
	}

	stats := tmc.transport.GetMetrics()
	if stats == nil {
		return "unknown"
	}

	if transportType, ok := stats["type"].(string); ok {
		return transportType
	}

	return "unknown"
}
