package performance

import (
	"runtime"
	"time"
)

// ActionConfig represents the configuration for an action call
type ActionConfig struct {
	Config  map[string]ActionCallConfig `json:"config"`
	Payload []byte                      `json:"payload"`
}

// ActionCallConfig represents the configuration for a specific action
type ActionCallConfig struct {
	Actions              []string `json:"actions"`
	ReturnPayloadSize    int      `json:"return_payload_size"`
	ParameterPayloadSize int      `json:"parameter_payload_size"`
}

// ActionResult represents the result of an action call
type ActionResult struct {
	ActionName  string `json:"action_name"`
	RandomValue int64  `json:"random_value"`
	Payload     []byte `json:"payload"`
}

// EnhancedTestConfig represents the complete test configuration
type EnhancedTestConfig struct {
	BrokerCount           int                         `json:"broker_count"`
	ServicesPerBroker     int                         `json:"services_per_broker"`
	ServiceDistribution   map[string][]string         `json:"service_distribution"`
	CallChainConfig       map[string]ActionCallConfig `json:"call_chain_config"`
	EventAggregatorConfig map[string][]string         `json:"event_aggregator_config"`
	TestDurationSeconds   int                         `json:"test_duration_seconds"`
	TransporterType       string                      `json:"transporter_type"`
	TransporterConfig     map[string]interface{}      `json:"transporter_config"`
	MemoryThresholdBytes  int64                       `json:"memory_threshold_bytes"`
	GoroutineThreshold    int                         `json:"goroutine_threshold"`
}

// EnhancedTestResult represents the result of an enhanced test
type EnhancedTestResult struct {
	TestName          string                 `json:"test_name"`
	Transporter       string                 `json:"transporter"`
	Timestamp         time.Time              `json:"timestamp"`
	DurationSeconds   float64                `json:"duration_seconds"`
	Success           bool                   `json:"success"`
	Error             string                 `json:"error,omitempty"`
	Metrics           map[string]interface{} `json:"metrics"`
	MemoryStats       *MemoryStats           `json:"memory_stats,omitempty"`
	TestSettings      *TestSettings          `json:"test_settings,omitempty"`
	ActionResults     []ActionResult         `json:"action_results,omitempty"`
	EventResults      []ActionResult         `json:"event_results,omitempty"`
	ValidationResults *ValidationResults     `json:"validation_results,omitempty"`
}

// ValidationResults represents the results of test validation
type ValidationResults struct {
	ActionEventMatch    bool     `json:"action_event_match"`
	PayloadSizeMatch    bool     `json:"payload_size_match"`
	LoadBalancingWorked bool     `json:"load_balancing_worked"`
	TotalActionsCalled  int      `json:"total_actions_called"`
	TotalEventsReceived int      `json:"total_events_received"`
	ValidationErrors    []string `json:"validation_errors,omitempty"`
}

// TestSettings represents the configuration and parameters used in a test
type TestSettings struct {
	TransporterType    string                 `json:"transporter_type"`
	TransporterConfig  map[string]interface{} `json:"transporter_config"`
	BrokerCount        int                    `json:"broker_count"`
	ServicesPerBroker  int                    `json:"services_per_broker"`
	ActionsPerService  int                    `json:"actions_per_service"`
	EventsPerService   int                    `json:"events_per_service"`
	TestDuration       time.Duration          `json:"test_duration_ms"`
	ConcurrencyLevel   int                    `json:"concurrency_level"`
	MemoryThreshold    int64                  `json:"memory_threshold_bytes"`
	GoroutineThreshold int                    `json:"goroutine_threshold"`
	OtherSettings      map[string]interface{} `json:"other_settings,omitempty"`
}

// MemoryStats tracks memory usage during tests
type MemoryStats struct {
	InitialHeap    uint64        `json:"initial_heap"`
	PeakHeap       uint64        `json:"peak_heap"`
	FinalHeap      uint64        `json:"final_heap"`
	HeapGrowth     int64         `json:"heap_growth"`
	GoroutineCount int           `json:"goroutine_count"`
	Measurements   []Measurement `json:"measurements"`
}

// Measurement represents a single memory measurement
type Measurement struct {
	Timestamp      time.Time `json:"timestamp"`
	HeapSize       uint64    `json:"heap_size"`
	GoroutineCount int       `json:"goroutine_count"`
	BrokerCount    int       `json:"broker_count"`
}

// NewMemoryStats creates a new MemoryStats instance
func NewMemoryStats() *MemoryStats {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	return &MemoryStats{
		InitialHeap:    memStats.HeapAlloc,
		PeakHeap:       memStats.HeapAlloc,
		FinalHeap:      memStats.HeapAlloc,
		HeapGrowth:     0,
		GoroutineCount: runtime.NumGoroutine(),
		Measurements:   make([]Measurement, 0),
	}
}

// update updates the memory stats
func (ms *MemoryStats) update(brokerCount int) {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	if memStats.HeapAlloc > ms.PeakHeap {
		ms.PeakHeap = memStats.HeapAlloc
	}

	ms.FinalHeap = memStats.HeapAlloc
	ms.GoroutineCount = runtime.NumGoroutine()

	// Calculate heap growth
	if ms.FinalHeap >= ms.InitialHeap {
		ms.HeapGrowth = int64(ms.FinalHeap - ms.InitialHeap)
	} else {
		ms.HeapGrowth = -int64(ms.InitialHeap - ms.FinalHeap)
	}

	// Add measurement
	ms.Measurements = append(ms.Measurements, Measurement{
		Timestamp:      time.Now(),
		HeapSize:       memStats.HeapAlloc,
		GoroutineCount: runtime.NumGoroutine(),
		BrokerCount:    brokerCount,
	})
}
