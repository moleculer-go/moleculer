package performance

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// TestResult represents the result of a single test run
type TestResult struct {
	TestName          string                 `json:"test_name"`
	Transporter       string                 `json:"transporter,omitempty"`
	Timestamp         time.Time              `json:"timestamp"`
	Duration          time.Duration          `json:"duration_ms"`
	Success           bool                   `json:"success"`
	Error             string                 `json:"error,omitempty"`
	Metrics           map[string]interface{} `json:"metrics"`
	MemoryStats       *MemoryStats           `json:"memory_stats,omitempty"`
	Configuration     map[string]interface{} `json:"configuration,omitempty"`
	TestSettings      *TestSettings          `json:"test_settings,omitempty"`
	ActionResults     []ActionResult         `json:"action_results,omitempty"`
	EventResults      []ActionResult         `json:"event_results,omitempty"`
	ValidationResults *ValidationResults     `json:"validation_results,omitempty"`
}

// TestSuiteResult represents results for a complete test suite
type TestSuiteResult struct {
	SuiteName     string                 `json:"suite_name"`
	Timestamp     time.Time              `json:"timestamp"`
	TotalTests    int                    `json:"total_tests"`
	PassedTests   int                    `json:"passed_tests"`
	FailedTests   int                    `json:"failed_tests"`
	TotalDuration time.Duration          `json:"total_duration_ms"`
	Results       []TestResult           `json:"results"`
	Environment   map[string]interface{} `json:"environment"`
}

// TestResultWriter handles writing test results to files
type TestResultWriter struct {
	outputDir string
	results   []TestResult
}

// NewTestResultWriter creates a new test result writer
func NewTestResultWriter(outputDir string) *TestResultWriter {
	// Create output directory if it doesn't exist
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		panic(fmt.Sprintf("Failed to create output directory: %v", err))
	}

	return &TestResultWriter{
		outputDir: outputDir,
		results:   make([]TestResult, 0),
	}
}

// AddResult adds a test result to the writer
func (w *TestResultWriter) AddResult(result TestResult) {
	w.results = append(w.results, result)
}

// SaveResults saves all results to files
func (w *TestResultWriter) SaveResults() error {
	if len(w.results) == 0 {
		return nil
	}

	// Create suite result
	suiteResult := TestSuiteResult{
		SuiteName:     "Moleculer Performance Tests",
		Timestamp:     time.Now(),
		TotalTests:    len(w.results),
		PassedTests:   0,
		FailedTests:   0,
		TotalDuration: 0,
		Results:       w.results,
		Environment: map[string]interface{}{
			"go_version": "1.21", // You might want to get this dynamically
			"os":         "linux",
			"arch":       "amd64",
		},
	}

	// Count passed/failed tests and calculate total duration
	for _, result := range w.results {
		if result.Success {
			suiteResult.PassedTests++
		} else {
			suiteResult.FailedTests++
		}
		suiteResult.TotalDuration += result.Duration
	}

	// Save individual test results
	for i, result := range w.results {
		transporterSuffix := ""
		if result.Transporter != "" {
			transporterSuffix = "_" + result.Transporter
		}

		filename := fmt.Sprintf("test_%d_%s%s_%s.json",
			i+1,
			result.TestName,
			transporterSuffix,
			result.Timestamp.Format("20060102_150405"))
		filepath := filepath.Join(w.outputDir, filename)

		if err := w.saveToFile(result, filepath); err != nil {
			return fmt.Errorf("failed to save individual result %d: %v", i+1, err)
		}
	}

	// Save suite summary
	suiteFilename := fmt.Sprintf("test_suite_%s.json",
		suiteResult.Timestamp.Format("20060102_150405"))
	suiteFilepath := filepath.Join(w.outputDir, suiteFilename)

	if err := w.saveToFile(suiteResult, suiteFilepath); err != nil {
		return fmt.Errorf("failed to save suite result: %v", err)
	}

	// Save latest results for easy access
	latestFilepath := filepath.Join(w.outputDir, "latest_results.json")
	if err := w.saveToFile(suiteResult, latestFilepath); err != nil {
		return fmt.Errorf("failed to save latest results: %v", err)
	}

	return nil
}

// saveToFile saves data to a JSON file
func (w *TestResultWriter) saveToFile(data interface{}, filepath string) error {
	file, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(data)
}

// CreateTestResult creates a new test result
func CreateTestResult(testName, transporter string, success bool, duration time.Duration, err error) TestResult {
	result := TestResult{
		TestName:    testName,
		Transporter: transporter,
		Timestamp:   time.Now(),
		Duration:    duration,
		Success:     success,
		Metrics:     make(map[string]interface{}),
	}

	if err != nil {
		result.Error = err.Error()
	}

	return result
}

// AddMetric adds a metric to the test result
func (tr *TestResult) AddMetric(key string, value interface{}) {
	tr.Metrics[key] = value
}

// AddMemoryStats adds memory statistics to the test result
func (tr *TestResult) AddMemoryStats(stats *MemoryStats) {
	tr.MemoryStats = stats
}

// AddConfiguration adds configuration details to the test result
func (tr *TestResult) AddConfiguration(config map[string]interface{}) {
	tr.Configuration = config
}

// AddTestSettings adds test settings to the test result
func (tr *TestResult) AddTestSettings(settings *TestSettings) {
	tr.TestSettings = settings
}

// CreateTestSettings creates a new TestSettings struct
func CreateTestSettings(transporterType string, transporterConfig map[string]interface{}, brokerCount, servicesPerBroker, actionsPerService, eventsPerService int, testDuration time.Duration, concurrencyLevel int, memoryThreshold int64, goroutineThreshold int) *TestSettings {
	return &TestSettings{
		TransporterType:    transporterType,
		TransporterConfig:  transporterConfig,
		BrokerCount:        brokerCount,
		ServicesPerBroker:  servicesPerBroker,
		ActionsPerService:  actionsPerService,
		EventsPerService:   eventsPerService,
		TestDuration:       testDuration,
		ConcurrencyLevel:   concurrencyLevel,
		MemoryThreshold:    memoryThreshold,
		GoroutineThreshold: goroutineThreshold,
		OtherSettings:      make(map[string]interface{}),
	}
}

// AddOtherSetting adds additional settings to the test settings
func (ts *TestSettings) AddOtherSetting(key string, value interface{}) {
	if ts.OtherSettings == nil {
		ts.OtherSettings = make(map[string]interface{})
	}
	ts.OtherSettings[key] = value
}
