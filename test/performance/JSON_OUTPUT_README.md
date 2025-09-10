# 📄 JSON Output for Unified Performance Tests

The unified performance test framework now automatically generates JSON output files for every test run, with one file per transporter type.

## 🚀 Features

- **Automatic JSON Generation**: Every test run automatically saves results to a JSON file
- **One File Per Transporter**: Each transporter type gets its own JSON file
- **Comprehensive Data**: Complete test results including metrics, validation, and raw data
- **Timestamped Files**: Files include timestamps for easy identification
- **Custom Output Directory**: Configurable output directory for JSON files

## 📁 File Naming Convention

JSON files are named using the following pattern:
```
unified_test_<TestName>_<TransporterType>_<Timestamp>.json
```

Example:
```
unified_test_JSON_Output_Example_TCP_20241201_143022.json
unified_test_JSON_Output_Example_Memory_20241201_143025.json
```

## 🔧 Usage

### Basic Usage (Default Output Directory)
```go
config := &UnifiedTestConfig{
    TestName: "My Test",
    // ... other config
}

test := NewUnifiedTest(config)
result, err := test.Run("TCP")
// JSON file automatically saved to "test_results/" directory
```

### Custom Output Directory
```go
config := &UnifiedTestConfig{
    TestName: "My Test",
    // ... other config
}

test := NewUnifiedTestWithOutputDir(config, "my_custom_results")
result, err := test.Run("TCP")
// JSON file saved to "my_custom_results/" directory
```

### Set Output Directory After Creation
```go
test := NewUnifiedTest(config)
test.SetOutputDir("custom_output")
result, err := test.Run("TCP")
// JSON file saved to "custom_output/" directory
```

## 📊 JSON Structure

The generated JSON files contain the complete test result with the following structure:

```json
{
  "test_name": "My Test",
  "test_description": "Test description",
  "timestamp": "2024-12-01T14:30:22Z",
  "configuration": { ... },
  "success": true,
  "discovery_time_ms": 7668.50,
  "execution_time_ms": 196337.26,
  "total_time_ms": 204005.77,
  "metrics": {
    "discovery_time_ms": 7668.50,
    "execution_time_ms": 196337.26,
    "total_time_ms": 204005.77,
    "total_actions_called": 2,
    "successful_actions": 2,
    "failed_actions": 0,
    "total_events_received": 2,
    "expected_events": 2,
    "initial_heap_bytes": 1234567,
    "peak_heap_bytes": 2345678,
    "final_heap_bytes": 1234567,
    "heap_growth_bytes": 0,
    "initial_goroutines": 10,
    "peak_goroutines": 15,
    "final_goroutines": 12,
    "goroutine_leak": 2,
    "actions_per_second": 0.01,
    "events_per_second": 0.01,
    "call_chain_complete": true,
    "event_chain_complete": true,
    "payload_sizes_correct": true,
    "action_order_correct": true
  },
  "validation_report": {
    "is_valid": true,
    "call_chain_complete": true,
    "event_chain_complete": true,
    "payload_sizes_correct": true,
    "action_order_correct": true,
    "event_aggregation_valid": true,
    "expected_actions_executed": true,
    "expected_events_collected": true,
    "validation_errors": []
  },
  "action_results": [ ... ],
  "event_results": [ ... ],
  "memory_stats": { ... }
}
```

## 🎯 Benefits

1. **Performance Analysis**: Easy comparison between different transporter types
2. **Historical Tracking**: Timestamped files allow tracking performance over time
3. **Automated Processing**: JSON format enables automated analysis and reporting
4. **Debugging**: Complete test data available for troubleshooting
5. **Integration**: Easy integration with CI/CD pipelines and monitoring systems

## 📈 Example Comparison

After running tests with multiple transporters, you can easily compare results:

| Metric | TCP | NATS | Difference |
|--------|-----|------|------------|
| Discovery Time | 7,668.50 ms | 106,305.85 ms | +98,637.35 ms |
| Execution Time | 196,337.26 ms | 254,191.56 ms | +57,854.30 ms |
| Memory Growth | 8,701,568 bytes | 7,584,680 bytes | -1,116,888 bytes |
| Goroutine Leak | 288 | 154 | -134 |

## 🔍 Error Handling

- JSON file generation failures are logged as warnings but don't fail the test
- Output directory is automatically created if it doesn't exist
- File permissions are set to 0755 for the output directory

## 🚀 Getting Started

1. Run any unified test - JSON output is automatic
2. Check the `test_results/` directory (or your custom directory)
3. Open the generated JSON files for detailed analysis
4. Use the data for performance comparisons and optimization

The JSON output feature makes it easy to track, analyze, and compare transporter performance across different test runs!



