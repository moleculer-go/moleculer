package performance

import (
	"os"
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/serializer"
	"github.com/moleculer-go/moleculer/transit/amqp"
	"github.com/moleculer-go/moleculer/transit/kafka"
	"github.com/moleculer-go/moleculer/transit/memory"
	"github.com/moleculer-go/moleculer/transit/nats"
	"github.com/moleculer-go/moleculer/transit/redis"
	"github.com/moleculer-go/moleculer/transit/tcp"
	log "github.com/sirupsen/logrus"
)

// TransporterType represents different types of transporters
type TransporterType string

const (
	TransporterMemory TransporterType = "Memory"
	TransporterTCP    TransporterType = "TCP"
	TransporterNATS   TransporterType = "NATS"
	TransporterRedis  TransporterType = "Redis"
	TransporterAMQP   TransporterType = "AMQP"
	TransporterKafka  TransporterType = "Kafka"
)

// TransporterConfig holds configuration for different transporters
type TransporterConfig struct {
	Type   TransporterType
	Memory *MemoryTransporterConfig
	TCP    *TCPTransporterConfig
	NATS   *NATSTransporterConfig
	Redis  *RedisTransporterConfig
	AMQP   *AMQPTransporterConfig
	Kafka  *KafkaTransporterConfig
}

// MemoryTransporterConfig configuration for memory transporter
type MemoryTransporterConfig struct {
	SharedMemory *memory.SharedMemory
}

// TCPTransporterConfig configuration for TCP transporter
type TCPTransporterConfig struct {
	Port           int
	Host           string
	MaxConnections int
}

// NATSTransporterConfig configuration for NATS transporter
type NATSTransporterConfig struct {
	URL string
}

// RedisTransporterConfig configuration for Redis transporter
type RedisTransporterConfig struct {
	URL string
}

// AMQPTransporterConfig configuration for AMQP transporter
type AMQPTransporterConfig struct {
	URL string
}

// KafkaTransporterConfig configuration for Kafka transporter
type KafkaTransporterConfig struct {
	Brokers []string
}

// TransporterFactory creates transporter instances for testing
type TransporterFactory struct {
	config *TransporterConfig
}

// NewTransporterFactory creates a new transporter factory
func NewTransporterFactory(config *TransporterConfig) *TransporterFactory {
	return &TransporterFactory{
		config: config,
	}
}

// CreateTransporter creates a transporter based on the configuration
func (tf *TransporterFactory) CreateTransporter() interface{} {
	switch tf.config.Type {
	case TransporterMemory:
		return tf.createMemoryTransporter()
	case TransporterTCP:
		return tf.createTCPTransporter()
	case TransporterNATS:
		return tf.createNATSTransporter()
	case TransporterRedis:
		return tf.createRedisTransporter()
	case TransporterAMQP:
		return tf.createAMQPTransporter()
	case TransporterKafka:
		return tf.createKafkaTransporter()
	default:
		// Default to memory transporter
		return tf.createMemoryTransporter()
	}
}

// createMemoryTransporter creates a memory transporter
func (tf *TransporterFactory) createMemoryTransporter() interface{} {
	var sharedMem *memory.SharedMemory
	if tf.config.Memory != nil && tf.config.Memory.SharedMemory != nil {
		sharedMem = tf.config.Memory.SharedMemory
	} else {
		sharedMem = &memory.SharedMemory{}
	}

	transport := memory.Create(log.WithField("transport", "memory"), sharedMem)
	return &transport
}

// createTCPTransporter creates a TCP transporter
func (tf *TransporterFactory) createTCPTransporter() interface{} {
	// Use default TCP configuration with UDP discovery
	options := tcp.TCPOptions{
		UdpDiscovery:          true,
		UdpReuseAddr:          true,
		UdpPort:               4445, // Default UDP listening and discovery port
		UdpBindAddress:        "",
		UdpPeriod:             2 * time.Second,  // Fast discovery for tests
		UdpMaxDiscovery:       0,                // Unlimited
		WorkerPoolSize:        20,               // Default worker pool size
		ConnectionTimeout:     30 * time.Second, // Default connection timeout
		IdleConnectionTimeout: 60 * time.Second, // Default idle connection timeout
		UdpMulticast:          "239.0.0.0",
		UdpMulticastTTL:       1,
		UdpBroadcast:          []string{},
		Port:                  0, // Random TCP port
		Urls:                  []string{},
		UseHostname:           true,
		GossipPeriod:          2, // 2 seconds
		MaxConnections:        32,
		MaxPacketSize:         1024 * 1024, // 1MB
		Logger:                log.WithField("transport", "tcp"),
		Serializer:            serializer.CreateJSONSerializer(log.WithField("transport", "tcp")),
	}

	transport := tcp.CreateTCPTransporter(options)
	return transport
}

// createNATSTransporter creates a NATS transporter
func (tf *TransporterFactory) createNATSTransporter() interface{} {
	url := "nats://localhost:4222"

	if tf.config.NATS != nil && tf.config.NATS.URL != "" {
		url = tf.config.NATS.URL
	}

	options := nats.NATSOptions{
		URL:            url,
		Logger:         log.WithField("transport", "nats"),
		Serializer:     serializer.CreateJSONSerializer(log.WithField("transport", "nats")),
		AllowReconnect: true,
		ReconnectWait:  5 * time.Second,
		MaxReconnect:   10,
	}

	transport := nats.CreateNatsTransporter(options)
	return transport
}

// createRedisTransporter creates a Redis transporter
func (tf *TransporterFactory) createRedisTransporter() interface{} {
	host := "localhost"
	port := 6379

	if tf.config.Redis != nil {
		if tf.config.Redis.URL != "" {
			// Parse URL if provided (simple parsing for redis://host:port format)
			// For now, just use localhost:6379 as default
		}
	}

	transport := redis.NewRedisTransporter(&redis.RedisConfig{
		Host: host,
		Port: port,
	})
	transport.SetSerializer(serializer.CreateJSONSerializer(log.WithField("transport", "redis")))
	return transport
}

// createAMQPTransporter creates an AMQP transporter
func (tf *TransporterFactory) createAMQPTransporter() interface{} {
	url := "amqp://guest:guest@localhost:5672/"

	if tf.config.AMQP != nil && tf.config.AMQP.URL != "" {
		url = tf.config.AMQP.URL
	}

	options := amqp.AmqpOptions{
		Url:        []string{url},
		Logger:     log.WithField("transport", "amqp"),
		Serializer: serializer.CreateJSONSerializer(log.WithField("transport", "amqp")),
	}

	transport := amqp.CreateAmqpTransporter(options)
	return transport
}

// createKafkaTransporter creates a Kafka transporter
func (tf *TransporterFactory) createKafkaTransporter() interface{} {
	brokers := []string{"localhost:9092"}

	if tf.config.Kafka != nil && len(tf.config.Kafka.Brokers) > 0 {
		brokers = tf.config.Kafka.Brokers
	}

	options := kafka.KafkaOptions{
		Url:        brokers[0], // Use first broker as URL
		Logger:     log.WithField("transport", "kafka"),
		Serializer: serializer.CreateJSONSerializer(log.WithField("transport", "kafka")),
	}

	transport := kafka.CreateKafkaTransporter(options)
	return transport
}

// GetAvailableTransporters returns a list of available transporters
func GetAvailableTransporters() []TransporterType {
	return []TransporterType{
		TransporterMemory,
		TransporterTCP,
		TransporterNATS,
		TransporterRedis,
		TransporterAMQP,
		TransporterKafka,
	}
}

// IsTransporterAvailable checks if a transporter is available for testing
func IsTransporterAvailable(transporterType TransporterType) bool {
	switch transporterType {
	case TransporterMemory:
		return true // Always available
	case TransporterTCP:
		return true // Always available (local)
	case TransporterNATS:
		return checkNATSAvailability()
	case TransporterRedis:
		return checkRedisAvailability()
	case TransporterAMQP:
		return checkAMQPAvailability()
	case TransporterKafka:
		return checkKafkaAvailability()
	default:
		return false
	}
}

// checkNATSAvailability checks if NATS is available
func checkNATSAvailability() bool {
	// Simple check - in real implementation, you might want to try connecting
	return os.Getenv("NATS_URL") != "" || os.Getenv("NATS_AVAILABLE") == "true"
}

// checkRedisAvailability checks if Redis is available
func checkRedisAvailability() bool {
	return os.Getenv("REDIS_URL") != "" || os.Getenv("REDIS_AVAILABLE") == "true"
}

// checkAMQPAvailability checks if AMQP is available
func checkAMQPAvailability() bool {
	return os.Getenv("AMQP_URL") != "" || os.Getenv("AMQP_AVAILABLE") == "true"
}

// checkKafkaAvailability checks if Kafka is available
func checkKafkaAvailability() bool {
	return os.Getenv("KAFKA_BROKERS") != "" || os.Getenv("KAFKA_AVAILABLE") == "true"
}

// CreateTestConfig creates a test configuration for a specific transporter
func CreateTestConfig(transporterType TransporterType, logLevel string) *moleculer.Config {
	factory := NewTransporterFactory(&TransporterConfig{
		Type: transporterType,
	})

	return &moleculer.Config{
		TransporterFactory: func() interface{} {
			return factory.CreateTransporter()
		},
		LogLevel: logLevel,
	}
}

// CreateMultiTransporterTestConfig creates a test configuration that can use multiple transporters
func CreateMultiTransporterTestConfig(transporterTypes []TransporterType, logLevel string) []*moleculer.Config {
	configs := make([]*moleculer.Config, 0, len(transporterTypes))

	for _, transporterType := range transporterTypes {
		if IsTransporterAvailable(transporterType) {
			configs = append(configs, CreateTestConfig(transporterType, logLevel))
		}
	}

	return configs
}

// TransporterTestSuite runs tests across multiple transporters
type TransporterTestSuite struct {
	transporterTypes []TransporterType
	configs          []*moleculer.Config
}

// NewTransporterTestSuite creates a new transporter test suite
func NewTransporterTestSuite(transporterTypes []TransporterType) *TransporterTestSuite {
	availableTypes := make([]TransporterType, 0)
	configs := make([]*moleculer.Config, 0)

	for _, transporterType := range transporterTypes {
		if IsTransporterAvailable(transporterType) {
			availableTypes = append(availableTypes, transporterType)
			configs = append(configs, CreateTestConfig(transporterType, "ERROR"))
		}
	}

	return &TransporterTestSuite{
		transporterTypes: availableTypes,
		configs:          configs,
	}
}

// GetTransporterTypes returns the available transporter types
func (ts *TransporterTestSuite) GetTransporterTypes() []TransporterType {
	return ts.transporterTypes
}

// GetConfigs returns the configurations for available transporters
func (ts *TransporterTestSuite) GetConfigs() []*moleculer.Config {
	return ts.configs
}

// RunTestForAllTransporters runs a test function for all available transporters
func (ts *TransporterTestSuite) RunTestForAllTransporters(testFunc func(transporterType TransporterType, config *moleculer.Config) error) map[TransporterType]error {
	results := make(map[TransporterType]error)

	for i, transporterType := range ts.transporterTypes {
		config := ts.configs[i]
		err := testFunc(transporterType, config)
		results[transporterType] = err
	}

	return results
}

// GetTransporterName returns a human-readable name for the transporter type
func GetTransporterName(transporterType TransporterType) string {
	switch transporterType {
	case TransporterMemory:
		return "Memory"
	case TransporterTCP:
		return "TCP"
	case TransporterNATS:
		return "NATS"
	case TransporterRedis:
		return "Redis"
	case TransporterAMQP:
		return "AMQP"
	case TransporterKafka:
		return "Kafka"
	default:
		return "Unknown"
	}
}

// GetTransporterDescription returns a description for the transporter type
func GetTransporterDescription(transporterType TransporterType) string {
	switch transporterType {
	case TransporterMemory:
		return "In-memory transporter for local testing"
	case TransporterTCP:
		return "TCP-based transporter for direct node communication"
	case TransporterNATS:
		return "NATS message broker transporter"
	case TransporterRedis:
		return "Redis-based transporter with pub/sub"
	case TransporterAMQP:
		return "AMQP message broker transporter (RabbitMQ)"
	case TransporterKafka:
		return "Apache Kafka transporter for high-throughput messaging"
	default:
		return "Unknown transporter type"
	}
}
