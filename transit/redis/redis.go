package redis

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-redis/redis/v8"
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/serializer"
	"github.com/moleculer-go/moleculer/transit"
	log "github.com/sirupsen/logrus"
)

// RedisTransporter implements the Transport interface using Redis pub/sub
type RedisTransporter struct {
	client     *redis.Client
	subscriber *redis.PubSub
	registry   moleculer.Registry
	logger     *log.Entry
	config     *RedisConfig
	connected  bool
	ctx        context.Context
	cancel     context.CancelFunc
	nodeID     string
	prefix     string
	serializer serializer.Serializer
}

// RedisConfig holds configuration for Redis transporter
type RedisConfig struct {
	URL      string `json:"url"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Password string `json:"password"`
	DB       int    `json:"db"`
	Prefix   string `json:"prefix"`
}

// DefaultRedisConfig returns default Redis configuration
func DefaultRedisConfig() *RedisConfig {
	return &RedisConfig{
		Host:     "localhost",
		Port:     6379,
		Password: "",
		DB:       0,
		Prefix:   "moleculer",
	}
}

// NewRedisTransporter creates a new Redis transporter
func NewRedisTransporter(config *RedisConfig) *RedisTransporter {
	if config == nil {
		config = DefaultRedisConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", config.Host, config.Port),
		Password: config.Password,
		DB:       config.DB,
	})

	return &RedisTransporter{
		client: rdb,
		config: config,
		ctx:    ctx,
		cancel: cancel,
		logger: log.WithField("component", "RedisTransporter"),
	}
}

// Connect establishes connection to Redis
func (r *RedisTransporter) Connect(registry moleculer.Registry) chan error {
	result := make(chan error, 1)

	go func() {
		// Test connection
		_, err := r.client.Ping(r.ctx).Result()
		if err != nil {
			result <- fmt.Errorf("failed to connect to Redis: %w", err)
			return
		}

		r.registry = registry
		r.connected = true
		r.logger.Info("Redis transporter connected")
		result <- nil
	}()

	return result
}

// Disconnect closes Redis connection
func (r *RedisTransporter) Disconnect() chan error {
	result := make(chan error, 1)

	go func() {
		if r.subscriber != nil {
			r.subscriber.Close()
		}
		if r.client != nil {
			r.client.Close()
		}
		r.connected = false
		r.cancel()
		r.logger.Info("Redis transporter disconnected")
		result <- nil
	}()

	return result
}

// Subscribe subscribes to a channel
func (r *RedisTransporter) Subscribe(command, nodeID string, handler transit.TransportHandler) {
	if !r.connected {
		r.logger.Error("Redis transporter not connected")
		return
	}

	channel := r.getChannelName(command, nodeID)
	r.subscriber = r.client.Subscribe(r.ctx, channel)

	go func() {
		ch := r.subscriber.Channel()
		for {
			select {
			case msg := <-ch:
				if msg != nil {
					var payload moleculer.Payload
					if err := json.Unmarshal([]byte(msg.Payload), &payload); err != nil {
						r.logger.Error("Failed to unmarshal message:", err)
						continue
					}

					handler(payload)
				}
			case <-r.ctx.Done():
				return
			}
		}
	}()
}

// Publish publishes a message to a channel
func (r *RedisTransporter) Publish(command, nodeID string, message moleculer.Payload) {
	if !r.connected {
		r.logger.Error("Redis transporter not connected")
		return
	}

	jsonData := r.serializer.PayloadToBytes(message)

	channel := r.getChannelName(command, nodeID)
	err := r.client.Publish(r.ctx, channel, jsonData).Err()
	if err != nil {
		r.logger.Error("Failed to publish message:", err)
	}
}

// SetPrefix sets the channel prefix
func (r *RedisTransporter) SetPrefix(prefix string) {
	r.prefix = prefix
}

// SetNodeID sets the node ID
func (r *RedisTransporter) SetNodeID(nodeID string) {
	r.nodeID = nodeID
}

// SetSerializer sets the message serializer
func (r *RedisTransporter) SetSerializer(ser serializer.Serializer) {
	r.serializer = ser
}

// GetMetrics returns transporter statistics
func (r *RedisTransporter) GetMetrics() map[string]interface{} {
	if !r.connected {
		return map[string]interface{}{}
	}

	stats := r.client.PoolStats()
	return map[string]interface{}{
		"hits":        stats.Hits,
		"misses":      stats.Misses,
		"timeouts":    stats.Timeouts,
		"total_conns": stats.TotalConns,
		"idle_conns":  stats.IdleConns,
		"stale_conns": stats.StaleConns,
	}
}

// getChannelName returns the full channel name with prefix
func (r *RedisTransporter) getChannelName(command, nodeID string) string {
	prefix := r.prefix
	if prefix == "" {
		prefix = r.config.Prefix
	}
	return fmt.Sprintf("%s:%s:%s", prefix, command, nodeID)
}

// GetName returns the transporter name
func (r *RedisTransporter) GetName() string {
	return "Redis"
}

// IsConnected returns connection status
func (r *RedisTransporter) IsConnected() bool {
	return r.connected
}

// GetConfig returns the transporter configuration
func (r *RedisTransporter) GetConfig() interface{} {
	return r.config
}

// SetConfig updates the transporter configuration
func (r *RedisTransporter) SetConfig(config interface{}) {
	if redisConfig, ok := config.(*RedisConfig); ok {
		r.config = redisConfig
	}
}

// Ensure RedisTransporter implements Transport interface
var _ transit.Transport = (*RedisTransporter)(nil)
