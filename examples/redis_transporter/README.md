# Redis Transporter Examples

This directory contains examples demonstrating how to use the Redis transporter with Moleculer-Go.

## Prerequisites

1. **Redis Server**: Make sure Redis is running on your system
   ```bash
   # Using Docker
   docker run -d --name redis -p 6379:6379 redis:latest
   
   # Or install Redis locally
   # Ubuntu/Debian: sudo apt-get install redis-server
   # macOS: brew install redis
   ```

2. **Go Dependencies**: Ensure all dependencies are installed
   ```bash
   go mod tidy
   ```

## Examples

### 1. Basic Usage (`main.go`)

A simple example showing how to:
- Configure Redis transporter
- Create services with actions and events
- Make service calls
- View Redis metrics

**Run:**
```bash
go run main.go
```

**Expected Output:**
```
🚀 Moleculer-Go broker started with Redis transporter
📡 Redis connection: true
🧮 Testing math operations...
10 + 5 = 15
10 * 5 = 50
20 + 30 = 50
📊 Redis Metrics:
  Total Connections: 1
  Idle Connections: 1
  Hits: 0
  Misses: 0
```

### 2. Advanced Configuration (`advanced_config.go`)

An advanced example showing:
- Custom Redis configuration (password, database, prefix)
- Event-driven communication
- Multiple services listening to events
- Detailed logging

**Run:**
```bash
go run advanced_config.go
```

## Redis Configuration Options

```go
redisConfig := &redis.RedisConfig{
    Host:     "localhost",        // Redis server host
    Port:     6379,              // Redis server port
    Password: "your-password",   // Optional password
    DB:       0,                 // Redis database number (0-15)
    Prefix:   "moleculer",       // Channel prefix for namespacing
}
```

## Key Features

- **Pub/Sub Communication**: Services communicate via Redis pub/sub
- **Event Broadcasting**: Events are automatically published to Redis
- **Connection Pooling**: Efficient Redis connection management
- **Metrics**: Built-in Redis connection pool statistics
- **Error Handling**: Robust error handling and logging
- **Configurable**: Flexible configuration options

## Troubleshooting

### Connection Issues

1. **Redis not running**: Make sure Redis is running on the specified host/port
2. **Wrong credentials**: Check password and database settings
3. **Network issues**: Verify host and port are accessible

### Performance Tips

1. **Use appropriate DB**: Use different Redis databases for different environments
2. **Set prefix**: Use meaningful prefixes to avoid channel conflicts
3. **Monitor metrics**: Keep an eye on connection pool statistics
4. **Connection limits**: Be aware of Redis connection limits

## Integration with Other Transporters

The Redis transporter can be used alongside other transporters:

```go
// For development/testing
transporter := redis.NewRedisTransporter(redisConfig)

// For production with fallback
transporter := tcp.NewTCPTransporter(tcpConfig)
// or
transporter := nats.NewNATSTransporter(natsConfig)
```

## Monitoring

Monitor Redis performance using the built-in metrics:

```go
metrics := transporter.GetMetrics()
fmt.Printf("Connection pool stats: %+v\n", metrics)
```

Available metrics:
- `hits`: Number of times a connection was reused
- `misses`: Number of times a new connection was created
- `timeouts`: Number of connection timeouts
- `total_conns`: Total number of connections in the pool
- `idle_conns`: Number of idle connections
- `stale_conns`: Number of stale connections
