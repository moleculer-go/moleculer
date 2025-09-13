package amqp

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/serializer"
	"github.com/moleculer-go/moleculer/transit"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

const (
	DurationNotDefined = time.Duration(-1)
)

type safeHandler func(moleculer.Payload) error

type binding struct {
	queueName string
	topic     string
	pattern   string
}

type subscriber struct {
	command string
	nodeID  string
	handler transit.TransportHandler
}

var DefaultConfig = AmqpOptions{
	Prefetch: 1,

	AutoDeleteQueues:    DurationNotDefined,
	EventTimeToLive:     DurationNotDefined,
	HeartbeatTimeToLive: DurationNotDefined,
}

type AmqpTransporter struct {
	opts       *AmqpOptions
	prefix     string
	logger     *log.Entry
	serializer serializer.Serializer

	connectionDisconnecting bool
	connectionRecovering    bool
	connection              *amqp.Connection
	channel                 *amqp.Channel

	nodeID      string
	subscribers []subscriber
	bindings    []binding

	// Worker pool for consumer goroutines
	workerPool      *WorkerPool
	activeConsumers map[string]chan struct{} // Track active consumers for cleanup
	consumerMutex   sync.RWMutex
}

type AmqpOptions struct {
	Url             []string
	QueueOptions    map[string]interface{}
	ExchangeOptions map[string]interface{}
	MessageOptions  map[string]interface{}
	ConsumeOptions  amqp.Table

	Logger     *log.Entry
	Serializer serializer.Serializer

	DisableReconnect    bool
	AutoDeleteQueues    time.Duration
	EventTimeToLive     time.Duration
	HeartbeatTimeToLive time.Duration
	Prefetch            int
	WorkerPoolSize      int
}

// WorkerPool manages a limited number of goroutines for consumer handling
type WorkerPool struct {
	workers  int
	jobQueue chan func()
	stopChan chan struct{}
	wg       sync.WaitGroup
}

// NewWorkerPool creates a new worker pool
func NewWorkerPool(workers int) *WorkerPool {
	wp := &WorkerPool{
		workers:  workers,
		jobQueue: make(chan func(), workers*2), // Buffer for 2x workers
		stopChan: make(chan struct{}),
	}

	// Start workers
	for i := 0; i < workers; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}

	return wp
}

// Submit submits a job to the worker pool
func (wp *WorkerPool) Submit(job func()) {
	select {
	case wp.jobQueue <- job:
		// Job submitted successfully
	case <-wp.stopChan:
		// Pool is stopping, ignore job
	default:
		// Queue is full, execute synchronously to avoid blocking
		go job()
	}
}

// worker runs a single worker goroutine
func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()

	for {
		select {
		case job := <-wp.jobQueue:
			job()
		case <-wp.stopChan:
			return
		}
	}
}

// Stop stops the worker pool
func (wp *WorkerPool) Stop() {
	close(wp.stopChan)
	wp.wg.Wait()
}

func mergeConfigs(baseConfig AmqpOptions, userConfig AmqpOptions) AmqpOptions {
	// Number of requests a broker will handle concurrently
	if userConfig.Prefetch != 0 {
		baseConfig.Prefetch = userConfig.Prefetch
	}

	// Number of milliseconds before an event expires
	if userConfig.EventTimeToLive != 0 {
		baseConfig.EventTimeToLive = userConfig.EventTimeToLive
	}

	if userConfig.HeartbeatTimeToLive != 0 {
		baseConfig.HeartbeatTimeToLive = userConfig.HeartbeatTimeToLive
	}

	if userConfig.QueueOptions != nil {
		baseConfig.QueueOptions = userConfig.QueueOptions
	}

	if userConfig.ExchangeOptions != nil {
		baseConfig.ExchangeOptions = userConfig.ExchangeOptions
	}

	if userConfig.MessageOptions != nil {
		baseConfig.MessageOptions = userConfig.MessageOptions
	}

	if userConfig.ConsumeOptions != nil {
		baseConfig.ConsumeOptions = userConfig.ConsumeOptions
	}

	if userConfig.AutoDeleteQueues != 0 {
		baseConfig.AutoDeleteQueues = userConfig.AutoDeleteQueues
	}

	baseConfig.DisableReconnect = userConfig.DisableReconnect

	// Support for multiple URLs (clusters)
	if len(userConfig.Url) != 0 {
		baseConfig.Url = userConfig.Url
	}

	if userConfig.Logger != nil {
		baseConfig.Logger = userConfig.Logger
	}

	if userConfig.WorkerPoolSize != 0 {
		baseConfig.WorkerPoolSize = userConfig.WorkerPoolSize
	}

	return baseConfig
}

func CreateAmqpTransporter(options AmqpOptions) transit.Transport {
	options = mergeConfigs(DefaultConfig, options)

	// Set default worker pool size if not specified
	if options.WorkerPoolSize <= 0 {
		options.WorkerPoolSize = 10 // Default value
		if options.Logger != nil {
			options.Logger.Warn("Invalid WorkerPoolSize, using default value of 10")
		}
	}

	return &AmqpTransporter{
		opts:            &options,
		logger:          options.Logger,
		workerPool:      NewWorkerPool(options.WorkerPoolSize),
		activeConsumers: make(map[string]chan struct{}),
	}
}

func (t *AmqpTransporter) Connect(registry moleculer.Registry) chan error {
	endChan := make(chan error)

	go func() {
		t.logger.Debug("AMQP Connect() - url: ", t.opts.Url)

		isConnected := false
		connectAttempt := 0

		for {
			connectAttempt++
			urlIndex := (connectAttempt - 1) % len(t.opts.Url)
			uri := t.opts.Url[urlIndex]

			closeNotifyChan, err := t.doConnect(uri)
			if err != nil {
				t.logger.Error("AMQP Connect() - Error: ", err, " url: ", uri)
			} else if !isConnected {
				isConnected = true
				endChan <- nil
			} else {
				// recovery subscribers
				for _, subscriber := range t.subscribers {
					t.subscribeInternal(subscriber)
				}

				t.connectionRecovering = false
			}

			if closeNotifyChan != nil {
				err = <-closeNotifyChan
				if t.connectionDisconnecting {
					t.logger.Info("AMQP connection is closed gracefully")
					return
				}

				t.logger.Error("AMQP connection is closed -> ", err)
			}

			if t.opts.DisableReconnect {
				return
			}

			t.connectionRecovering = true

			time.Sleep(5 * time.Second)
		}
	}()
	return endChan
}

func (t *AmqpTransporter) doConnect(uri string) (chan *amqp.Error, error) {
	var err error

	t.connection, err = amqp.Dial(uri)
	if err != nil {
		return nil, errors.Wrap(err, "AMQP failed to connect")
	}

	t.logger.Info("AMQP is connected")

	if t.channel, err = t.connection.Channel(); err != nil {
		return nil, errors.Wrap(err, "AMQP failed to create channel")
	}

	t.logger.Info("AMQP channel is created")

	if err := t.channel.Qos(t.opts.Prefetch, 0, false); err != nil {
		return nil, errors.Wrap(err, "AMQP failed set prefetch count")
	}

	closeNotifyChan := make(chan *amqp.Error)
	t.connection.NotifyClose(closeNotifyChan)

	return closeNotifyChan, nil
}

func (t *AmqpTransporter) Disconnect() chan error {
	errChan := make(chan error)

	t.connectionDisconnecting = true

	go func() {
		// Stop all active consumers first
		t.stopAllConsumers()

		if t.connection != nil && t.channel != nil {
			for _, bind := range t.bindings {
				if err := t.channel.QueueUnbind(bind.queueName, bind.pattern, bind.topic, nil); err != nil {
					t.logger.Errorf("AMQP Disconnect() - Can't unbind queue '%#v': %s", bind, err)
				}
			}

			t.subscribers = []subscriber{}
			t.bindings = []binding{}
			t.connectionDisconnecting = true

			if err := t.channel.Close(); err != nil {
				t.logger.Error("AMQP Disconnect() - Channel close error: ", err)
				errChan <- err
				return
			}

			t.channel = nil

			if err := t.connection.Close(); err != nil {
				t.logger.Error("AMQP Disconnect() - Connection close error: ", err)
				errChan <- err
				return
			}

			t.connection = nil
		}

		// Stop the worker pool
		if t.workerPool != nil {
			t.workerPool.Stop()
		}

		errChan <- nil
	}()

	return errChan
}

func (t *AmqpTransporter) Subscribe(command, nodeID string, handler transit.TransportHandler) {
	subscriber := subscriber{command, nodeID, handler}

	// Save subscribers for recovery logic
	t.subscribers = append(t.subscribers, subscriber)

	t.subscribeInternal(subscriber)
}

func (t *AmqpTransporter) subscribeInternal(subscriber subscriber) {
	if t.channel == nil {
		return
	}

	topic := t.topicName(subscriber.command, subscriber.nodeID)

	if subscriber.nodeID != "" {
		// Some topics are specific to this node already, in these cases we don't need an exchange.
		needAck := subscriber.command == "REQ"
		autoDelete, durable, exclusive, args := t.getQueueOptions(subscriber.command, false)
		if _, err := t.channel.QueueDeclare(topic, durable, autoDelete, exclusive, false, args); err != nil {
			t.logger.Error("AMQP Subscribe() - Queue declare error: ", err)
			return
		}

		t.startConsumer(topic, needAck, subscriber.handler)
	} else {
		// Create a queue specific to this nodeID so that this node can receive broadcasted messages.
		queueName := t.prefix + "." + subscriber.command + "." + t.nodeID

		// Save binding arguments for easy unbinding later.
		b := binding{
			queueName: queueName,
			topic:     topic,
			pattern:   "",
		}
		t.bindings = append(t.bindings, b)

		autoDelete, durable, exclusive, args := t.getQueueOptions(subscriber.command, false)
		if _, err := t.channel.QueueDeclare(queueName, durable, autoDelete, exclusive, false, args); err != nil {
			t.logger.Error("AMQP Subscribe() - Queue declare error: ", err)
			return
		}

		durable, autoDelete, args = t.getExchangeOptions()
		if err := t.channel.ExchangeDeclare(topic, "fanout", durable, autoDelete, false, false, args); err != nil {
			t.logger.Error("AMQP Subscribe() - Exchange declare error: ", err)
			return
		}

		if err := t.channel.QueueBind(b.queueName, b.pattern, b.topic, false, nil); err != nil {
			t.logger.Error("AMQP Subscribe() - Can't bind queue to exchange: ", err)
			return
		}

		t.startConsumer(queueName, false, subscriber.handler)
	}
}

func (t *AmqpTransporter) Publish(command, nodeID string, message moleculer.Payload) {
	if t.channel == nil {
		msg := fmt.Sprint("AMQP Publish() No connection -> command: ", command, " nodeID: ", nodeID)
		t.logger.Error(msg)
		panic(errors.New(msg))
	}

	if t.connectionRecovering {
		t.waitForRecovering()
	}

	topic := t.topicName(command, nodeID)
	routingKey := ""

	if nodeID != "" {
		routingKey = topic
		topic = ""
	}

	data := t.serializer.PayloadToBytes(message)

	msg := amqp.Publishing{
		Body: data,
	}

	if err := t.channel.Publish(topic, routingKey, false, false, msg); err != nil {
		t.logger.Warnf("AMQP Publish - Can't publish command: %s, nodeID: %s, error: %s", command, nodeID, err)
	}
}

func (t *AmqpTransporter) waitForRecovering() {
	for {
		if !t.connectionRecovering {
			return
		}

		time.Sleep(time.Second)
	}
}

func (t *AmqpTransporter) SetPrefix(prefix string) {
	t.prefix = prefix
}

func (t *AmqpTransporter) SetNodeID(nodeID string) {
	t.nodeID = nodeID
}

func (t *AmqpTransporter) SetSerializer(serializer serializer.Serializer) {
	t.serializer = serializer
}

// startConsumer starts a consumer using the worker pool
func (t *AmqpTransporter) startConsumer(queueName string, needAck bool, handler transit.TransportHandler) {
	// Create a stop channel for this consumer
	stopChan := make(chan struct{})

	// Track this consumer
	t.consumerMutex.Lock()
	t.activeConsumers[queueName] = stopChan
	t.consumerMutex.Unlock()

	// Submit consumer job to worker pool
	t.workerPool.Submit(func() {
		t.doConsume(queueName, needAck, handler, stopChan)
	})
}

// stopAllConsumers stops all active consumers
func (t *AmqpTransporter) stopAllConsumers() {
	t.consumerMutex.Lock()
	defer t.consumerMutex.Unlock()

	for queueName, stopChan := range t.activeConsumers {
		select {
		case <-stopChan:
			// Channel already closed
		default:
			close(stopChan)
		}
		delete(t.activeConsumers, queueName)
	}
}

func (t *AmqpTransporter) doConsume(queueName string, needAck bool, handler transit.TransportHandler, stopChan chan struct{}) {
	defer func() {
		// Clean up consumer from active consumers map when done
		t.consumerMutex.Lock()
		delete(t.activeConsumers, queueName)
		t.consumerMutex.Unlock()
		t.logger.Debug("AMQP doConsume() - Consumer cleanup completed for queue: ", queueName)
	}()

	t.logger.Debug("AMQP doConsume() - queue: ", queueName)

	msgs, err := t.channel.Consume(queueName, "", !needAck, false, false, true, t.opts.ConsumeOptions)
	if err != nil {
		t.logger.Errorf("AMQP doConsume - Can't start consume for queue '%s': %s", queueName, err)
		return
	}

	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				return
			}

			payload := t.serializer.BytesToPayload(&msg.Body)
			t.logger.Debugf("Incoming %s packet from '%s'", queueName, payload.Get("sender").String())

			handler(payload)

			if needAck {
				if err = msg.Ack(false); err != nil {
					t.logger.Error("AMQP doConsume() - Can't acknowledge message: ", err)

					if err = msg.Nack(false, true); err != nil {
						t.logger.Error("AMQP doConsume() - Can't negatively acknowledge message: ", err)
					}
				}
			}
		case <-stopChan:
			t.logger.Debug("AMQP doConsume() - Stopping consumer for queue: ", queueName)
			return
		}
	}
}

func (t *AmqpTransporter) getExchangeOptions() (durable, autoDelete bool, args amqp.Table) {
	args = amqp.Table{}

	for key, value := range t.opts.ExchangeOptions {
		switch key {
		case "durable":
			durable = value.(bool)
		case "autoDelete":
			autoDelete = value.(bool)
		default:
			args[key] = value
		}
	}

	return durable, autoDelete, args
}

func (t *AmqpTransporter) getQueueOptions(command string, balancedQueue bool) (autoDelete, durable, exclusive bool, args amqp.Table) {
	args = amqp.Table{}

	switch command {
	// Requests and responses don't expire.
	case "REQ":
		if t.opts.AutoDeleteQueues != DurationNotDefined && !balancedQueue {
			args["x-expires"] = int(t.opts.AutoDeleteQueues / time.Millisecond)
		}

	case "RES":
		if t.opts.AutoDeleteQueues != DurationNotDefined {
			args["x-expires"] = int(t.opts.AutoDeleteQueues / time.Millisecond)
		}

	// Consumers can decide how long events live
	// Load-balanced/grouped events
	case "EVENT", "EVENTLB":
		if t.opts.AutoDeleteQueues != DurationNotDefined {
			args["x-expires"] = int(t.opts.AutoDeleteQueues / time.Millisecond)
		}

		// If eventTimeToLive is specified, add to options.
		if t.opts.EventTimeToLive != DurationNotDefined {
			args["x-message-ttl"] = int(t.opts.AutoDeleteQueues / time.Millisecond)
		}

	// Packet types meant for internal use
	case "HEARTBEAT":
		autoDelete = true

		// If heartbeatTimeToLive is specified, add to options.
		if t.opts.HeartbeatTimeToLive != DurationNotDefined {
			args["x-message-ttl"] = int(t.opts.AutoDeleteQueues / time.Millisecond)
		}
	case "DISCOVER", "DISCONNECT", "INFO", "PING", "PONG":
		autoDelete = true
	}

	for key, value := range t.opts.QueueOptions {
		switch key {
		case "exclusive":
			exclusive = value.(bool)
		case "durable":
			durable = value.(bool)
		default:
			args[key] = value
		}
	}

	return autoDelete, durable, exclusive, args
}

func (t *AmqpTransporter) topicName(command string, nodeID string) string {
	parts := []string{t.prefix, command}
	if nodeID != "" {
		parts = append(parts, nodeID)
	}
	return strings.Join(parts, ".")
}

// GetMetrics returns transport-specific metrics
func (t *AmqpTransporter) GetMetrics() map[string]interface{} {
	return map[string]interface{}{
		"type":      "amqp",
		"url":       t.opts.Url,
		"prefix":    t.prefix,
		"connected": t.connection != nil,
	}
}
