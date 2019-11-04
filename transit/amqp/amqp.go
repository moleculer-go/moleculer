package amqp

import (
	"fmt"
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/serializer"
	"github.com/moleculer-go/moleculer/transit"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"strings"
	"sync"
	"time"
)

const (
	DurationNotDefined = time.Duration(-1)
)

type binding struct {
	channelName string
	queueName   string
	topic       string
	pattern     string
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

	nodeID             string
	subscribers        []subscriber
	bindings           []binding
	subscriberChannels map[string]*amqp.Channel
	publishChannel     *amqp.Channel
	publishMutex       sync.Mutex
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

	return baseConfig
}

func CreateAmqpTransporter(options AmqpOptions) transit.Transport {
	options = mergeConfigs(DefaultConfig, options)

	return &AmqpTransporter{
		opts:   &options,
		logger: options.Logger,

		subscriberChannels: map[string]*amqp.Channel{},
	}
}

func (t *AmqpTransporter) Connect() chan error {
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
				if err := t.recoverSubscribers(); err != nil {
					t.logger.Error(err)

					t.closeConnection()
					continue
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

	if t.publishChannel, err = t.connection.Channel(); err != nil {
		return nil, errors.Wrap(err, "AMQP failed to create channel 'publish'")
	}

	t.logger.Debugf("AMQP 'publish' channel is created")

	if err := t.publishChannel.Qos(t.opts.Prefetch, 0, false); err != nil {
		return nil, errors.Wrapf(err, "AMQP failed set prefetch count for 'publish' channel")
	}

	closeNotifyChan := make(chan *amqp.Error)
	t.connection.NotifyClose(closeNotifyChan)

	return closeNotifyChan, nil
}

func (t *AmqpTransporter) Disconnect() chan error {
	errChan := make(chan error)

	t.connectionDisconnecting = true

	go func() {
		t.closeConnection()

		errChan <- nil
	}()

	return errChan
}

func (t *AmqpTransporter) closeConnection() {
	if t.connection != nil {
		for _, bind := range t.bindings {
			if channel, ok := t.subscriberChannels[bind.channelName]; ok {
				if err := channel.QueueUnbind(bind.queueName, bind.pattern, bind.topic, nil); err != nil {
					t.logger.Errorf("AMQP Disconnect() - Can't unbind queue '%#v': %s", bind, err)
				}
			}
		}

		t.subscribers = []subscriber{}
		t.bindings = []binding{}
		t.connectionDisconnecting = true

		for name, channel := range t.subscriberChannels {
			if err := channel.Close(); err != nil {
				t.logger.Errorf("AMQP Disconnect() - Channel '%s' close error: %s", name, err)
			}
		}

		t.subscriberChannels = map[string]*amqp.Channel{}

		if err := t.publishChannel.Close(); err != nil {
			t.logger.Errorf("AMQP Disconnect() - Channel 'publish' close error: %s", err)
		}

		t.publishChannel = nil

		if err := t.connection.Close(); err != nil {
			t.logger.Errorf("AMQP Disconnect() - Connection close error: %s", err)
		}

		t.connection = nil
	}
}

func (t *AmqpTransporter) Subscribe(command, nodeID string, handler transit.TransportHandler) {
	subscriber := subscriber{command, nodeID, handler}

	// Save subscribers for recovery logic
	t.subscribers = append(t.subscribers, subscriber)

	if err := t.subscribeInternal(subscriber); err != nil {
		t.logger.Error(err)
	}
}

func (t *AmqpTransporter) recoverSubscribers() error {
	for _, subscriber := range t.subscribers {
		if err := t.subscribeInternal(subscriber); err != nil {
			return err
		}
	}

	return nil
}

func (t *AmqpTransporter) subscribeInternal(subscriber subscriber) error {
	if t.connection == nil {
		return nil
	}

	var channel *amqp.Channel
	var err error

	topic := t.topicName(subscriber.command, subscriber.nodeID)

	if channel, err = t.connection.Channel(); err != nil {
		return errors.Wrapf(err, "AMQP failed to create channel for topic '%s'", topic)
	}

	t.logger.Debugf("AMQP channel for topic '%s' is created", topic)

	t.subscriberChannels[topic] = channel

	if err := channel.Qos(t.opts.Prefetch, 0, false); err != nil {
		return errors.Wrapf(err, "AMQP failed set prefetch count for channel '%s'", topic)
	}

	if subscriber.nodeID != "" {
		// Some topics are specific to this node already, in these cases we don't need an exchange.
		needAck := subscriber.command == "REQ"
		autoDelete, durable, exclusive, args := t.getQueueOptions(subscriber.command, false)
		if _, err := channel.QueueDeclare(topic, durable, autoDelete, exclusive, false, args); err != nil {
			return nil
		}

		go t.doConsume(channel, topic, needAck, subscriber.handler)
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
		if _, err := channel.QueueDeclare(queueName, durable, autoDelete, exclusive, false, args); err != nil {
			return errors.Wrap(err, "AMQP Subscribe() - Queue declare error")
		}

		durable, autoDelete, args = t.getExchangeOptions()
		if err := channel.ExchangeDeclare(topic, "fanout", durable, autoDelete, false, false, args); err != nil {
			return errors.Wrap(err, "AMQP Subscribe() - Exchange declare error")
		}

		if err := channel.QueueBind(b.queueName, b.pattern, b.topic, false, nil); err != nil {
			return errors.Wrap(err, "AMQP Subscribe() - Can't bind queue to exchange")
		}

		go t.doConsume(channel, queueName, false, subscriber.handler)
	}

	return nil
}

func (t *AmqpTransporter) Publish(command, nodeID string, message moleculer.Payload) {
	if t.connection == nil {
		msg := fmt.Sprint("AMQP Publish() No connection -> command: ", command, " nodeID: ", nodeID)
		t.logger.Error(msg)
		panic(errors.New(msg))
	}

	if t.connectionRecovering {
		t.waitForRecovering()
	}

	topic := t.topicName(command, nodeID)
	routingKey := ""

	t.publishMutex.Lock()
	defer t.publishMutex.Unlock()

	if t.publishChannel == nil {
		msg := fmt.Sprint("AMQP Publish() No channel found -> command: ", command, " nodeID: ", nodeID)
		t.logger.Error(msg)
		panic(errors.New(msg))
	}

	if nodeID != "" {
		routingKey = topic
		topic = ""
	}

	data := t.serializer.PayloadToBytes(message)

	msg := amqp.Publishing{
		Body: data,
	}

	if err := t.publishChannel.Publish(topic, routingKey, false, false, msg); err != nil {
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

func (t *AmqpTransporter) doConsume(channel *amqp.Channel, queueName string, needAck bool, handler transit.TransportHandler) {
	t.logger.Debug("AMQP doConsume() - queue: ", queueName)

	msgs, err := channel.Consume(queueName, "", !needAck, false, false, true, t.opts.ConsumeOptions)
	if err != nil {
		t.logger.Errorf("AMQP doConsume - Can't start consume for queue '%s': %s", queueName, err)
		return
	}

	for {
		msg, ok := <-msgs
		if !ok {
			break
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
