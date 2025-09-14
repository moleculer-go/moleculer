package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/serializer"
	"github.com/moleculer-go/moleculer/transit"
	"github.com/segmentio/kafka-go"

	log "github.com/sirupsen/logrus"
)

var DefaultConfig = KafkaOptions{
	partition: 0,
}

type subscriber struct {
	command string
	nodeID  string
	handler transit.TransportHandler
}

type subscription struct {
	doneChannel chan bool
	reader      *kafka.Reader
	ctx         context.Context
	cancel      context.CancelFunc
}

type KafkaTransporter struct {
	prefix     string
	opts       *KafkaOptions
	logger     *log.Entry
	serializer serializer.Serializer

	connectionEnable bool
	nodeID           string
	subscribers      []subscriber
	subscriptions    []*subscription
	publishers       map[string]*kafka.Writer
	shutdownCtx      context.Context
	shutdownCancel   context.CancelFunc
	shutdownMutex    sync.Mutex
}

type KafkaOptions struct {
	Url        string
	Addr       string
	Name       string
	Logger     *log.Entry
	Serializer serializer.Serializer

	partition int
}

func mergeConfigs(baseConfig KafkaOptions, userConfig KafkaOptions) KafkaOptions {

	if len(userConfig.Url) != 0 {
		baseConfig.Url = userConfig.Url
	}

	if len(userConfig.Addr) != 0 {
		baseConfig.Addr = strings.Replace(userConfig.Url, "kafka://", "", 1)
	} else {
		baseConfig.Addr = userConfig.Addr
	}

	if userConfig.Logger != nil {
		baseConfig.Logger = userConfig.Logger
	}

	if userConfig.Logger != nil {
		baseConfig.Logger = userConfig.Logger
	}

	if userConfig.partition != 0 {
		baseConfig.partition = userConfig.partition
	}

	return baseConfig
}

func CreateKafkaTransporter(options KafkaOptions) transit.Transport {
	options = mergeConfigs(DefaultConfig, options)
	ctx, cancel := context.WithCancel(context.Background())

	return &KafkaTransporter{
		opts:           &options,
		logger:         options.Logger,
		publishers:     make(map[string]*kafka.Writer),
		shutdownCtx:    ctx,
		shutdownCancel: cancel,
	}
}

func (t *KafkaTransporter) Connect(registry moleculer.Registry) chan error {
	endChan := make(chan error)
	go func() {
		t.logger.Debug("Kafka Connect() - url: ", t.opts.Url)

		topic := t.topicName("PING", t.nodeID)
		_, err := kafka.DialLeader(context.Background(), "tcp", t.opts.Addr, topic, t.opts.partition)

		if err != nil {
			t.logger.Error("Kafka Connect() - Error: ", err, " url: ", t.opts.Url)
			endChan <- errors.New(fmt.Sprint("Error connection to Kafka. error: ", err, " url: ", t.opts.Url))
			return
		}

		for _, subscriber := range t.subscribers {
			t.subscribeInternal(subscriber)
		}
		t.connectionEnable = true
		endChan <- nil
	}()
	return endChan
}

func (t *KafkaTransporter) Subscribe(command, nodeID string, handler transit.TransportHandler) {
	if !t.connectionEnable {
		panic("KafkaTransporter disconnected")
	}
	subscriber := subscriber{command, nodeID, handler}
	t.subscribers = append(t.subscribers, subscriber)
	t.subscribeInternal(subscriber)
}

func (t *KafkaTransporter) subscribeInternal(subscriber subscriber) {
	topic := t.topicName(subscriber.command, subscriber.nodeID)
	doneChannel := make(chan bool)
	autoDelete := t.getQueueOptions(subscriber.command)

	// Create context for this subscription
	ctx, cancel := context.WithCancel(t.shutdownCtx)

	// Create reader first
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:         []string{t.opts.Addr},
		Topic:           topic,
		GroupID:         t.nodeID,
		Partition:       t.opts.partition,
		ReadLagInterval: -1,
	})

	subscription := &subscription{
		doneChannel: doneChannel,
		reader:      reader,
		ctx:         ctx,
		cancel:      cancel,
	}
	t.subscriptions = append(t.subscriptions, subscription)

	if subscriber.nodeID == "" {
		go t.doConsume(topic, subscriber.handler, autoDelete, doneChannel, reader, ctx)
	} else {
		queueName := t.prefix + "." + subscriber.command + "." + t.nodeID
		go t.doConsume(queueName, subscriber.handler, autoDelete, doneChannel, reader, ctx)
	}
}

func (t *KafkaTransporter) getQueueOptions(command string) (autoDelete bool) {
	switch command {
	// Requests and responses don't expire.
	case "REQ", "RES", "EVENT", "EVENTLB":
		autoDelete = false

	// Packet types meant for internal use
	case "HEARTBEAT", "DISCOVER", "DISCONNECT", "INFO", "PING", "PONG":
		autoDelete = true
	}
	return
}

func (t *KafkaTransporter) doConsume(
	queueName string, handler transit.TransportHandler, autoDelete bool, doneChannel chan bool, reader *kafka.Reader, ctx context.Context) {
	defer t.closeReader(reader)

	messageChannel := make(chan []byte)
	errorChannel := make(chan error)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				var msg kafka.Message
				var err error
				if autoDelete {
					msg, err = reader.ReadMessage(ctx)
				} else {
					msg, err = reader.FetchMessage(ctx)
				}
				if err != nil {
					select {
					case errorChannel <- err:
					case <-ctx.Done():
						return
					}
					continue
				}
				select {
				case messageChannel <- msg.Value:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	for {
		select {
		case err := <-errorChannel:
			if err != nil {
				t.logger.Error("failed to read messages:", err)
			}
		case msg := <-messageChannel:
			payload := t.serializer.BytesToPayload(&msg)
			handler(payload)
		case <-doneChannel:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (t *KafkaTransporter) closeReader(reader *kafka.Reader) {
	if err := reader.Close(); err != nil {
		t.logger.Error("Could not close topic reader:", err)
	}
}

func (t *KafkaTransporter) Disconnect() chan error {
	errChan := make(chan error)
	go func() {
		t.shutdownMutex.Lock()
		defer t.shutdownMutex.Unlock()

		// Cancel all subscription contexts first
		for _, subscription := range t.subscriptions {
			if subscription.cancel != nil {
				subscription.cancel()
			}
		}

		// Send shutdown signal to all subscriptions
		for _, subscription := range t.subscriptions {
			select {
			case subscription.doneChannel <- true:
			default:
				// Channel might be full, continue
			}
		}

		// Wait a moment for goroutines to receive shutdown signal
		time.Sleep(200 * time.Millisecond)

		// Close all readers to ensure goroutines exit
		for _, subscription := range t.subscriptions {
			if subscription.reader != nil {
				t.closeReader(subscription.reader)
			}
		}

		// Clean up publishers
		for _, publisher := range t.publishers {
			t.closeWriter(publisher)
		}

		// Cancel the main shutdown context
		if t.shutdownCancel != nil {
			t.shutdownCancel()
		}

		// Clear subscriptions slice to prevent memory leaks
		t.subscriptions = nil

		// Clear subscribers slice to prevent memory leaks
		t.subscribers = nil

		// Clear publishers map to prevent memory leaks
		t.publishers = make(map[string]*kafka.Writer)

		t.connectionEnable = false
		errChan <- nil
	}()

	return errChan
}

func (t *KafkaTransporter) Publish(command, nodeID string, message moleculer.Payload) {
	if !t.connectionEnable {
		panic("KafkaTransporter disconnected")
	}
	topic := t.topicName(command, nodeID)

	data := t.serializer.PayloadToBytes(message)
	t.publishMessage(data, topic)
}

func (t *KafkaTransporter) publishMessage(message []byte, topic string) {
	writer := t.publishers[topic]
	if writer == nil {
		writer = &kafka.Writer{
			Addr:         kafka.TCP(t.opts.Addr),
			Topic:        topic,
			Balancer:     &kafka.LeastBytes{},
			BatchTimeout: 10 * time.Millisecond,
		}
		t.publishers[topic] = writer
	}

	err := writer.WriteMessages(context.Background(),
		kafka.Message{
			Value: message,
		},
	)

	if err != nil {
		t.logger.Error("failed to write messages:", err)
		return
	}
}

func (t *KafkaTransporter) closeWriter(writer *kafka.Writer) {
	if err := writer.Close(); err != nil {
		t.logger.Fatal("Could not close topic writer:", err)
	}
}

func (t *KafkaTransporter) SetPrefix(prefix string) {
	t.prefix = prefix
}

func (t *KafkaTransporter) SetNodeID(nodeID string) {
	t.nodeID = nodeID
}

func (t *KafkaTransporter) SetSerializer(serializer serializer.Serializer) {
	t.serializer = serializer
}

func (t *KafkaTransporter) GetMetrics() map[string]interface{} {
	return map[string]interface{}{
		"type":   "kafka",
		"active": t.connectionEnable,
		"url":    t.opts.Url,
	}
}

func (t *KafkaTransporter) topicName(command string, nodeID string) string {
	parts := []string{t.prefix, command}
	if nodeID != "" {
		parts = append(parts, nodeID)
	}
	return strings.Join(parts, ".")
}
