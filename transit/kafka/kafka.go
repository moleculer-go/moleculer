package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
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

	return &KafkaTransporter{
		opts:       &options,
		logger:     options.Logger,
		publishers: make(map[string]*kafka.Writer),
	}
}

func (t *KafkaTransporter) Connect() chan error {
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

	if subscriber.nodeID == "" {
		go t.doConsume(topic, subscriber.handler, autoDelete, doneChannel)
	} else {
		queueName := t.prefix + "." + subscriber.command + "." + t.nodeID
		go t.doConsume(queueName, subscriber.handler, autoDelete, doneChannel)
	}
	t.subscriptions = append(t.subscriptions, &subscription{
		doneChannel: doneChannel,
	})
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
	queueName string, handler transit.TransportHandler, autoDelete bool, doneChannel chan bool) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:         []string{t.opts.Addr},
		Topic:           queueName,
		GroupID:         t.nodeID,
		Partition:       t.opts.partition,
		ReadLagInterval: -1,
	})
	defer t.closeReader(reader)

	messageChannel := make(chan []byte)
	errorChannel := make(chan error)
	stopRead := make(chan bool)

	ctx := context.Background()

	go func() {
		for {
			select {
			case <-stopRead:
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
					errorChannel <- err
					continue
				}
				messageChannel <- msg.Value
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
			stopRead <- true
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
		for _, subscription := range t.subscriptions {
			subscription.doneChannel <- true
		}

		for _, publisher := range t.publishers {
			t.closeWriter(publisher)
		}
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

func (t *KafkaTransporter) topicName(command string, nodeID string) string {
	parts := []string{t.prefix, command}
	if nodeID != "" {
		parts = append(parts, nodeID)
	}
	return strings.Join(parts, ".")
}
