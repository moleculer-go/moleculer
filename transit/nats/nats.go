package nats

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/serializer"
	"github.com/moleculer-go/moleculer/transit"
	"github.com/nats-io/go-nats"
	log "github.com/sirupsen/logrus"
)

type NatsTransporter struct {
	prefix        string
	opts          *nats.Options
	conn          *nats.Conn
	logger        *log.Entry
	serializer    serializer.Serializer
	subscriptions []*nats.Subscription
}

type NATSOptions struct {
	URL         string
	Name        string
	Logger      *log.Entry
	Serializer  serializer.Serializer
	ValidateMsg transit.ValidateMsgFunc

	AllowReconnect bool
	ReconnectWait  time.Duration
	MaxReconnect   int
}

func natsOptions(options NATSOptions) *nats.Options {
	opts := nats.GetDefaultOptions()
	opts.Name = options.Name
	opts.Url = options.URL
	opts.AllowReconnect = options.AllowReconnect
	if options.ReconnectWait != 0 {
		opts.ReconnectWait = options.ReconnectWait
	}
	if options.MaxReconnect != 0 {
		opts.MaxReconnect = options.MaxReconnect
	}
	return &opts
}

func CreateNatsTransporter(options NATSOptions) transit.Transport {
	return &NatsTransporter{
		opts:          natsOptions(options),
		logger:        options.Logger,
		serializer:    options.Serializer,
		subscriptions: []*nats.Subscription{},
	}
}

func (t *NatsTransporter) Connect() chan error {
	endChan := make(chan error)
	go func() {
		t.logger.Debug("NATS Connect() - url: ", t.opts.Url, " Name: ", t.opts.Name)
		conn, err := t.opts.Connect()
		if err != nil {
			t.logger.Error("NATS Connect() - Error: ", err, " url: ", t.opts.Url, " Name: ", t.opts.Name)
			endChan <- errors.New(fmt.Sprint("Error connection to NATS. error: ", err, " url: ", t.opts.Url))
			return
		}

		t.logger.Info("Connected to ", t.opts.Url)
		t.conn = conn
		endChan <- nil
	}()
	return endChan
}

func (t *NatsTransporter) Disconnect() chan error {
	endChan := make(chan error)
	go func() {
		if t.conn == nil {
			endChan <- nil
			return
		}
		for _, sub := range t.subscriptions {
			if err := sub.Unsubscribe(); err != nil {
				t.logger.Error(err)
			}
		}
		t.conn.Close()
		t.conn = nil
		endChan <- nil
	}()
	return endChan
}

func (t *NatsTransporter) topicName(command string, nodeID string) string {
	parts := []string{t.prefix, command}
	if nodeID != "" {
		parts = append(parts, nodeID)
	}
	return strings.Join(parts, ".")
}

func (t *NatsTransporter) Subscribe(command, nodeID string, handler transit.TransportHandler) {
	if t.conn == nil {
		msg := fmt.Sprint("nats.Subscribe() No connection :( -> command: ", command, " nodeID: ", nodeID)
		t.logger.Warn(msg)
		panic(errors.New(msg))
	}

	topic := t.topicName(command, nodeID)

	sub, err := t.conn.Subscribe(topic, func(msg *nats.Msg) {
		payload := t.serializer.BytesToPayload(&msg.Data)
		t.logger.Debug(fmt.Sprintf("Incoming %s packet from '%s'", topic, payload.Get("sender").String()))
		handler(payload)
	})
	if err != nil {
		t.logger.Error("Cannot subscribe: ", topic, " error: ", err)
		return
	}
	t.subscriptions = append(t.subscriptions, sub)
}

func (t *NatsTransporter) Publish(command, nodeID string, message moleculer.Payload) {
	if t.conn == nil {
		msg := fmt.Sprint("nats.Publish() No connection :( -> command: ", command, " nodeID: ", nodeID)
		t.logger.Warn(msg)
		panic(errors.New(msg))
	}

	topic := t.topicName(command, nodeID)
	t.logger.Debug("nats.Publish() command: ", command, " topic: ", topic, " nodeID: ", nodeID)
	t.logger.Trace("message: \n", message, "\n - end")
	err := t.conn.Publish(topic, t.serializer.PayloadToBytes(message))
	if err != nil {
		t.logger.Error("Error on publish: error: ", err, " command: ", command, " topic: ", topic)
		panic(err)
	}
}

func (t *NatsTransporter) SetPrefix(prefix string) {
	t.prefix = prefix
}

func (t *NatsTransporter) SetNodeID(nodeID string) {
}

func (t *NatsTransporter) SetSerializer(serializer serializer.Serializer) {
	// Ignored while transporter initialized in pubsub function
}
