package nats

import (
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/serializer"
	"github.com/moleculer-go/moleculer/transit"
	"github.com/nats-io/go-nats"
	log "github.com/sirupsen/logrus"
	"time"
	"fmt"
)

type NATSTransporter struct {
	opts *nats.Options
	conn *nats.Conn
	logger *log.Entry
	prefix string
	serializer serializer.Serializer
	subscriptions []*nats.Subscription
}

type NATSOptions struct {
	URL       string
	ClientID  string
	Logger     *log.Entry
	Serializer serializer.Serializer
}

func CreateNATSTransporter(options NATSOptions) transit.Transport {
	opts := nats.GetDefaultOptions()
	opts.Name = options.ClientID
	opts.Url = options.URL
	opts.AllowReconnect = false
	opts.ReconnectWait = time.Second * 2
	opts.MaxReconnect = -1

	transporter := &NATSTransporter{}
	transporter.opts = &opts
	transporter.logger = options.Logger

	if transporter.logger == nil {
		transporter.logger = log.NewEntry(log.New())
	}
	transporter.serializer = options.Serializer

	if transporter.serializer == nil {
		transporter.serializer = serializer.CreateJSONSerializer(transporter.logger)
	}

	transporter.subscriptions = make([]*nats.Subscription, 0)
	return transporter
}

func (t *NATSTransporter) Connect() chan bool {
	endChan := make(chan bool)

	go func() {
		conn, err := t.opts.Connect()
		if err != nil {
			t.logger.Error("Cannot connect to ", t.opts.Url, err)
			endChan <- true
			return
		}

		t.logger.Info("Connected to ", t.opts.Url)
		t.conn = conn
		endChan <- true
	}()

	return endChan
}

func (t *NATSTransporter) Disconnect() chan bool {
	endChan := make(chan bool)

	go func() {
		if t.conn == nil {
			endChan <- true
		}

		for _, sub := range t.subscriptions {
			if err := sub.Unsubscribe(); err != nil {
				t.logger.Error(err)
			}
		}

		t.conn.Close()
		endChan <- true
	}()

	return endChan
}

func (t *NATSTransporter) Subscribe(command, nodeID string, handler transit.TransportHandler) {
	topic := t.prefix + "." + command
	if nodeID != "" {
		topic = topic + "." + nodeID
	}
	t.logger.Info("Subscribe to ", topic)

	sub, err := t.conn.Subscribe(topic, func(msg *nats.Msg) {
		payload := t.serializer.BytesToPayload(&msg.Data)
		t.logger.Debug(fmt.Sprintf("Incoming %s packet from '%s'", topic, payload.Get("sender").String()))
		handler(payload)
	})
	if err != nil {
		t.logger.Error("Cannot subscribe: ", topic, " = ", err)
		return
	}
	t.subscriptions = append(t.subscriptions, sub)
}

func (t *NATSTransporter) Publish(command, nodeID string, message moleculer.Payload) {
	topic := t.prefix + "." + command
	if nodeID != "" {
		topic = topic + "." + nodeID
	}

	t.logger.Debug(fmt.Sprintf("Send %s packet to '%s'", command, nodeID))
	err := t.conn.Publish(topic, t.serializer.PayloadToBytes(message))
	if err != nil {
		t.logger.Error("Cannot publish: ", topic, " = ", err)
	}
}

func (t *NATSTransporter) SetPrefix(prefix string) {
	t.prefix = prefix
	t.logger.Debug("Set NATS prefix: ", t.prefix)
}
