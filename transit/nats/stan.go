package nats

import (
	"errors"
	"fmt"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/serializer"
	"github.com/moleculer-go/moleculer/transit"
	stan "github.com/nats-io/go-nats-streaming"
	log "github.com/sirupsen/logrus"
)

type StanTransporter struct {
	prefix      string
	url         string
	clusterID   string
	clientID    string
	logger      *log.Entry
	validateMsg transit.ValidateMsgFunc

	serializer    serializer.Serializer
	connection    stan.Conn
	subscriptions []stan.Subscription
}

type StanOptions struct {
	URL       string
	ClusterID string
	ClientID  string

	Logger     *log.Entry
	Serializer serializer.Serializer

	ValidateMsg transit.ValidateMsgFunc
}

func CreateStanTransporter(options StanOptions) StanTransporter {
	transport := StanTransporter{}
	transport.url = options.URL
	transport.clusterID = options.ClusterID
	transport.clientID = options.ClientID
	transport.serializer = options.Serializer
	transport.logger = options.Logger
	transport.validateMsg = options.ValidateMsg
	return transport
}

func (transporter *StanTransporter) Connect() chan error {
	endChan := make(chan error)
	go func() {
		transporter.logger.Debug("STAN Connect() - url: ", transporter.url, " clusterID: ", transporter.clusterID, " clientID: ", transporter.clientID)
		connection, err := stan.Connect(transporter.clusterID, transporter.clientID, stan.NatsURL(transporter.url))
		if err != nil {
			transporter.logger.Error("STAN Connect() - Error: ", err, " clusterID: ", transporter.clusterID, " clientID: ", transporter.clientID)
			//panic("Error trying to connect to stan server. url: " + transporter.url + " clusterID: " + transporter.clusterID + " clientID: " + transporter.clientID + " -> Stan error: " + error.Error())
			endChan <- err
			return
		}
		transporter.logger.Info("STAN Connect() - connection success!")
		transporter.connection = connection
		endChan <- nil
	}()
	return endChan
}

func (transporter *StanTransporter) Disconnect() chan error {
	endChan := make(chan error)
	go func() {
		if transporter.connection == nil {
			endChan <- nil
			return
		}
		transporter.logger.Debug("Disconnect() # of subscriptions: ", len(transporter.subscriptions))
		for _, sub := range transporter.subscriptions {
			error := sub.Unsubscribe()
			if error != nil {
				transporter.logger.Error("Disconnect() error when unsubscribing stan subscription: ", error)
			}
		}
		transporter.logger.Debug("Disconnect() subscriptions unsubscribed.")
		err := transporter.connection.Close()
		if err == nil {
			transporter.logger.Debug("Disconnect() stan connection closed :)")
			endChan <- nil
		} else {
			transporter.logger.Error("Disconnect() error when closing stan connection :( ", err)
			endChan <- err
		}
		transporter.connection = nil
	}()
	return endChan
}

func topicName(transporter *StanTransporter, command string, nodeID string) string {
	if nodeID != "" {
		return fmt.Sprint(transporter.prefix, ".", command, ".", nodeID)
	}
	return fmt.Sprint(transporter.prefix, ".", command)
}

func (transporter *StanTransporter) SetPrefix(prefix string) {
	transporter.prefix = prefix
}

func (transporter *StanTransporter) SetNodeID(nodeID string) {
}

func (transporter *StanTransporter) SetSerializer(serializer serializer.Serializer) {
	// Ignored while transporter initialized in pubsub function
}

func (transporter *StanTransporter) Subscribe(command string, nodeID string, handler transit.TransportHandler) {
	if transporter.connection == nil {
		msg := fmt.Sprint("stan.Subscribe() No connection :( -> command: ", command, " nodeID: ", nodeID)
		transporter.logger.Warn(msg)
		panic(errors.New(msg))
	}

	topic := topicName(transporter, command, nodeID)
	transporter.logger.Trace("stan.Subscribe() command: ", command, " nodeID: ", nodeID, " topic: ", topic)

	sub, err := transporter.connection.Subscribe(topic, func(msg *stan.Msg) {
		transporter.logger.Trace("stan.Subscribe() command: ", command, " nodeID: ", nodeID, " msg: \n", msg, "\n - end")
		message := transporter.serializer.BytesToPayload(&msg.Data)
		if transporter.validateMsg(message) {
			handler(message)
		}
	})
	if err != nil {
		transporter.logger.Error("Subscribe() - Error: ", err)
		panic(err)
	}
	transporter.subscriptions = append(transporter.subscriptions, sub)
}

func (transporter *StanTransporter) Publish(command, nodeID string, message moleculer.Payload) {
	if transporter.connection == nil {
		msg := fmt.Sprint("stan.Publish() No connection :( -> command: ", command, " nodeID: ", nodeID)
		transporter.logger.Warn(msg)
		panic(errors.New(msg))
	}
	topic := topicName(transporter, command, nodeID)
	transporter.logger.Trace("stan.Publish() command: ", command, " nodeID: ", nodeID, " message: \n", message, "\n - end")
	err := transporter.connection.Publish(topic, transporter.serializer.PayloadToBytes(message))
	if err != nil {
		transporter.logger.Error("Error on publish: error: ", err, " command: ", command, " topic: ", topic)
		panic(err)
	}
}
