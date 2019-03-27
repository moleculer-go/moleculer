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
	Prefix    string
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
	transport.prefix = options.Prefix
	transport.clusterID = options.ClusterID
	transport.clientID = options.ClientID
	transport.serializer = options.Serializer
	transport.logger = options.Logger
	transport.validateMsg = options.ValidateMsg
	return transport
}

func (transporter *StanTransporter) Connect() chan bool {
	endChan := make(chan bool)
	go func() {
		transporter.logger.Debug("Connect() - url: ", transporter.url, " clusterID: ", transporter.clusterID, " clientID: ", transporter.clientID)
		connection, error := stan.Connect(transporter.clusterID, transporter.clientID, stan.NatsURL(transporter.url))
		if error != nil {
			transporter.logger.Error("\nConnect() - Error: ", error, " clusterID: ", transporter.clusterID, " clientID: ", transporter.clientID)
			panic("Error trying to connect to stan server. url: " + transporter.url + " clusterID: " + transporter.clusterID + " clientID: " + transporter.clientID + " -> Stan error: " + error.Error())
		}
		transporter.logger.Info("Connect() - connection success!")

		transporter.connection = connection
		endChan <- true
	}()
	return endChan
}

func (transporter *StanTransporter) Disconnect() chan bool {
	endChan := make(chan bool)
	go func() {
		transporter.logger.Debug("Disconnect() # of subscriptions: ", len(transporter.subscriptions))
		for _, sub := range transporter.subscriptions {
			error := sub.Unsubscribe()
			if error != nil {
				transporter.logger.Error("Disconnect() error when unsubscribing stan subscription: ", error)
			}
		}
		transporter.logger.Debug("Disconnect() subscriptions unsubscribed.")
		error := transporter.connection.Close()
		if error == nil {
			transporter.logger.Debug("Disconnect() stan connection closed :)")
		} else {
			transporter.logger.Error("Disconnect() error when closing stan connection :( ", error)
		}

		transporter.connection = nil
		endChan <- true
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

func (transporter *StanTransporter) Subscribe(command string, nodeID string, handler transit.TransportHandler) {
	if transporter.connection == nil {
		msg := fmt.Sprint("stan.Subscribe() No connection :( -> command: ", command, " nodeID: ", nodeID)
		transporter.logger.Warn(msg)
		panic(errors.New(msg))
	}

	topic := topicName(transporter, command, nodeID)
	transporter.logger.Trace("stan.Subscribe() command: ", command, " nodeID: ", nodeID, " topic: ", topic)

	sub, error := transporter.connection.Subscribe(topic, func(msg *stan.Msg) {
		transporter.logger.Trace("stan.Subscribe() command: ", command, " nodeID: ", nodeID, " msg: \n", msg, "\n - end")
		message := transporter.serializer.BytesToPayload(&msg.Data)
		if transporter.validateMsg(message) {
			handler(message)
		}
	})
	if error != nil {
		transporter.logger.Error("Subscribe() - Error: ", error)
		panic(error)
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
	bmsg := transporter.serializer.PayloadToBytes(message)
	transporter.connection.Publish(topic, bmsg)
}
