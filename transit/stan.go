package transit

import (
	"errors"
	"fmt"

	. "github.com/moleculer-go/moleculer/common"
	stan "github.com/nats-io/go-nats-streaming"
	log "github.com/sirupsen/logrus"
)

type MessageIsValidFunction func(*TransitMessage) bool

type StanTransporter struct {
	prefix         string
	url            string
	clusterID      string
	clientID       string
	logger         *log.Entry
	messageIsValid MessageIsValidFunction

	serializer    *Serializer
	connection    *stan.Conn
	subscriptions []*stan.Subscription
	self          *StanTransporter
}

type StanTransporterOptions struct {
	Prefix    string
	URL       string
	ClusterID string
	ClientID  string

	Logger     *log.Entry
	Serializer *Serializer

	MessageIsValidHandler MessageIsValidFunction
}

func CreateStanTransporter(options StanTransporterOptions) StanTransporter {
	transport := StanTransporter{}
	transport.url = options.URL
	transport.prefix = options.Prefix
	transport.clusterID = options.ClusterID
	transport.clientID = options.ClientID
	transport.serializer = options.Serializer
	transport.logger = options.Logger
	transport.messageIsValid = options.MessageIsValidHandler

	transport.self = &transport
	return transport
}

// stanTopicName : return the topic name given the command and nodeID
func stanTopicName(transporter *StanTransporter, command string, nodeID string) string {
	if nodeID != "" {
		return fmt.Sprint(transporter.prefix, ".", command, ".", nodeID)
	}
	return fmt.Sprint(transporter.prefix, ".", command)
}

func (transporter StanTransporter) createTransitMessage(msg *stan.Msg) TransitMessage {
	return (*transporter.serializer).BytesToMessage(&msg.Data)
}

func (transporter StanTransporter) MakeBalancedSubscriptions() {

}

func (transporter *StanTransporter) getConnection() *stan.Conn {
	return transporter.self.connection
}
func (transporter *StanTransporter) setConnection(connection *stan.Conn) {
	transporter.self.connection = connection
}

func (transporter StanTransporter) Connect() chan bool {
	endChan := make(chan bool)
	go func() {
		transporter.logger.Debug("Connect() - url: ", transporter.url, " clusterID: ", transporter.clusterID, " clientID: ", transporter.clientID)
		connection, error := stan.Connect(transporter.clusterID, transporter.clientID, stan.NatsURL(transporter.url))
		if error != nil {
			transporter.logger.Error("\nConnect() - Error: ", error)
			panic(error)
		}
		transporter.logger.Info("Connect() - connection success!")

		transporter.setConnection(&connection)
		endChan <- true
	}()
	return endChan
}

func (transporter StanTransporter) Disconnect() chan bool {
	endChan := make(chan bool)
	go func() {
		(*transporter.getConnection()).Close()
		endChan <- true
	}()
	return endChan
}

func (transporter StanTransporter) Subscribe(command string, nodeID string, handler TransportHandler) {
	connection := *transporter.self.connection
	if connection == nil {
		msg := fmt.Sprint("stan.Subscribe() No connection :( -> command: ", command, " nodeID: ", nodeID)
		transporter.logger.Warn(msg)
		panic(errors.New(msg))
	}

	topic := stanTopicName(&transporter, command, nodeID)
	transporter.logger.Trace("stan.Subscribe() command: ", command, " nodeID: ", nodeID, " topic: ", topic)

	sub, error := connection.Subscribe(topic, func(msg *stan.Msg) {
		transporter.logger.Trace("stan.Subscribe() command: ", command, " nodeID: ", nodeID, " msg: \n", msg, "\n - end")
		transitMessage := transporter.createTransitMessage(msg)
		if transporter.messageIsValid(&transitMessage) {
			handler(transitMessage)
		}
	})
	if error != nil {
		transporter.logger.Error("Subscribe() - Error: ", error)
		panic(error)
	}
	transporter.subscriptions = append(transporter.subscriptions, &sub)
}

func (transporter StanTransporter) Publish(command, nodeID string, message TransitMessage) {
	topic := stanTopicName(&transporter, command, nodeID)
	transporter.logger.Trace("stan.Publish() command: ", command, " nodeID: ", nodeID, " message: \n", message, "\n - end")
	(*transporter.getConnection()).Publish(topic, []byte(message.String()))
}

func (transporter StanTransporter) Request(message TransitMessage) chan interface{} {
	resultChan := make(chan interface{})
	//PACKET_REQUEST 		= "REQ";

	return resultChan
}
