package transit

import (
	"fmt"

	. "github.com/moleculer-go/moleculer/serializer"
	stan "github.com/nats-io/go-nats-streaming"
)

type StanTransporter struct {
	prefix    string
	url       string
	clusterID string
	clientID  string

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

	Serializer *Serializer
}

func CreateStanTransporter(options StanTransporterOptions) StanTransporter {
	transport := StanTransporter{}
	transport.url = options.URL
	transport.prefix = options.Prefix
	transport.clusterID = options.ClusterID
	transport.clientID = options.ClientID
	transport.serializer = options.Serializer

	transport.self = &transport
	return transport
}

// stanTopicName : return the topic name given the command and nodeID
func stanTopicName(transporter *StanTransporter, command string, nodeID string) string {
	if nodeID != "" {
		return fmt.Sprint(transporter.prefix, ".", command, ":", nodeID)
	}
	return fmt.Sprint(transporter.prefix, ".", command)
}

func (transporter StanTransporter) createTransitMessage(msg *stan.Msg) TransitMessage {
	return (*transporter.serializer).BytesToMessage(msg.Data)
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
		fmt.Println("Connect() - url: ", transporter.url, " clusterID: ", transporter.clusterID)
		connection, error := stan.Connect(transporter.clusterID, transporter.clientID, stan.NatsURL(transporter.url))
		if error != nil {
			fmt.Print("\nConnect() - Error: ", error)
			panic(error)
		}
		fmt.Println("Connect() - connection success!")

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
		fmt.Print("No connection.. fodeu !!!")
	}

	topic := stanTopicName(&transporter, command, nodeID)
	fmt.Printf("\nSubscribe() - topic: %s", topic)
	sub, error := connection.Subscribe(topic, func(msg *stan.Msg) {
		fmt.Printf("\n(Subscribe) Received a message: %s\n", string(msg.Data))
		handler(transporter.createTransitMessage(msg))
	})
	if error != nil {
		fmt.Print("Subscribe() - Error: ", error)
		panic(error)
	}
	transporter.subscriptions = append(transporter.subscriptions, &sub)
	fmt.Print("\nSubscribe() - Done!")
}

func (transporter StanTransporter) Publish(command, nodeID string, message TransitMessage) {
	topic := stanTopicName(&transporter, command, nodeID)
	(*transporter.getConnection()).Publish(topic, []byte(message.String()))
}
