package nats

import (
	"fmt"

	"github.com/moleculer-go/moleculer/transit"
	stan "github.com/nats-io/go-nats-streaming"
)

type NatsStreaming struct {
	self          *NatsStreaming
	connection    *stan.Conn
	url           string
	clusterID     string
	clientID      string
	subscriptions []*stan.Subscription
}

func CreateNatsStreaming(clusterID string, clientID string, url string) NatsStreaming {
	transport := NatsStreaming{}
	transport.clusterID = clusterID
	transport.clientID = clientID
	transport.self = &transport
	transport.url = url
	return transport
}

// getCmdNodeTopic : create the topic name given the command and nodeID
func getCmdNodeTopic(command string, nodeID string) string {
	return fmt.Sprint(command, ":", nodeID)
}

func (transporter NatsStreaming) Subscribe(command string, nodeID string, handler transit.TransportHandler) {
	connection := *transporter.self.connection
	if connection == nil {
		fmt.Print("No connection.. fodeu !!!")
	}

	topic := getCmdNodeTopic(command, nodeID)
	fmt.Printf("\nSubscribe() - topic: %s", topic)
	sub, error := connection.Subscribe(topic, func(msg *stan.Msg) {
		fmt.Printf("\n(Subscribe) Received a message: %s\n", string(msg.Data))
		//handler(string(msg.Data))
	})
	if error != nil {
		fmt.Print("Subscribe() - Error: ", error)
		panic(error)
	}
	transporter.subscriptions = append(transporter.subscriptions, &sub)

	fmt.Print("\nSubscribe() - Done!")
}

func (transporter *NatsStreaming) getConnection() *stan.Conn {
	return transporter.self.connection
}
func (transporter *NatsStreaming) setConnection(connection *stan.Conn) {
	transporter.self.connection = connection
}

func (transporter NatsStreaming) Connect() chan bool {
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

func (transporter NatsStreaming) Disconnect() chan bool {
	endChan := make(chan bool)
	go func() {
		(*transporter.getConnection()).Close()
		endChan <- true
	}()
	return endChan
}

// func (transporter NatsStreaming) Publish(packet Packet) {
// 	(*transporter.getConnection()).Publish(packet.GetTarget(), []byte(packet.GetPayload()))
// }
