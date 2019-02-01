package transit

import (
	"github.com/moleculer-go/moleculer"
)

type TransportHandler func(message Message)

type ValidateMsgFunc func(Message) bool

type Transit interface {
	Request(moleculer.BrokerContext) chan interface{}
	Connect() chan bool
	DiscoverNode(nodeID string)

	//DiscoverNodes checks if there are neighbours and return true if any are found ;).
	DiscoverNodes() chan bool
	SendHeartbeat()
}

//TODO add the reminaing from Result type from GJSON (https://github.com/tidwall/gjson)
type Message interface {
	AsMap() map[string]interface{}
	Exists() bool
	Value() interface{}
	Int() int64
	Float() float64
	String() string
	Get(path string) Message
}
type Transport interface {
	Connect() chan bool
	Disconnect() chan bool
	Subscribe(command, nodeID string, handler TransportHandler)
	Publish(command, nodeID string, message Message)
}
