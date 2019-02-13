package transit

import (
	"github.com/moleculer-go/moleculer"
)

type TransportHandler func(moleculer.Payload)

type ValidateMsgFunc func(moleculer.Payload) bool

type Transit interface {
	Emit(moleculer.BrokerContext)
	Request(moleculer.BrokerContext) chan moleculer.Payload
	Connect() chan bool
	Disconnect() chan bool
	DiscoverNode(nodeID string)

	//DiscoverNodes checks if there are neighbours and return true if any are found ;).
	DiscoverNodes() chan bool
	SendHeartbeat()
}

type Transport interface {
	Connect() chan bool
	Disconnect() chan bool
	Subscribe(command, nodeID string, handler TransportHandler)
	Publish(command, nodeID string, message moleculer.Payload)

	SetPrefix(prefix string)
}
