package transit

import (
	"time"

	. "github.com/moleculer-go/moleculer/serializer"
	. "github.com/moleculer-go/moleculer/util"
)

// type packetTypeKey int

// const (
// 	PacketTypeEvent packetTypeKey = iota
// 	PacketTypeAction
// )

type TransportHandler func(message TransitMessage)

type Transport interface {
	Subscribe(command, nodeID string, handler TransportHandler)
	MakeBalancedSubscriptions()
	Publish(command, nodeID string, message TransitMessage)
	Connect() chan bool
	Disconnect() chan bool
}

// // getCmdNodeTopic : create the topic name given the command and nodeID
// func getCmdNodeTopic(transport *Transport, command string, nodeID string) string {
// 	return fmt.Sprint(transport.GetPrefix(), ".", command, ".", nodeID)
// }

type Transit struct {
	transport  *Transport
	serializer *Serializer
	isReady    bool
}

func CreateTransit(serializer *Serializer) *Transit {
	transit := Transit{
		serializer: serializer,
		isReady:    false,
	}
	return &transit
}

// CreateTransport : based on config it will load the transporter
// for now is hard coded for NATS Streaming localhost
func CreateTransport(serializer *Serializer) *Transport {
	//TODO: move this to config and params
	prefix := "MOL"
	url := "stan://localhost:4222"
	clusterID := "test-cluster"
	nodeID := RandomString(5)

	options := StanTransporterOptions{
		prefix,
		url,
		clusterID,
		nodeID,
		serializer,
	}

	var transport Transport = CreateStanTransporter(options)
	return &transport
}

func (transit *Transit) Connect() chan bool {
	endChan := make(chan bool)
	if transit.isReady {
		endChan <- true
		return endChan
	}
	transit.transport = CreateTransport(transit.serializer)
	go func() {
		connected := <-(*transit.transport).Connect()
		transit.isReady = connected
		endChan <- connected
	}()
	return endChan
}

func (transit *Transit) Ready() chan bool {
	endChan := make(chan bool)
	go func() {
		for {
			if transit.isReady {
				endChan <- true
			}
			time.Sleep(500 * time.Millisecond)
		}
	}()
	return endChan
}
