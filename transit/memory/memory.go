package memory

import (
	"fmt"

	bus "github.com/moleculer-go/goemitter"

	"github.com/moleculer-go/moleculer/transit"
	log "github.com/sirupsen/logrus"
)

type MemoryTransporter struct {
	events         *bus.Emitter
	prefix         string
	logger         *log.Entry
	messageIsValid transit.ValidateMsgFunc
}

func CreateTransporter(prefix string, logger *log.Entry, messageIsValid transit.ValidateMsgFunc) MemoryTransporter {
	events := bus.Construct()
	return MemoryTransporter{events, prefix, logger, messageIsValid}
}

func (transporter *MemoryTransporter) Connect() chan bool {
	endChan := make(chan bool)
	go func() {
		endChan <- true
	}()
	return endChan
}

func (transporter *MemoryTransporter) Disconnect() chan bool {
	endChan := make(chan bool)
	go func() {
		endChan <- true
	}()
	return endChan
}

func topicName(transporter *MemoryTransporter, command string, nodeID string) string {
	if nodeID != "" {
		return fmt.Sprint(transporter.prefix, ".", command, ".", nodeID)
	}
	return fmt.Sprint(transporter.prefix, ".", command)
}

func (transporter *MemoryTransporter) Subscribe(command string, nodeID string, handler transit.TransportHandler) {
	topic := topicName(transporter, command, nodeID)
	transporter.logger.Trace("memory.Subscribe() command: ", command, " nodeID: ", nodeID, " topic: ", topic)

	transporter.events.On(topic, func(args ...interface{}) {
		msg := args[0]
		transporter.logger.Trace("stan.Subscribe() command: ", command, " nodeID: ", nodeID, " msg: \n", msg, "\n - end")
		message := msg.(transit.Message)
		if transporter.messageIsValid(message) {
			handler(message)
		}
	})
}

func (transporter *MemoryTransporter) Publish(command, nodeID string, message transit.Message) {
	topic := topicName(transporter, command, nodeID)
	transporter.logger.Trace("stan.Publish() command: ", command, " nodeID: ", nodeID, " message: \n", message, "\n - end")
	transporter.events.EmitAsync(topic, []interface{}{message})

}
