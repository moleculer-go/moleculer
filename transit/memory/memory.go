package memory

import (
	"errors"
	"fmt"

	"github.com/moleculer-go/moleculer/transit"
	log "github.com/sirupsen/logrus"
)

type MemoryTransporter struct {
	prefix         string
	logger         *log.Entry
	messageIsValid transit.ValidateMsgFunc
	connected      bool
	handlers       map[string][]transit.TransportHandler
}

func CreateTransporter(prefix string, logger *log.Entry, messageIsValid transit.ValidateMsgFunc) MemoryTransporter {
	handlers := make(map[string][]transit.TransportHandler)
	return MemoryTransporter{prefix: prefix, logger: logger, messageIsValid: messageIsValid, connected: false, handlers: handlers}
}

func (transporter *MemoryTransporter) Connect() chan bool {
	endChan := make(chan bool)
	transporter.connected = true
	go func() {
		endChan <- transporter.connected
	}()
	transporter.logger.Debug("Memory transporter connected!")
	return endChan
}

func (transporter *MemoryTransporter) Disconnect() chan bool {
	endChan := make(chan bool)
	transporter.connected = false
	go func() {
		endChan <- true
	}()
	transporter.logger.Debug("Memory transporter disconnected!")
	return endChan
}

func topicName(transporter *MemoryTransporter, command string, nodeID string) string {
	if nodeID != "" {
		return fmt.Sprint(transporter.prefix, ".", command, ".", nodeID)
	}
	return fmt.Sprint(transporter.prefix, ".", command)
}

func (transporter *MemoryTransporter) Subscribe(command string, nodeID string, handler transit.TransportHandler) {
	if !transporter.connected {
		panic(errors.New("Transport is not connected !"))
	}
	topic := topicName(transporter, command, nodeID)
	transporter.logger.Trace("memory.Subscribe() listen for command: ", command, " nodeID: ", nodeID, " topic: ", topic)

	_, exists := transporter.handlers[topic]
	if exists {
		transporter.handlers[topic] = append(transporter.handlers[topic], handler)
	} else {
		transporter.handlers[topic] = []transit.TransportHandler{handler}
	}
}

func (transporter *MemoryTransporter) Publish(command, nodeID string, message transit.Message) {
	if !transporter.connected {
		panic(errors.New("Transport is not connected !"))
	}
	topic := topicName(transporter, command, nodeID)
	transporter.logger.Trace("memory.Publish() command: ", command, " nodeID: ", nodeID, " message: \n", message, "\n - end")

	handlers, exists := transporter.handlers[topic]
	if exists {
		for _, handler := range handlers {
			handler(message)
		}
	}
}
