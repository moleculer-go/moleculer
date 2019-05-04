package memory

import (
	"fmt"
	"sync"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/transit"
	"github.com/moleculer-go/moleculer/util"
	log "github.com/sirupsen/logrus"
)

type Subscription struct {
	id      string
	handler transit.TransportHandler
	active  bool
}

type SharedMemory struct {
	handlers map[string][]Subscription
	mutex    *sync.Mutex
}

type MemoryTransporter struct {
	prefix        string
	instanceID    string
	logger        *log.Entry
	memory        *SharedMemory
	subscriptions []Subscription
}

func Create(logger *log.Entry, memory *SharedMemory) MemoryTransporter {
	instanceID := util.RandomString(5)
	subscriptions := make([]Subscription, 0)
	if memory.handlers == nil {
		memory.handlers = make(map[string][]Subscription)
	}
	if memory.mutex == nil {
		memory.mutex = &sync.Mutex{}
	}
	return MemoryTransporter{memory: memory, logger: logger, instanceID: instanceID, subscriptions: subscriptions}
}

func (transporter *MemoryTransporter) SetPrefix(prefix string) {
	transporter.prefix = prefix
}

func (transporter *MemoryTransporter) Connect() chan error {
	transporter.logger.Debug("[Mem-Trans-", transporter.instanceID, "] -> Connecting() ...")
	endChan := make(chan error)
	go func() {
		endChan <- nil
	}()
	transporter.logger.Info("[Mem-Trans-", transporter.instanceID, "] -> Connected() !")
	return endChan
}

func (transporter *MemoryTransporter) Disconnect() chan error {
	endChan := make(chan error)
	transporter.logger.Debug("[Mem-Trans-", transporter.instanceID, "] -> Disconnecting() ...")
	for _, subscription := range transporter.subscriptions {
		subscription.active = false
		subscription.handler = nil
	}
	go func() {
		endChan <- nil
	}()
	transporter.logger.Info("[Mem-Trans-", transporter.instanceID, "] -> Disconnected() !")
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
	transporter.logger.Trace("[Mem-Trans-", transporter.instanceID, "] Subscribe() listen for command: ", command, " nodeID: ", nodeID, " topic: ", topic)

	subscription := Subscription{util.RandomString(5), handler, true}
	transporter.subscriptions = append(transporter.subscriptions, subscription)

	transporter.memory.mutex.Lock()
	_, exists := transporter.memory.handlers[topic]
	if exists {
		transporter.memory.handlers[topic] = append(transporter.memory.handlers[topic], subscription)
	} else {
		transporter.memory.handlers[topic] = []Subscription{subscription}
	}
	transporter.memory.mutex.Unlock()
}

func (transporter *MemoryTransporter) Publish(command, nodeID string, message moleculer.Payload) {
	topic := topicName(transporter, command, nodeID)
	transporter.logger.Trace("[Mem-Trans-", transporter.instanceID, "] Publish() command: ", command, " nodeID: ", nodeID, " message: \n", message, "\n - end")

	transporter.memory.mutex.Lock()
	subscriptions, exists := transporter.memory.handlers[topic]
	transporter.memory.mutex.Unlock()
	if exists {
		for _, subscription := range subscriptions {
			if subscription.active {
				go subscription.handler(message)
			}
		}
	}
}
