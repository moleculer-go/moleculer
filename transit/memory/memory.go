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
}

type MemoryTransporter struct {
	prefix        string
	instanceID    string
	logger        *log.Entry
	memory        *SharedMemory
	mutex         *sync.Mutex
	subscriptions []Subscription
}

func Create(logger *log.Entry, memory *SharedMemory) MemoryTransporter {
	instanceID := util.RandomString(5)
	mutex := &sync.Mutex{}
	subscriptions := make([]Subscription, 0)
	if memory.handlers == nil {
		memory.handlers = make(map[string][]Subscription)
	}
	return MemoryTransporter{mutex: mutex, memory: memory, logger: logger, instanceID: instanceID, subscriptions: subscriptions}
}

func (transporter *MemoryTransporter) SetPrefix(prefix string) {
	transporter.prefix = prefix
}

func (transporter *MemoryTransporter) Connect() chan bool {
	transporter.logger.Info("Memory Transporter (", transporter.instanceID, ") -> Connect() ... ")
	endChan := make(chan bool)
	go func() {
		endChan <- true
	}()
	return endChan
}

func (transporter *MemoryTransporter) Disconnect() chan bool {
	transporter.logger.Info("Memory Transporterr (", transporter.instanceID, ") -> Disconnect() !")
	endChan := make(chan bool)

	for _, subscription := range transporter.subscriptions {
		subscription.active = false
		subscription.handler = nil
	}
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
	transporter.logger.Debug("[", transporter.instanceID, "] memory.Subscribe() listen for command: ", command, " nodeID: ", nodeID, " topic: ", topic)

	subscription := Subscription{util.RandomString(5), handler, true}
	transporter.subscriptions = append(transporter.subscriptions, subscription)

	transporter.mutex.Lock()
	_, exists := transporter.memory.handlers[topic]
	if exists {
		transporter.memory.handlers[topic] = append(transporter.memory.handlers[topic], subscription)
	} else {
		transporter.memory.handlers[topic] = []Subscription{subscription}
	}
	transporter.mutex.Unlock()
}

func (transporter *MemoryTransporter) Publish(command, nodeID string, message moleculer.Payload) {
	topic := topicName(transporter, command, nodeID)
	transporter.logger.Debug("[Transporter-", transporter.instanceID, "] memory.Publish() command: ", command, " nodeID: ", nodeID, " message: \n", message, "\n - end")

	transporter.mutex.Lock()
	subscriptions, exists := transporter.memory.handlers[topic]
	transporter.mutex.Unlock()
	if exists {
		for _, subscription := range subscriptions {
			if subscription.active {
				go subscription.handler(message)
			}
		}
	}
}
