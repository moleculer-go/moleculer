package pubsub

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/moleculer-go/moleculer/context"
	"github.com/moleculer-go/moleculer/transit"
	"github.com/moleculer-go/moleculer/transit/memory"
	"github.com/moleculer-go/moleculer/transit/nats"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/serializer"
	log "github.com/sirupsen/logrus"
)

// PubSub is a transit implementation.
type PubSub struct {
	logger               *log.Entry
	transport            transit.Transport
	broker               moleculer.BrokerDelegates
	isConnected          bool
	pendingRequests      map[string]pendingRequest
	pendingRequestsMutex *sync.Mutex
	serializer           serializer.Serializer

	knownNeighbours   map[string]int64
	neighboursTimeout time.Duration
	neighboursMutex   *sync.Mutex
}

func (pubsub *PubSub) onBrokerStarted(values ...interface{}) {
	if pubsub.isConnected {
		pubsub.broadcastNodeInfo("")
	}
}

func Create(broker moleculer.BrokerDelegates) transit.Transit {
	pendingRequests := make(map[string]pendingRequest)
	knownNeighbours := make(map[string]int64)
	transitImpl := PubSub{
		broker:               broker,
		isConnected:          false,
		pendingRequests:      pendingRequests,
		logger:               broker.Logger("Transit", ""),
		serializer:           serializer.FromConfig(broker),
		neighboursTimeout:    1 * time.Second,
		knownNeighbours:      knownNeighbours,
		neighboursMutex:      &sync.Mutex{},
		pendingRequestsMutex: &sync.Mutex{},
	}

	broker.Bus().On("$node.disconnected", transitImpl.onNodeDisconnected)
	broker.Bus().On("$node.connected", transitImpl.onNodeConnected)
	broker.Bus().On("$broker.started", transitImpl.onBrokerStarted)

	return &transitImpl
}

func (pubsub *PubSub) onNodeDisconnected(values ...interface{}) {
	var nodeID string = values[0].(string)
	pubsub.logger.Debug("onNodeDisconnected() nodeID: ", nodeID)
	pending, exists := pubsub.pendingRequests[nodeID]
	if exists {
		(*pending.resultChan) <- fmt.Errorf("Node %s disconnected. Request being canceled.", nodeID)
		delete(pubsub.pendingRequests, nodeID)
	}
	delete(pubsub.knownNeighbours, nodeID)
}

func (pubsub *PubSub) onNodeConnected(values ...interface{}) {
	nodeID := values[0].(string)
	neighbours := values[1].(int64)
	pubsub.logger.Debug("onNodeConnected() nodeID: ", nodeID, " neighbours: ", neighbours)
	pubsub.neighboursMutex.Lock()
	pubsub.knownNeighbours[nodeID] = neighbours
	pubsub.neighboursMutex.Unlock()
}

// CreateTransport : based on config it will load the transporter
// for now is hard coded for NATS Streaming localhost
func (pubsub *PubSub) createTransport(broker moleculer.BrokerDelegates) transit.Transport {
	if broker.Config.Transporter == "STAN" {
		pubsub.logger.Debug("createTransport() creating NATS Streaming Transporter")
		return pubsub.createStanTransporter()
	} else {
		pubsub.logger.Debug("createTransport() creating Memory Transporter")
		return pubsub.createMemoryTransporter()
	}
}

func (pubsub *PubSub) createMemoryTransporter() transit.Transport {

	pubsub.logger.Debug("createMemoryTransporter() ... ")
	broker := pubsub.broker
	prefix := "MOL"
	logger := log.WithField("transport", "memory")
	localNodeID := broker.LocalNode().GetID()
	transport := memory.CreateTransporter(prefix, logger, func(message transit.Message) bool {
		sender := message.Get("sender").String()
		return sender != localNodeID
	})

	return &transport
}

func (pubsub *PubSub) createStanTransporter() transit.Transport {
	//TODO: move this to config and params
	broker := pubsub.broker
	prefix := "MOL"
	url := "stan://localhost:4222"
	clusterID := "test-cluster"

	localNodeID := broker.LocalNode().GetID()
	logger := broker.Logger("transport", "stan")

	options := nats.StanOptions{
		prefix,
		url,
		clusterID,
		localNodeID,
		logger,
		pubsub.serializer,
		func(message transit.Message) bool {
			sender := message.Get("sender").String()
			return sender != localNodeID
		},
	}

	stanTransporter := nats.CreateStanTransporter(options)
	var transport transit.Transport = &stanTransporter
	return transport
}

type pendingRequest struct {
	context    moleculer.BrokerContext
	resultChan *chan interface{}
}

func (pubsub *PubSub) checkMaxQueueSize() {
	//TODO: check transit.js line 524
}

// waitForNeighbours this function will wait for neighbour nodes or timeout if the expected number is not received after a time out.
func (pubsub *PubSub) waitForNeighbours() bool {
	start := time.Now()
	for {
		expected := pubsub.expectedNeighbours()
		neighbours := pubsub.neighbours()
		if expected <= neighbours && expected > 0 && neighbours > 0 {
			pubsub.logger.Info("waitForNeighbours() - received INDO from all expected neighbours: ", expected)
			return true
		}
		if time.Since(start) > pubsub.neighboursTimeout {
			pubsub.logger.Warn("waitForNeighbours() - Time out ! did not receive info from all expected neighbours: ", expected, "  INFOs received: ", neighbours)
			return false
		}
		time.Sleep(100 * time.Millisecond)
	}
}

//DiscoverNodes will check if there are neighbours and return true if any are found ;).
func (pubsub *PubSub) DiscoverNodes() chan bool {
	result := make(chan bool)
	go func() {
		pubsub.DiscoverNode("")
		result <- pubsub.waitForNeighbours()
	}()
	return result
}

func (pubsub *PubSub) SendHeartbeat() {
	node := pubsub.broker.LocalNode().ExportAsMap()
	payload := map[string]interface{}{
		"sender": node["id"],
		"cpu":    node["cpu"],
		"cpuSeq": node["cpuSeq"],
	}
	message, err := pubsub.serializer.MapToMessage(&payload)
	if err == nil {
		pubsub.transport.Publish("HEARTBEAT", "", message)
	}
}

func (pubsub *PubSub) DiscoverNode(nodeID string) {
	payload := map[string]interface{}{"sender": pubsub.broker.LocalNode().GetID()}
	message, err := pubsub.serializer.MapToMessage(&payload)
	if err == nil {
		pubsub.transport.Publish("DISCOVER", nodeID, message)
	}
}

func (pubsub *PubSub) Request(context moleculer.BrokerContext) chan interface{} {
	pubsub.checkMaxQueueSize()

	resultChan := make(chan interface{})

	targetNodeID := context.TargetNodeID()
	payload := context.AsMap()
	payload["sender"] = pubsub.broker.LocalNode().GetID()

	pubsub.logger.Trace("Request() targetNodeID: ", targetNodeID, " payload: ", payload)

	message, err := pubsub.serializer.MapToMessage(&payload)
	if err != nil {
		pubsub.logger.Error("Request() Error serializing the payload: ", payload, " error: ", err)
		panic(fmt.Errorf("Error trying to serialize the payload. Likely issues with the action params. Error: %s", err))
	}
	pubsub.pendingRequestsMutex.Lock()
	pubsub.pendingRequests[context.ID()] = pendingRequest{
		context,
		&resultChan,
	}
	pubsub.pendingRequestsMutex.Unlock()

	pubsub.transport.Publish("REQ", targetNodeID, message)
	return resultChan
}

func (pubsub *PubSub) reponseHandler() transit.TransportHandler {
	return func(message transit.Message) {
		id := message.Get("id").String()
		sender := message.Get("sender").String()
		pubsub.logger.Debug("reponseHandler() - response arrived from nodeID: ", sender, " context id: ", id)

		request := pubsub.pendingRequests[id]
		delete(pubsub.pendingRequests, id)
		result := message.Get("data").Value()

		pubsub.logger.Trace("reponseHandler() id: ", id, " result: ", result)
		go func() {
			(*request.resultChan) <- result
		}()
	}
}

func (pubsub *PubSub) sendResponse(context moleculer.BrokerContext, response interface{}) {
	targetNodeID := context.TargetNodeID()

	payload := make(map[string]interface{})
	payload["sender"] = pubsub.broker.LocalNode().GetID()
	payload["id"] = context.ID()
	payload["meta"] = context.Meta()
	payload["success"] = true
	payload["data"] = response

	message, err := pubsub.serializer.MapToMessage(&payload)
	if err != nil {
		pubsub.logger.Error("sendResponse() Erro serializing the payload: ", payload, " error: ", err)
		panic(err)
	}

	pubsub.logger.Debug("sendResponse() targetNodeID: ", targetNodeID, " payload: ", payload)

	pubsub.transport.Publish("RES", targetNodeID, message)
}

// requestHandler : handles when a request arrives on this node.
// 1: create a moleculer.Context from the message, the moleculer.Context contains the target action
// 2: invoke the action
// 3: send a response
func (pubsub *PubSub) requestHandler() transit.TransportHandler {
	return func(message transit.Message) {
		values := pubsub.serializer.MessageToContextMap(message)
		context := context.RemoteActionContext(pubsub.broker, values)
		result := <-pubsub.broker.ActionDelegate(context)
		pubsub.sendResponse(context, result)
	}
}

// expectedNeighbours calculate the expected number of neighbours
func (pubsub *PubSub) expectedNeighbours() int64 {
	neighbours := pubsub.neighbours()
	if neighbours == 0 {
		return 0
	}

	var total int64
	for _, value := range pubsub.knownNeighbours {
		total = total + value
	}
	return total / neighbours
}

// neighbours return the total number of known neighbours.
func (pubsub *PubSub) neighbours() int64 {
	return int64(len(pubsub.knownNeighbours))
}

//TODO
func (pubsub *PubSub) eventHandler() transit.TransportHandler {
	return func(message transit.Message) {
		//moleculer.Context := pubsub.serializer.MessageTomoleculer.Context(&message)
		// result := <-moleculer.Context.InvokeAction()
		// transit.sendResponse(&moleculer.Context, result)
	}
}

func (pubsub *PubSub) broadcastNodeInfo(targetNodeID string) {
	payload := pubsub.broker.LocalNode().ExportAsMap()
	payload["sender"] = payload["id"]
	payload["neighbours"] = pubsub.neighbours()

	message, _ := pubsub.serializer.MapToMessage(&payload)
	pubsub.transport.Publish("INFO", targetNodeID, message)
}

func (pubsub *PubSub) discoverHandler() transit.TransportHandler {
	return func(message transit.Message) {
		sender := message.Get("sender").String()
		pubsub.broadcastNodeInfo(sender)
	}
}

func (pubsub *PubSub) emitRegistryEvent(command string) transit.TransportHandler {
	return func(message transit.Message) {
		pubsub.logger.Trace("emitRegistryEvent() command: ", command, " message: ", message)
		pubsub.broker.Bus().EmitAsync("$registry.transit.message", []interface{}{command, message})
	}
}

func (pubsub *PubSub) SendPing() {
	ping := make(map[string]interface{})
	sender := pubsub.broker.LocalNode().GetID()
	ping["sender"] = sender
	ping["time"] = time.Now().Unix()
	pingMessage, _ := pubsub.serializer.MapToMessage(&ping)
	pubsub.transport.Publish("PING", sender, pingMessage)

}

func (pubsub *PubSub) pingHandler() transit.TransportHandler {
	return func(message transit.Message) {
		pong := make(map[string]interface{})
		sender := message.Get("sender").String()
		pong["sender"] = sender
		pong["time"] = message.Get("time").Int()
		pong["arrived"] = time.Now().Unix()

		pongMessage, _ := pubsub.serializer.MapToMessage(&pong)
		pubsub.transport.Publish("PONG", sender, pongMessage)
	}
}

func (pubsub *PubSub) pongHandler() transit.TransportHandler {
	return func(message transit.Message) {
		now := time.Now().Unix()
		elapsed := now - message.Get("time").Int()
		arrived := message.Get("arrived").Int()
		timeDiff := math.Round(
			float64(now) - float64(arrived) - float64(elapsed)/2)

		mapValue := make(map[string]interface{})
		mapValue["nodeID"] = message.Get("sender").String()
		mapValue["elapsedTime"] = elapsed
		mapValue["timeDiff"] = timeDiff

		pubsub.broker.Bus().EmitAsync("$node.pong", []interface{}{mapValue})
	}
}

func (pubsub *PubSub) subscribe() {
	nodeID := pubsub.broker.LocalNode().GetID()
	pubsub.transport.Subscribe("RES", nodeID, pubsub.reponseHandler())
	pubsub.transport.Subscribe("REQ", nodeID, pubsub.requestHandler())

	pubsub.transport.Subscribe("HEARTBEAT", "", pubsub.emitRegistryEvent("HEARTBEAT"))
	pubsub.transport.Subscribe("DISCONNECT", "", pubsub.emitRegistryEvent("DISCONNECT"))
	pubsub.transport.Subscribe("INFO", "", pubsub.emitRegistryEvent("INFO"))
	pubsub.transport.Subscribe("INFO", nodeID, pubsub.emitRegistryEvent("INFO"))
	pubsub.transport.Subscribe("EVENT", nodeID, pubsub.eventHandler())
	pubsub.transport.Subscribe("DISCOVER", nodeID, pubsub.discoverHandler())
	pubsub.transport.Subscribe("DISCOVER", "", pubsub.discoverHandler())
	pubsub.transport.Subscribe("PING", nodeID, pubsub.pingHandler())
	pubsub.transport.Subscribe("PONG", nodeID, pubsub.pongHandler())

}

// sendDisconnect broadcast a DISCONNECT pkt to all nodes informing this one is stoping.
func (pubsub *PubSub) sendDisconnect() {
	payload := make(map[string]interface{})
	payload["sender"] = pubsub.broker.LocalNode().GetID()
	msg, _ := pubsub.serializer.MapToMessage(&payload)
	pubsub.transport.Publish("DISCONNECT", "", msg)
}

// Disconnect : disconnect the transit's  transporter.
func (pubsub *PubSub) Disconnect() chan bool {
	endChan := make(chan bool)
	if !pubsub.isConnected {
		endChan <- true
		return endChan
	}
	pubsub.logger.Info("PubSub - Disconnecting transport...")
	pubsub.sendDisconnect()
	endChan = pubsub.transport.Disconnect()
	pubsub.isConnected = false
	return endChan
}

// Connect : connect the transit with the transporter, subscribe to all events and start publishing its node info
func (pubsub *PubSub) Connect() chan bool {
	endChan := make(chan bool)
	if pubsub.isConnected {
		endChan <- true
		return endChan
	}
	pubsub.logger.Info("PubSub - Connecting transport...")
	pubsub.transport = pubsub.createTransport(pubsub.broker)
	go func() {
		pubsub.isConnected = <-pubsub.transport.Connect()
		pubsub.logger.Debug("Transport Connected!")
		if pubsub.isConnected {
			pubsub.subscribe()
		}
		endChan <- pubsub.isConnected
	}()
	return endChan
}

func (pubsub *PubSub) Ready() {

}
