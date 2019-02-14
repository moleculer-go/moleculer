package pubsub

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/moleculer-go/moleculer/version"

	"github.com/moleculer-go/moleculer/payload"

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

func (pubsub *PubSub) onServiceAdded(values ...interface{}) {
	if pubsub.isConnected {
		pubsub.broadcastNodeInfo("")
	}
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
		neighboursTimeout:    broker.Config.NeighboursCheckTimeout,
		knownNeighbours:      knownNeighbours,
		neighboursMutex:      &sync.Mutex{},
		pendingRequestsMutex: &sync.Mutex{},
	}

	broker.Bus().On("$node.disconnected", transitImpl.onNodeDisconnected)
	broker.Bus().On("$node.connected", transitImpl.onNodeConnected)
	broker.Bus().On("$broker.started", transitImpl.onBrokerStarted)
	broker.Bus().On("$registry.service.added", transitImpl.onServiceAdded)

	return &transitImpl
}

func (pubsub *PubSub) onNodeDisconnected(values ...interface{}) {
	var nodeID string = values[0].(string)
	pubsub.logger.Debug("onNodeDisconnected() nodeID: ", nodeID)
	pending, exists := pubsub.pendingRequests[nodeID]
	pubsub.neighboursMutex.Lock()
	if exists {
		(*pending.resultChan) <- payload.Create(fmt.Errorf("Node %s disconnected. Request being canceled.", nodeID))
		delete(pubsub.pendingRequests, nodeID)
	}
	delete(pubsub.knownNeighbours, nodeID)
	pubsub.neighboursMutex.Unlock()
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
func (pubsub *PubSub) createTransport() transit.Transport {
	var transport transit.Transport
	if pubsub.broker.Config.TransporterFactory != nil {
		pubsub.logger.Info("createTransport() using a custom factory ...")
		transport = pubsub.broker.Config.TransporterFactory().(transit.Transport)
	} else if pubsub.broker.Config.Transporter == "STAN" {
		pubsub.logger.Info("createTransport() creating NATS Streaming Transporter")
		transport = pubsub.createStanTransporter()
	} else {
		pubsub.logger.Info("createTransport() creating default Memory Transporter")
		transport = pubsub.createMemoryTransporter()
	}
	transport.SetPrefix("MOL")
	return transport
}

func (pubsub *PubSub) createMemoryTransporter() transit.Transport {
	pubsub.logger.Debug("createMemoryTransporter() ... ")
	logger := pubsub.logger.WithField("transport", "memory")
	mem := memory.Create(logger, &memory.SharedMemory{})
	return &mem
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
		func(message moleculer.Payload) bool {
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
	resultChan *chan moleculer.Payload
}

func (pubsub *PubSub) checkMaxQueueSize() {
	//TODO: check transit.js line 524
}

// waitForNeighbours this function will wait for neighbour nodes or timeout if the expected number is not received after a time out.
func (pubsub *PubSub) waitForNeighbours() bool {
	if pubsub.broker.Config.DontWaitForNeighbours {
		return true
	}
	start := time.Now()
	for {
		expected := pubsub.expectedNeighbours()
		neighbours := pubsub.neighbours()
		if expected <= neighbours && (expected > 0 || neighbours > 0) {
			pubsub.logger.Info("waitForNeighbours() - received info from all expected neighbours :) -> expected: ", expected)
			return true
		}
		if time.Since(start) > pubsub.neighboursTimeout {
			pubsub.logger.Warn("waitForNeighbours() - Time out ! did not receive info from all expected neighbours: ", expected, "  INFOs received: ", neighbours)
			return false
		}
		if !pubsub.isConnected {
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
		"ver":    version.MoleculerProtocol(),
	}
	message, err := pubsub.serializer.MapToPayload(&payload)
	if err == nil {
		pubsub.transport.Publish("HEARTBEAT", "", message)
	}
}

func (pubsub *PubSub) DiscoverNode(nodeID string) {
	payload := map[string]interface{}{
		"sender": pubsub.broker.LocalNode().GetID(),
		"ver":    version.MoleculerProtocol(),
	}
	message, err := pubsub.serializer.MapToPayload(&payload)
	if err == nil {
		pubsub.transport.Publish("DISCOVER", nodeID, message)
	}
}

// Emit emit an event to all services that listens to this event.
func (pubsub *PubSub) Emit(context moleculer.BrokerContext) {
	targetNodeID := context.TargetNodeID()
	payload := context.AsMap()
	payload["sender"] = pubsub.broker.LocalNode().GetID()
	payload["ver"] = version.MoleculerProtocol()

	pubsub.logger.Trace("Emit() targetNodeID: ", targetNodeID, " payload: ", payload)

	message, err := pubsub.serializer.MapToPayload(&payload)
	if err != nil {
		pubsub.logger.Error("Emit() Error serializing the payload: ", payload, " error: ", err)
		panic(fmt.Errorf("Error trying to serialize the payload. Likely issues with the action params. Error: %s", err))
	}
	pubsub.transport.Publish("EVENT", targetNodeID, message)
}

func (pubsub *PubSub) Request(context moleculer.BrokerContext) chan moleculer.Payload {
	pubsub.checkMaxQueueSize()

	resultChan := make(chan moleculer.Payload)

	targetNodeID := context.TargetNodeID()
	payload := context.AsMap()
	payload["sender"] = pubsub.broker.LocalNode().GetID()
	payload["ver"] = version.MoleculerProtocol()

	pubsub.logger.Trace("Request() targetNodeID: ", targetNodeID, " payload: ", payload)

	message, err := pubsub.serializer.MapToPayload(&payload)
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

// validateVersion check that version of the message is correct.
func (pubsub *PubSub) validate(handler func(message moleculer.Payload)) transit.TransportHandler {
	return func(msg moleculer.Payload) {
		valid := pubsub.validateVersion(msg) && pubsub.sameHost(msg)
		if valid {
			handler(msg)
		}
	}
}

func (pubsub *PubSub) sameHost(msg moleculer.Payload) bool {
	sender := msg.Get("sender").String()
	localNodeID := pubsub.broker.LocalNode().GetID()
	return sender != localNodeID
}

// validateVersion check that version of the message is correct.
func (pubsub *PubSub) validateVersion(msg moleculer.Payload) bool {
	msgVersion := msg.Get("ver").String()
	if msgVersion == version.MoleculerProtocol() {
		return true
	} else {
		pubsub.logger.Error("Discarding msg - wronging version: ", msgVersion, " expected: ", version.MoleculerProtocol(), " msg: ", msg)
		return false
	}
}

func (pubsub *PubSub) reponseHandler() transit.TransportHandler {
	return func(message moleculer.Payload) {
		id := message.Get("id").String()
		sender := message.Get("sender").String()
		pubsub.logger.Debug("reponseHandler() - response arrived from nodeID: ", sender, " context id: ", id)

		request := pubsub.pendingRequests[id]
		defer delete(pubsub.pendingRequests, id)
		if request.resultChan == nil {
			pubsub.logger.Debug("reponseHandler() - discarding response -> request.resultChan is nil! ")
			return
		}

		var result moleculer.Payload
		if message.Get("success").Bool() {
			result = message.Get("data")
		} else {
			result = payload.Create(errors.New(message.Get("error").String()))
		}

		pubsub.logger.Trace("reponseHandler() id: ", id, " result: ", result)
		go func() {
			(*request.resultChan) <- result
		}()
	}
}

func (pubsub *PubSub) sendResponse(context moleculer.BrokerContext, response moleculer.Payload) {
	targetNodeID := context.TargetNodeID()

	pubsub.logger.Tracef("sendResponse() reponse type: %T ", response)

	values := make(map[string]interface{})
	values["sender"] = pubsub.broker.LocalNode().GetID()
	values["ver"] = version.MoleculerProtocol()
	values["id"] = context.ID()
	values["meta"] = context.Meta()

	if response.IsError() {
		values["error"] = response.String()
		values["success"] = false
	} else {
		values["success"] = true
		values["data"] = response.Value()
	}

	message, err := pubsub.serializer.MapToPayload(&values)
	if err != nil {
		pubsub.logger.Error("sendResponse() Erro serializing the values: ", values, " error: ", err)
		panic(err)
	}

	pubsub.logger.Trace("sendResponse() targetNodeID: ", targetNodeID, " values: ", values, " message: ", message)

	pubsub.transport.Publish("RES", targetNodeID, message)
}

// requestHandler : handles when a request arrives on this node.
// 1: create a moleculer.Context from the message, the moleculer.Context contains the target action
// 2: invoke the action
// 3: send a response
func (pubsub *PubSub) requestHandler() transit.TransportHandler {
	return func(message moleculer.Payload) {
		values := pubsub.serializer.PayloadToContextMap(message)
		context := context.ActionContext(pubsub.broker, values)
		result := <-pubsub.broker.ActionDelegate(context)
		pubsub.sendResponse(context, result)
	}
}

//eventHandler handles when a event msg is sent to this broker
func (pubsub *PubSub) eventHandler() transit.TransportHandler {
	return func(message moleculer.Payload) {
		values := pubsub.serializer.PayloadToContextMap(message)
		context := context.ChildEventContext(pubsub.broker, values)
		pubsub.broker.HandleRemoteEvent(context)
	}
}

// expectedNeighbours calculate the expected number of neighbours
func (pubsub *PubSub) expectedNeighbours() int64 {
	neighbours := pubsub.neighbours()
	if neighbours == 0 {
		return 0
	}

	var total int64
	pubsub.neighboursMutex.Lock()
	for _, value := range pubsub.knownNeighbours {
		total = total + value
	}
	pubsub.neighboursMutex.Unlock()
	return total / neighbours
}

// neighbours return the total number of known neighbours.
func (pubsub *PubSub) neighbours() int64 {
	return int64(len(pubsub.knownNeighbours))
}

func (pubsub *PubSub) broadcastNodeInfo(targetNodeID string) {
	payload := pubsub.broker.LocalNode().ExportAsMap()
	payload["sender"] = payload["id"]
	payload["neighbours"] = pubsub.neighbours()
	payload["ver"] = version.MoleculerProtocol()

	message, _ := pubsub.serializer.MapToPayload(&payload)
	pubsub.transport.Publish("INFO", targetNodeID, message)
}

func (pubsub *PubSub) discoverHandler() transit.TransportHandler {
	return func(message moleculer.Payload) {
		sender := message.Get("sender").String()
		pubsub.broadcastNodeInfo(sender)
	}
}

func (pubsub *PubSub) emitRegistryEvent(command string) transit.TransportHandler {
	return func(message moleculer.Payload) {
		pubsub.logger.Trace("emitRegistryEvent() command: ", command, " message: ", message)
		pubsub.broker.Bus().EmitAsync("$registry.transit.message", []interface{}{command, message})
	}
}

func (pubsub *PubSub) SendPing() {
	ping := make(map[string]interface{})
	sender := pubsub.broker.LocalNode().GetID()
	ping["sender"] = sender
	ping["ver"] = version.MoleculerProtocol()
	ping["time"] = time.Now().Unix()
	pingMessage, _ := pubsub.serializer.MapToPayload(&ping)
	pubsub.transport.Publish("PING", sender, pingMessage)

}

func (pubsub *PubSub) pingHandler() transit.TransportHandler {
	return func(message moleculer.Payload) {
		pong := make(map[string]interface{})
		sender := message.Get("sender").String()
		pong["sender"] = sender
		pong["ver"] = version.MoleculerProtocol()
		pong["time"] = message.Get("time").Int()
		pong["arrived"] = time.Now().Unix()

		pongMessage, _ := pubsub.serializer.MapToPayload(&pong)
		pubsub.transport.Publish("PONG", sender, pongMessage)
	}
}

func (pubsub *PubSub) pongHandler() transit.TransportHandler {
	return func(message moleculer.Payload) {
		now := time.Now().Unix()
		elapsed := now - message.Get("time").Int64()
		arrived := message.Get("arrived").Int64()
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
	pubsub.transport.Subscribe("RES", nodeID, pubsub.validate(pubsub.reponseHandler()))

	pubsub.transport.Subscribe("REQ", nodeID, pubsub.validate(pubsub.requestHandler()))
	//pubsub.transport.Subscribe("REQB", nodeID, pubsub.requestHandler())
	pubsub.transport.Subscribe("EVENT", nodeID, pubsub.validate(pubsub.eventHandler()))

	pubsub.transport.Subscribe("HEARTBEAT", "", pubsub.validate(pubsub.emitRegistryEvent("HEARTBEAT")))
	pubsub.transport.Subscribe("DISCONNECT", "", pubsub.validate(pubsub.emitRegistryEvent("DISCONNECT")))
	pubsub.transport.Subscribe("INFO", "", pubsub.validate(pubsub.emitRegistryEvent("INFO")))
	pubsub.transport.Subscribe("INFO", nodeID, pubsub.validate(pubsub.emitRegistryEvent("INFO")))
	pubsub.transport.Subscribe("DISCOVER", nodeID, pubsub.validate(pubsub.discoverHandler()))
	pubsub.transport.Subscribe("DISCOVER", "", pubsub.validate(pubsub.discoverHandler()))
	pubsub.transport.Subscribe("PING", nodeID, pubsub.validate(pubsub.pingHandler()))
	pubsub.transport.Subscribe("PONG", nodeID, pubsub.validate(pubsub.pongHandler()))

}

// sendDisconnect broadcast a DISCONNECT pkt to all nodes informing this one is stoping.
func (pubsub *PubSub) sendDisconnect() {
	payload := make(map[string]interface{})
	payload["sender"] = pubsub.broker.LocalNode().GetID()
	payload["ver"] = version.MoleculerProtocol()
	msg, _ := pubsub.serializer.MapToPayload(&payload)
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
	pubsub.isConnected = false
	return pubsub.transport.Disconnect()
}

// Connect : connect the transit with the transporter, subscribe to all events and start publishing its node info
func (pubsub *PubSub) Connect() chan bool {
	endChan := make(chan bool)
	if pubsub.isConnected {
		endChan <- true
		return endChan
	}
	pubsub.logger.Info("PubSub - Connecting transport...")
	pubsub.transport = pubsub.createTransport()
	go func() {
		pubsub.isConnected = <-pubsub.transport.Connect()
		pubsub.logger.Debug("PubSub - Transport Connected!")
		if pubsub.isConnected {
			pubsub.subscribe()
		}
		endChan <- pubsub.isConnected
	}()
	return endChan
}

func (pubsub *PubSub) Ready() {

}
