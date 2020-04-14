package pubsub

import (
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
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
	broker               *moleculer.BrokerDelegates
	isConnected          bool
	pendingRequests      map[string]pendingRequest
	pendingRequestsMutex *sync.Mutex
	serializer           serializer.Serializer

	knownNeighbours   map[string]int64
	neighboursTimeout time.Duration
	neighboursMutex   *sync.Mutex
	brokerStarted     bool
}

func (pubsub *PubSub) onServiceAdded(values ...interface{}) {
	if pubsub.isConnected && pubsub.brokerStarted {
		localNodeID := pubsub.broker.LocalNode().GetID()

		// Checking that was added local service
		isLocalServiceAdded := false
		for _, value := range values {
			if value.(map[string]string)["nodeID"] == localNodeID {
				isLocalServiceAdded = true
				break
			}
		}

		if isLocalServiceAdded {
			pubsub.broker.LocalNode().IncreaseSequence()
			pubsub.broadcastNodeInfo("")
		}
	}
}

func (pubsub *PubSub) onBrokerStarted(values ...interface{}) {
	if pubsub.isConnected {
		pubsub.broadcastNodeInfo("")
		pubsub.brokerStarted = true
	}
}

func Create(broker *moleculer.BrokerDelegates) transit.Transit {
	pendingRequests := make(map[string]pendingRequest)
	knownNeighbours := make(map[string]int64)
	transitImpl := PubSub{
		broker:               broker,
		isConnected:          false,
		pendingRequests:      pendingRequests,
		logger:               broker.Logger("Transit", ""),
		serializer:           serializer.New(broker),
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

func (pubsub *PubSub) pendingRequestsByNode(nodeId string) []pendingRequest {
	list := []pendingRequest{}
	for _, p := range pubsub.pendingRequests {
		if p.context.TargetNodeID() == nodeId {
			list = append(list, p)
		}
	}
	return list
}

func (pubsub *PubSub) requestTimedOut(resultChan *chan moleculer.Payload, context moleculer.BrokerContext) func() {
	pError := payload.New(errors.New("request timeout"))
	return func() {
		pubsub.logger.Debug("requestTimedOut() nodeID: ", context.TargetNodeID())
		pubsub.pendingRequestsMutex.Lock()
		defer pubsub.pendingRequestsMutex.Unlock()

		p, exists := pubsub.pendingRequests[context.ID()]
		if exists {
			(*p.resultChan) <- pError
			p.timer.Stop()
			delete(pubsub.pendingRequests, p.context.ID())
		}
	}
}

func (pubsub *PubSub) onNodeDisconnected(values ...interface{}) {
	pubsub.pendingRequestsMutex.Lock()

	var nodeID string = values[0].(string)
	pending := pubsub.pendingRequestsByNode(nodeID)
	pubsub.logger.Debug("onNodeDisconnected() nodeID: ", nodeID, " pending: ", len(pending))
	if len(pending) > 0 {
		pError := payload.New(fmt.Errorf("Node %s disconnected. The request was canceled.", nodeID))
		for _, p := range pending {
			(*p.resultChan) <- pError
			p.timer.Stop()
			delete(pubsub.pendingRequests, p.context.ID())
		}
	}
	pubsub.pendingRequestsMutex.Unlock()

	pubsub.neighboursMutex.Lock()
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

func isNats(v string) bool {
	return strings.Index(v, "nats://") > -1
}

// CreateTransport : based on config it will load the transporter
// for now is hard coded for NATS Streaming localhost
func (pubsub *PubSub) createTransport() transit.Transport {
	var transport transit.Transport
	if pubsub.broker.Config.TransporterFactory != nil {
		pubsub.logger.Info("Transporter: Custom factory")
		transport = pubsub.broker.Config.TransporterFactory().(transit.Transport)
	} else if pubsub.broker.Config.Transporter == "STAN" {
		pubsub.logger.Info("Transporter: NatsStreamingTransporter")
		transport = pubsub.createStanTransporter()
	} else if isNats(pubsub.broker.Config.Transporter) {
		pubsub.logger.Info("Transporter: NatsTransporter")
		transport = pubsub.createNatsTransporter()

	} else {
		pubsub.logger.Info("Transporter: Memory")
		transport = pubsub.createMemoryTransporter()
	}
	transport.SetPrefix(resolveNamespace(pubsub.broker.Config.Namespace))
	transport.SetNodeID(pubsub.broker.LocalNode().GetID())
	transport.SetSerializer(pubsub.serializer)
	return transport
}

func resolveNamespace(namespace string) string {
	if namespace != "" {
		return "MOL-" + namespace
	}
	return "MOL"
}

func (pubsub *PubSub) createMemoryTransporter() transit.Transport {
	pubsub.logger.Debug("createMemoryTransporter() ... ")
	logger := pubsub.logger.WithField("transport", "memory")
	mem := memory.Create(logger, &memory.SharedMemory{})
	return &mem
}

func (pubsub *PubSub) createNatsTransporter() transit.Transport {
	pubsub.logger.Debug("createNatsTransporter()")

	return nats.CreateNatsTransporter(nats.NATSOptions{
		URL:            pubsub.broker.Config.Transporter,
		Name:           pubsub.broker.LocalNode().GetID(),
		Logger:         pubsub.logger.WithField("transport", "nats"),
		Serializer:     pubsub.serializer,
		AllowReconnect: true,
		ReconnectWait:  time.Second * 2,
		MaxReconnect:   -1,
	})
}

func (pubsub *PubSub) createStanTransporter() transit.Transport {
	broker := pubsub.broker
	logger := broker.Logger("transport", "stan")

	url := "stan://" + os.Getenv("STAN_HOST") + ":4222"
	clusterID := "test-cluster"
	localNodeID := broker.LocalNode().GetID()
	clientID := strings.ReplaceAll(localNodeID, ".", "_")

	options := nats.StanOptions{
		url,
		clusterID,
		clientID,
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
	timer      *time.Timer
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
			pubsub.logger.Debug("waitForNeighbours() - received info from all expected neighbours :) -> expected: ", expected)
			return true
		}
		if time.Since(start) > pubsub.neighboursTimeout {
			pubsub.logger.Warn("waitForNeighbours() - Time out ! did not receive info from all expected neighbours: ", expected, "  INFOs received: ", neighbours)
			return false
		}
		if !pubsub.isConnected {
			return false
		}
		time.Sleep(pubsub.broker.Config.WaitForNeighboursInterval)
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
	pubsub.logger.Debug("Request() pending request id: ", context.ID(), " targetNodeId: ", context.TargetNodeID())
	pubsub.pendingRequests[context.ID()] = pendingRequest{
		context,
		&resultChan,

		time.AfterFunc(
			pubsub.broker.Config.RequestTimeout,
			pubsub.requestTimedOut(&resultChan, context)),
	}
	pubsub.pendingRequestsMutex.Unlock()

	pubsub.transport.Publish("REQ", targetNodeID, message)
	return resultChan
}

// validateVersion check that version of the message is correct.
func (pubsub *PubSub) validate(handler func(message moleculer.Payload)) transit.TransportHandler {
	return func(msg moleculer.Payload) {
		valid := pubsub.validateVersion(msg) && !pubsub.sameHost(msg)
		if valid {
			handler(msg)
		} else {
			pubsub.logger.Trace("Discarding invalid msg -> ", msg.Value())
		}
	}
}

func (pubsub *PubSub) sameHost(msg moleculer.Payload) bool {
	sender := msg.Get("sender").String()
	localNodeID := pubsub.broker.LocalNode().GetID()
	return sender == localNodeID
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

// reponseHandler responsible for whem a reponse arrives form a remote node.
func (pubsub *PubSub) reponseHandler() transit.TransportHandler {
	return func(message moleculer.Payload) {
		pubsub.pendingRequestsMutex.Lock()
		defer pubsub.pendingRequestsMutex.Unlock()

		id := message.Get("id").String()
		sender := message.Get("sender").String()
		pubsub.logger.Debug("reponseHandler() - response arrived from nodeID: ", sender, " context id: ", id)

		request, exists := pubsub.pendingRequests[id]

		if !exists {
			pubsub.logger.Debug("reponseHandler() - discarding response -> request does not exist for id: ", id, " - message: ", message.Value())
			return
		}
		if request.resultChan == nil {
			pubsub.logger.Debug("reponseHandler() - discarding response -> request.resultChan is nil! - message: ", message.Value(), " pending context: ", request.context)
			return
		}

		request.timer.Stop()
		defer delete(pubsub.pendingRequests, id)
		var result moleculer.Payload
		if message.Get("success").Bool() {
			result = message.Get("data")
		} else {
			result = pubsub.parseError(message)
		}

		pubsub.logger.Trace("reponseHandler() id: ", id, " result: ", result)
		(*request.resultChan) <- result
	}
}

func (pubsub *PubSub) parseError(message moleculer.Payload) moleculer.Payload {
	if pubsub.isMoleculerJSError(message) {
		return payload.New(pubsub.moleculerJSError(message))
	}
	return payload.New(errors.New(message.Get("error").String()))
}

func (pubsub *PubSub) isMoleculerJSError(message moleculer.Payload) bool {
	return message.Get("error").Get("message").Exists()
}

func (pubsub *PubSub) moleculerJSError(message moleculer.Payload) error {
	msg := message.Get("error").Get("message").String()
	if message.Get("error").Get("stack").Exists() {
		pubsub.logger.Error(message.Get("error").Get("stack").Value())
	}
	return errors.New(msg)
}

func (pubsub *PubSub) sendResponse(context moleculer.BrokerContext, response moleculer.Payload) {
	targetNodeID := context.TargetNodeID()

	if targetNodeID == "" {
		panic(errors.New("sendResponse() targetNodeID is required !"))
	}

	pubsub.logger.Tracef("sendResponse() reponse type: %T ", response)

	values := make(map[string]interface{})
	values["sender"] = pubsub.broker.LocalNode().GetID()
	values["ver"] = version.MoleculerProtocol()
	values["id"] = context.ID()
	values["meta"] = context.Meta()

	if response.IsError() {
		var errMap map[string]string
		actionError, isActionError := response.Value().(ActionError)
		if isActionError {
			errMap = map[string]string{
				"message": actionError.Error(),
				"stack":   actionError.Stack(),
				"name":    "Error",
			}
		} else {
			errMap = map[string]string{
				"message": response.String(),
				"name":    "Error",
			}
		}
		values["success"] = false
		values["error"] = errMap
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

type ActionError interface {
	Error() string
	Stack() string
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
		context := context.EventContext(pubsub.broker, values)
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

// broadcastNodeInfo send the local node info to the target node, if empty to all nodes.
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
		if pubsub.brokerStarted {
			pubsub.broadcastNodeInfo(sender)
		} else {
			pubsub.broker.Bus().Once("$broker.started", func(...interface{}) {
				pubsub.broadcastNodeInfo(sender)
			})
		}
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

// sendDisconnect broadcast a DISCONNECT pkt to all nodes informing this one is stopping.
func (pubsub *PubSub) sendDisconnect() {
	payload := make(map[string]interface{})
	payload["sender"] = pubsub.broker.LocalNode().GetID()
	payload["ver"] = version.MoleculerProtocol()
	msg, _ := pubsub.serializer.MapToPayload(&payload)
	pubsub.transport.Publish("DISCONNECT", "", msg)
}

// Disconnect : disconnect the transit's  transporter.
func (pubsub *PubSub) Disconnect() chan error {
	endChan := make(chan error)
	if !pubsub.isConnected {
		endChan <- nil
		return endChan
	}
	pubsub.logger.Info("PubSub - Disconnecting transport...")
	pubsub.sendDisconnect()
	pubsub.isConnected = false
	return pubsub.transport.Disconnect()
}

// Connect : connect the transit with the transporter, subscribe to all events and start publishing its node info
func (pubsub *PubSub) Connect() chan error {
	endChan := make(chan error)
	if pubsub.isConnected {
		endChan <- nil
		return endChan
	}
	pubsub.logger.Debug("PubSub - Connecting transport...")
	pubsub.transport = pubsub.createTransport()
	go func() {
		err := <-pubsub.transport.Connect()
		if err == nil {
			pubsub.isConnected = true
			pubsub.logger.Debug("PubSub - Transport Connected!")

			pubsub.subscribe()

		} else {
			pubsub.logger.Debug("PubSub - Error connecting transport - error: ", err)
		}
		endChan <- err
	}()
	return endChan
}

func (pubsub *PubSub) Ready() {

}
