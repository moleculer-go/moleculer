package transit

import (
	"fmt"
	"math"
	"time"

	. "github.com/moleculer-go/moleculer/common"
	. "github.com/moleculer-go/moleculer/util"
)

type TransportHandler func(message TransitMessage)

type Transport interface {
	Subscribe(command, nodeID string, handler TransportHandler)
	MakeBalancedSubscriptions()
	Publish(command, nodeID string, message TransitMessage)
	Connect() chan bool
	Disconnect() chan bool
	Request(message TransitMessage) chan interface{}
}

type TransitImpl struct {
	self                   *TransitImpl
	transport              *Transport
	broker                 *BrokerInfo
	isReady                bool
	pendingRequests        map[string]pendingRequest
	registryMessageHandler RegistryMessageHandlerFunction
}

func CreateTransit(broker *BrokerInfo) *Transit {
	transitImpl := TransitImpl{
		broker:                 broker,
		isReady:                false,
		registryMessageHandler: broker.RegistryMessageHandler,
	}
	transitImpl.self = &transitImpl

	broker.GetLocalBus().On("$node.disconnected", func(values ...interface{}) {
		var node *Node = values[0].(*Node)
		transitImpl.self.nodeDisconnected(node)
	})

	var transit Transit = transitImpl
	return &transit
}

func (transit *TransitImpl) nodeDisconnected(node *Node) {
	nodeID := (*node).GetID()
	pending := transit.pendingRequests[nodeID]
	pending.resultChan <- fmt.Errorf("Node %s disconnected. Request being canceled.", nodeID)
	delete(transit.pendingRequests, nodeID)
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

func (transit TransitImpl) IsReady() bool {
	return transit.self.isReady
}

type pendingRequest struct {
	context    *Context
	resultChan chan interface{}
}

func (transit *TransitImpl) checkMaxQueueSize() {
	//TODO: check transit.js line 524
}

func (transit TransitImpl) DiscoverNodes() {
	transit.DiscoverNode("")
}

func (transit TransitImpl) SendHeartbeat() {
	transit.self.sendHeartbeatImpl()
}

func (transit *TransitImpl) sendHeartbeatImpl() {
	node := (*transit.broker.GetLocalNode()).ExportAsMap()
	payload := map[string]interface{}{
		"sender": node["id"],
		"cpu":    node["cpu"],
		"cpuSeq": node["cpuSeq"],
	}
	message := (*transit.broker.GetSerializer()).MapToMessage(&payload)
	(*transit.transport).Publish("HEARTBEAT", "", message)
}

func (transit TransitImpl) DiscoverNode(nodeID string) {
	transit.self.discoverNodeImpl(nodeID)
}
func (transit TransitImpl) discoverNodeImpl(nodeID string) {
	payload := make(map[string]interface{})
	payload["sender"] = (*transit.broker.GetLocalNode()).GetID()
	message := (*transit.broker.GetSerializer()).MapToMessage(&payload)
	(*transit.transport).Publish("DISCOVER", nodeID, message)
}

func (transit *TransitImpl) requestImpl(context *Context) chan interface{} {

	transit.checkMaxQueueSize()

	resultChan := make(chan interface{})
	message := (*transit.broker.GetSerializer()).ContextToMessage(context)
	nodeID := (*(*context).GetNode()).GetID()

	(*transit.transport).Publish("REQ", nodeID, message)

	transit.pendingRequests[(*context).GetID()] = pendingRequest{
		context,
		resultChan,
	}
	return resultChan
}

func (transit TransitImpl) Request(context *Context) chan interface{} {
	return (*transit.self).requestImpl(context)
}

func (transit *TransitImpl) reponseHandler() TransportHandler {
	return func(message TransitMessage) {
		id := message.Get("id").String()
		request := transit.pendingRequests[id]
		delete(transit.pendingRequests, id)
		request.resultChan <- message.Get("data").Value()
	}
}

func (transit *TransitImpl) sendResponse(context *Context, response interface{}) {
	payload := make(map[string]interface{})
	payload["id"] = (*context).GetID()
	payload["meta"] = (*context).GetMeta()
	payload["success"] = true
	payload["data"] = response

	message := (*transit.broker.GetSerializer()).MapToMessage(&payload)
	nodeID := (*(*context).GetNode()).GetID()

	(*transit.transport).Publish("RES", nodeID, message)
}

// requestHandler : handles when a request arrives on this node.
// 1: create a context from the message, the context contains the target action
// 2: invoke the action
// 3: send a response
func (transit *TransitImpl) requestHandler() TransportHandler {
	return func(message TransitMessage) {
		context := (*transit.broker.GetSerializer()).MessageToContext(&message)
		result := <-context.InvokeAction()
		transit.sendResponse(&context, result)
	}
}

//TODO
func (transit *TransitImpl) eventHandler() TransportHandler {
	return func(message TransitMessage) {
		//context := (*transit.serializer).MessageToContext(&message)
		// result := <-context.InvokeAction()
		// transit.sendResponse(&context, result)
	}
}

func (transit *TransitImpl) sendNodeInfo(targetNodeID string) {
	payload := (*transit.broker.GetLocalNode()).ExportAsMap()
	message := (*transit.broker.GetSerializer()).MapToMessage(&payload)
	(*transit.transport).Publish("INFO", targetNodeID, message)
}

func (transit *TransitImpl) discoverHandler() TransportHandler {
	return func(message TransitMessage) {
		nodeID := message.Get("sender").String()
		transit.sendNodeInfo(nodeID)
	}
}

func (transit *TransitImpl) registryDelegateHandler(command string) TransportHandler {
	return func(message TransitMessage) {
		transit.registryMessageHandler(command, &message)
	}
}

func (transit *TransitImpl) SendPing() {
	ping := make(map[string]interface{})
	sender := (*transit.broker.GetLocalNode()).GetID()
	ping["sender"] = sender
	ping["time"] = time.Now().Unix()
	pingMessage := (*transit.broker.GetSerializer()).MapToMessage(&ping)
	(*transit.transport).Publish("PING", sender, pingMessage)

}

func (transit *TransitImpl) pingHandler() TransportHandler {
	return func(message TransitMessage) {
		pong := make(map[string]interface{})
		sender := message.Get("sender").String()
		pong["sender"] = sender
		pong["time"] = message.Get("time").Int()
		pong["arrived"] = time.Now().Unix()

		pongMessage := (*transit.broker.GetSerializer()).MapToMessage(&pong)
		(*transit.transport).Publish("PONG", sender, pongMessage)
	}
}

func (transit *TransitImpl) pongHandler() TransportHandler {
	return func(message TransitMessage) {
		now := time.Now().Unix()
		elapsed := now - message.Get("time").Int()
		arrived := message.Get("arrived").Int()
		timeDiff := math.Round(
			float64(now) - float64(arrived) - float64(elapsed)/2)

		mapValue := make(map[string]interface{})
		mapValue["nodeID"] = message.Get("sender").String()
		mapValue["elapsedTime"] = elapsed
		mapValue["timeDiff"] = timeDiff

		transit.broker.GetLocalBus().EmitAsync("$node.pong", []interface{}{mapValue})
	}
}

func (transit *TransitImpl) subscribe() {
	nodeID := (*transit.broker.GetLocalNode()).GetID()
	(*transit.transport).Subscribe("RES", nodeID, transit.reponseHandler())
	(*transit.transport).Subscribe("REQ", nodeID, transit.requestHandler())
	(*transit.transport).Subscribe("HEARTBEAT", nodeID, transit.registryDelegateHandler("HEARTBEAT"))
	(*transit.transport).Subscribe("DISCONNECT", nodeID, transit.registryDelegateHandler("DISCONNECT"))
	(*transit.transport).Subscribe("INFO", nodeID, transit.registryDelegateHandler("INFO"))
	(*transit.transport).Subscribe("EVENT", nodeID, transit.eventHandler())
	(*transit.transport).Subscribe("DISCOVER", nodeID, transit.discoverHandler())
	(*transit.transport).Subscribe("DISCOVER", "", transit.discoverHandler())
	(*transit.transport).Subscribe("PING", nodeID, transit.pingHandler())
	(*transit.transport).Subscribe("PONG", nodeID, transit.pongHandler())

}

// Connect : connect the transit with the transporter, subscribe to all events and start publishing its node info
func (transit TransitImpl) Connect() chan bool {
	endChan := make(chan bool)
	if transit.IsReady() {
		endChan <- true
		return endChan
	}
	transport := CreateTransport(transit.broker.GetSerializer())
	transit.self.transport = transport
	go func() {
		connected := <-(*transport).Connect()
		transit.self.subscribe()
		transit.self.isReady = connected
		endChan <- connected
	}()
	return endChan
}

func (transit TransitImpl) Ready() chan bool {
	endChan := make(chan bool)
	go func() {
		for {
			if transit.IsReady() {
				endChan <- true
			}
			time.Sleep(500 * time.Millisecond)
		}
	}()
	return endChan
}
