package transit

import (
	"time"

	. "github.com/moleculer-go/moleculer/common"
	. "github.com/moleculer-go/moleculer/serializer"
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
	self            *TransitImpl
	transport       *Transport
	serializer      *Serializer
	localNode       *Node
	isReady         bool
	pendingRequests map[string]pendingRequest
}

func CreateTransit(serializer *Serializer, localNode *Node) *Transit {
	transitImpl := TransitImpl{
		serializer: serializer,
		isReady:    false,
		localNode:  localNode,
	}
	transitImpl.self = &transitImpl
	var transit Transit = transitImpl
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

func (transit TransitImpl) IsReady() bool {
	return transit.self.isReady
}

type pendingRequest struct {
	context    *Context
	resultChan chan interface{}
}

func (transit TransitImpl) Request(context *Context) chan interface{} {
	resultChan := make(chan interface{})
	self := (*transit.self)
	message := (*self.serializer).ContextToMessage(context)

	nodeID := (*(*context).GetNode()).GetID()

	(*self.transport).Publish("REQ", nodeID, message)

	self.pendingRequests[(*context).GetID()] = pendingRequest{
		context,
		resultChan,
	}

	return resultChan
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

	message := (*transit.serializer).MapToMessage(&payload)
	nodeID := (*(*context).GetNode()).GetID()

	(*transit.transport).Publish("RES", nodeID, message)
}

// requestHandler : handles when a request arrives on this node.
// 1: create a context from the message, the context contains the target action
// 2: invoke the action
// 3: send a response
func (transit *TransitImpl) requestHandler() TransportHandler {
	return func(message TransitMessage) {
		context := (*transit.serializer).MessageToContext(&message)
		result := <-context.InvokeAction()
		transit.sendResponse(&context, result)
	}
}

func (transit *TransitImpl) subscribe() {
	(*transit.transport).Subscribe("RES", (*transit.localNode).GetID(), transit.reponseHandler())
}

func (transit TransitImpl) Connect() chan bool {
	endChan := make(chan bool)
	if transit.IsReady() {
		endChan <- true
		return endChan
	}
	transport := CreateTransport(transit.serializer)
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
