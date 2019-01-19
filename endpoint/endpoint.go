package endpoint

import (
	"context"

	log "github.com/sirupsen/logrus"
)

type BrokerInfo struct {
	NodeID string
	Logger *log.Entry
}

type NodeInfo struct {
	ID string
}

type ServiceInfo struct {
	Name string
}

type ActionEventInfo struct {
	Name string
}

type Endpoint struct {
	Name        string
	Broker      BrokerInfo
	Node        NodeInfo
	Service     ServiceInfo
	ActionEvent ActionEventInfo
	IsLocal     bool
	IsAvailable bool
}

// ActionHandler : Invoke the action handler.
func (endpoint *Endpoint) ActionHandler(context *context.Context) {

}

// Update :
func (endpoint *Endpoint) Update(broker *BrokerInfo, node *NodeInfo, service *ServiceInfo, actionEvent *ActionEventInfo) {
	endpoint.Broker = *broker
	endpoint.Node = *node
	endpoint.Service = *service
	endpoint.ActionEvent = *actionEvent
	endpoint.IsLocal = node.ID == broker.NodeID

	broker.Logger.Debugf(
		"Endpoint Update called - ",
	)
	broker.Logger.Infof(
		"Endpoint Update called - Name: %s - Broker.NodeID: %s - Node.ID: %s - Service.Name: %s - ActionEvent.Name: %s - IsLocal: %t",
		endpoint.Name, broker.NodeID, node.ID, service.Name, actionEvent.Name, endpoint.IsLocal)

}
