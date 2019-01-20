package endpoint

import (
	"fmt"

	. "github.com/moleculer-go/moleculer/common"
	. "github.com/moleculer-go/moleculer/endpoint"
	. "github.com/moleculer-go/moleculer/strategy"
)

type EndpointList struct {
	endpoints []*Endpoint
	internal  bool
}

func (endpointList *EndpointList) Size() int {
	return len(endpointList.endpoints)
}

func (endpointList *EndpointList) SizeLocal() int {
	localEndpoints := filter(endpointList.endpoints, func(endpoint *Endpoint) bool {
		return endpoint.IsLocal
	})
	return len(localEndpoints)
}

// Add : Add an endpoint
func (endpointList *EndpointList) Add(broker *BrokerInfo, node *NodeInfo, service *ServiceInfo, actionEvent *ActionEventInfo) *Endpoint {
	found := find(endpointList.endpoints, func(endpoint *Endpoint) bool {
		return node.ID == endpoint.Node.ID &&
			service.Name == endpoint.Service.Name &&
			actionEvent.Name == endpoint.ActionEvent.Name
	})
	if found != nil {
		found.Update(broker, node, service, actionEvent)
		return found
	} else {
		name := fmt.Sprintf("%s:%s", node.ID, actionEvent.Name)
		local := node.ID == broker.NodeID
		isAvailable := true
		newEndpoint := &Endpoint{name, *broker, *node, *service, *actionEvent, local, isAvailable}

		broker.Logger.Infof(
			"Endpoint created - Name: %s - Broker.NodeID: %s - Node.ID: %s - Service.Name: %s - ActionEvent.Name: %s - local: %t",
			name, broker.NodeID, node.ID, service.Name, actionEvent.Name, local)

		endpointList.endpoints = append(endpointList.endpoints, newEndpoint)
		return newEndpoint
	}
}

// Next : Get next endpoint
func (endpointList *EndpointList) Next(strategy Strategy) *Endpoint {
	if len(endpointList.endpoints) == 0 {
		return nil
	}

	localEndpoints := filter(endpointList.endpoints, func(endpoint *Endpoint) bool {
		return endpoint.IsLocal
	})

	// If internal (service), return the local always
	// TODO: There is one more condition in the moleculer JS code -> this.registry.opts.preferLocal whih I'll only check if required when doing the registry!
	if endpointList.internal && len(localEndpoints) > 0 {
		return selectNextEndpoint(localEndpoints, strategy)
	}

	return selectNextEndpoint(endpointList.endpoints, strategy)
}

type endpointPredicate func(endpoint *Endpoint) bool

func filter(endpoints []*Endpoint, predicate endpointPredicate) []*Endpoint {
	var result []*Endpoint
	for _, endpoint := range endpoints {
		if predicate(endpoint) {
			result = append(result, endpoint)
		}
	}
	return result
}

func find(endpoints []*Endpoint, predicate endpointPredicate) *Endpoint {
	result := filter(endpoints, predicate)
	if len(result) > 0 {
		return result[0]
	}
	return nil
}

func selectNextEndpoint(endpoints []*Endpoint, strategy Strategy) *Endpoint {

	availableEndpoints := filter(endpoints, func(endpoint *Endpoint) bool {
		return endpoint.IsAvailable
	})

	if len(availableEndpoints) == 0 {
		return nil
	} else if len(availableEndpoints) == 1 {
		return availableEndpoints[0]
	}

	return strategy.SelectEndpoint(availableEndpoints)
}
