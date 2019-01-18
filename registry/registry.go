package registry

import (
	. "github.com/moleculer-go/moleculer/endpoint"
	. "github.com/moleculer-go/moleculer/endpointList"
)

type ServiceRegistry struct {
}

func (registry *ServiceRegistry) GetEndpointByNodeId(actionName string, nodeID string) *Endpoint {
	endpoint := Endpoint{}
	return &endpoint
}

func (registry *ServiceRegistry) GetEndpointList(actionName string) *EndpointList {
	return &EndpointList{}
}
