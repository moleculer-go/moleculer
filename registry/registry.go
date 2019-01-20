package registry

import (
	. "github.com/moleculer-go/moleculer/common"
	. "github.com/moleculer-go/moleculer/endpoint"
	. "github.com/moleculer-go/moleculer/endpointList"
	. "github.com/moleculer-go/moleculer/service"
	log "github.com/sirupsen/logrus"
)

type ServiceRegistry struct {
	logger *log.Entry

	nodes    *NodeCatalog
	services *ServiceCatalog
	actions  *ActionCatalog
	events   *EventCatalog
	broker   *BrokerInfo
}

func CreateRegistry(broker *BrokerInfo) *ServiceRegistry {
	registry := &ServiceRegistry{}
	registry.logger = broker.GetLogger("registry")
	registry.broker = broker

	registry.logger.Infof("Service Registry created for broker: %s", broker.NodeID)

	broker.GetLocalBus().On("$broker.started", func(args ...interface{}) {
		registry.logger.Debug("Registry -> $broker.started event")
		if registry.nodes.localNode != nil {
			registry.regenerateLocalRawInfo(true)
		}
	})

	return registry
}

func (registry *ServiceRegistry) registerAction(serviceAction *ServiceAction) {

}

func (registry *ServiceRegistry) registerEvent(serviceEvent *ServiceEvent) {

}

func (registry *ServiceRegistry) registerLocalService(service *Service) {
	if registry.services.Has(service.GetName(), service.GetVersion(), registry.broker.NodeID) {
		return
	}

	registry.services.Add(registry.nodes.localNode, service)

	for _, action := range service.GetActions() {
		registry.registerAction(&action)
	}

	for _, event := range service.GetEvents() {
		registry.registerEvent(&event)
	}

	registry.nodes.localNode.AddService(service)

	registry.regenerateLocalRawInfo(registry.broker.IsStarted())

	registry.logger.Infof("%s service is registered.", service.GetName())

	registry.broker.GetLocalBus().EmitAsync(
		"$registry.service.added",
		[]interface{}{service.Summary()})
}

func (registry *ServiceRegistry) regenerateLocalRawInfo(increaseSequence bool) map[string]interface{} {
	node := registry.nodes.localNode
	if increaseSequence {
		node.sequence++
	}
	services := registry.services.getLocalNodeServices()
	node.rawInfo = map[string]interface{}{
		"ipList":   node.ipList,
		"hostname": node.hostname,
		"client":   node.client,
		"config":   node.config,
		"port":     node.port,
		"seq":      node.sequence,
		"services": services,
	}
	return node.rawInfo
}

func (registry *ServiceRegistry) GetEndpointByNodeId(actionName string, nodeID string) *Endpoint {
	endpoint := Endpoint{}
	return &endpoint
}

func (registry *ServiceRegistry) GetEndpointList(actionName string) *EndpointList {
	return &EndpointList{}
}
