package registry

import (
	. "github.com/moleculer-go/moleculer/common"
	. "github.com/moleculer-go/moleculer/endpoint"
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
	registry.actions = CreateActionCatalog()

	registry.logger.Infof("Service Registry created for broker: %s", broker.NodeID)

	broker.GetLocalBus().On("$broker.started", func(args ...interface{}) {
		registry.logger.Debug("Registry -> $broker.started event")
		if registry.nodes.localNode != nil {
			registry.regenerateLocalRawInfo(true)
		}
	})

	return registry
}

// func (registry *ServiceRegistry) registerEvent(serviceEvent *ServiceEvent) {

// }

// func (broker *ServiceBroker) setupActionMiddlewares(service *Service) {
// 	for _, action := range service.GetActions() {
// 		action.ReplaceHandler(broker.middlewares.WrapHandler(
// 			"localAction", action.GetHandler(), action))
// 	}
// }

// AddLocalService : add a local service to the registry
// it will create endpoints for all service actions.
func (registry *ServiceRegistry) AddLocalService(service *Service) {
	if registry.services.Has(service.GetName(), service.GetVersion(), registry.broker.NodeID) {
		return
	}

	registry.services.Add(registry.nodes.localNode, service)

	for _, action := range service.GetActions() {
		registry.actions.Add(registry.nodes.localNode, action, true)
	}

	// for _, event := range service.GetEvents() {
	// 	registry.registerEvent(&event)
	// }

	//WHy we need it there?
	registry.nodes.localNode.AddService(service)

	registry.regenerateLocalRawInfo(registry.broker.IsStarted())

	registry.logger.Infof("%s service is registered.", service.GetName())

	registry.broker.GetLocalBus().EmitAsync(
		"$registry.service.added",
		[]interface{}{service.Summary()})
}

func (registry *ServiceRegistry) NextActionEndpoint(actionName string, strategy *Strategy, params interface{}, opts ...OptionsFunc) *Endpoint {
	nodeID := GetStringOption("nodeID", opts)
	if nodeID != "" {
		return registry.actions.NextEndpointFromNode(actionName, strategy, nodeID, WrapOptions(opts))
	}
	return registry.actions.NextEndpoint(actionName, strategy, WrapOptions(opts))
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

// func (registry *ServiceRegistry) GetEndpointByNodeId(actionName string, nodeID string) *Endpoint {
// 	endpoint := Endpoint{}
// 	return &endpoint
// }

// func (registry *ServiceRegistry) GetEndpointList(actionName string) *EndpointList {
// 	return &EndpointList{}
// }
