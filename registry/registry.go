package registry

import (
	"errors"
	"fmt"
	"time"

	. "github.com/moleculer-go/moleculer/common"
	. "github.com/moleculer-go/moleculer/service"
	log "github.com/sirupsen/logrus"
)

type messageHandlerFunction func(message *TransitMessage)

type ServiceRegistry struct {
	logger *log.Entry

	nodes                 *NodeCatalog
	localNode             *Node
	services              *ServiceCatalog
	actions               *ActionCatalog
	events                *EventCatalog
	broker                *BrokerInfo
	messageHandler        *map[string]messageHandlerFunction
	stoping               bool
	heartbeatFrequency    time.Duration
	heartbeatTimeout      time.Duration
	offlineCheckFrequency time.Duration
}

func CreateRegistry(broker *BrokerInfo) *ServiceRegistry {
	registry := &ServiceRegistry{}
	registry.logger = broker.GetLogger("registry", "Service Registry")
	registry.broker = broker
	registry.actions = CreateActionCatalog(broker.GetTransit)
	registry.services = CreateServiceCatalog()
	registry.nodes = CreateNodesCatalog()
	registry.localNode = broker.GetLocalNode()
	registry.messageHandler = &map[string]messageHandlerFunction{
		"HEARTBEAT":  registry.heartbeatMessageReceived,
		"DISCONNECT": registry.disconnectMessageReceived,
		"INFO":       registry.nodeInfoMessageReceived,
	}

	registry.logger.Infof("Service Registry created for broker: %s", (*broker.GetLocalNode()).GetID())

	broker.GetLocalBus().On("$broker.started", func(args ...interface{}) {
		registry.logger.Debug("Registry -> $broker.started event")
		if registry.localNode != nil {
			//registry.regenerateLocalRawInfo(true)
		}
	})

	registry.stoping = false

	//TODO use config
	registry.heartbeatFrequency = 5 * time.Second
	registry.heartbeatTimeout = 2 * registry.heartbeatFrequency
	registry.offlineCheckFrequency = 30 * time.Second
	return registry
}

func (registry *ServiceRegistry) GetLocalNode() *Node {
	return registry.localNode
}

// Start : start the registry background processes.
func (registry *ServiceRegistry) Start() {
	if registry.heartbeatFrequency > 0 {
		go registry.loopWhileAlive(registry.heartbeatFrequency, (*registry.broker.GetTransit()).SendHeartbeat)
	}
	if registry.heartbeatTimeout > 0 {
		go registry.loopWhileAlive(registry.heartbeatTimeout, registry.checkExpiredRemoteNodes)
	}
	if registry.offlineCheckFrequency > 0 {
		go registry.loopWhileAlive(registry.offlineCheckFrequency, registry.checkOfflineNodes)

	}
}

func (registry *ServiceRegistry) removeServicesByNodeID(nodeID string) {

}

func (registry *ServiceRegistry) disconnectNode(node *Node) {
	nodeID := (*node).GetID()
	registry.removeServicesByNodeID(nodeID)
	registry.broker.GetLocalBus().EmitAsync("$node.disconnected", []interface{}{nodeID})
	registry.logger.Warnf("Node %s disconnected ", nodeID)
}

func (registry *ServiceRegistry) checkExpiredRemoteNodes() {
	expiredNodes := registry.nodes.expiredNodes(registry.heartbeatTimeout)
	for _, node := range expiredNodes {
		registry.disconnectNode(node)
	}
}

func (registry *ServiceRegistry) checkOfflineNodes() {
	expiredNodes := registry.nodes.expiredNodes(registry.offlineCheckFrequency * 10)
	timeout := registry.offlineCheckFrequency * 10
	for _, node := range expiredNodes {
		nodeID := (*node).GetID()
		registry.nodes.removeNode(nodeID)
		registry.logger.Warnf("Removed offline Node: %s  from the registry because it hasn't submitted heartbeat in %d seconds.", nodeID, timeout)
	}
}

// loopWhileAlive : can the delegate runction in the given frequency and stop whe  the registry is stoping
func (registry *ServiceRegistry) loopWhileAlive(frequency time.Duration, delegate func()) {
	for {
		if registry.stoping {
			break
		}
		delegate()
		time.Sleep(frequency)
	}
}

func (registry *ServiceRegistry) Stop() {
	registry.stoping = true
}

func (registry *ServiceRegistry) heartbeatMessageReceived(message *TransitMessage) {
	heartbeat := (*message).AsMap()
	succesful := (*registry.nodes).HeartBeat(heartbeat)
	if !succesful {
		sender := heartbeat["sender"].(string)
		(*registry.broker.GetTransit()).DiscoverNode(sender)
	}
}

func (registry *ServiceRegistry) disconnectMessageReceived(message *TransitMessage) {
	node, exists := registry.nodes.getNode((*message).Get("sender").String())
	if exists {
		registry.disconnectNode(node)
	}
}

// nodeInfoMessageReceived process the node info message.
func (registry *ServiceRegistry) nodeInfoMessageReceived(message *TransitMessage) {
	nodeInfo := (*message).AsMap()
	fmt.Println("nodeInfoMessageReceived nodeInfo: ", nodeInfo)
	services := nodeInfo["services"].([]interface{})
	nodeID := nodeInfo["sender"].(string)

	exists, reconnected := (*registry.nodes).Info(nodeInfo)

	for _, item := range services {
		serviceInfo := item.(map[string]interface{})
		updatedActions, newActions, deletedActions := (*registry.services).updateRemote(nodeID, serviceInfo)

		for _, newAction := range newActions {
			serviceAction := CreateServiceAction(
				serviceInfo["name"].(string),
				newAction.GetName(),
				nil,
				ActionSchema{})
			registry.actions.Add(nodeID, serviceAction, false)
		}

		for _, updates := range updatedActions {
			fullname := updates["name"].(string)
			registry.actions.Update(nodeID, fullname, updates)
		}

		for _, deleted := range deletedActions {
			fullname := deleted.GetFullName()
			registry.actions.Remove(nodeID, fullname)
		}
	}

	eventParam := []interface{}{nodeID}
	eventName := "$node.connected"
	if exists {
		eventName = "$node.updated"
	} else if reconnected {
		eventName = "$node.reconnected"
	}
	(*registry.broker.GetLocalBus()).EmitAsync(eventName, eventParam)
}

// HandleTransitMessage generic handle for all transit messages relevant to registry
//TODO : remove this and use broker local bus instead !
func (registry *ServiceRegistry) HandleTransitMessage(command string, message *TransitMessage) {
	handler := (*registry.messageHandler)[command]
	if handler == nil {
		panic(errors.New(fmt.Sprintf("Registry - HandleTransitMessage() invalid command: %s", command)))
	}
	handler(message)
}

// AddLocalService : add a local service to the registry
// it will create endpoints for all service actions.
func (registry *ServiceRegistry) AddLocalService(service *Service) {
	if registry.services.Has(service.GetName(), service.GetVersion(), (*registry.localNode).GetID()) {
		return
	}

	nodeID := (*registry.localNode).GetID()

	registry.services.Add(nodeID, service)

	for _, action := range service.GetActions() {
		registry.actions.Add(nodeID, action, true)
	}

	// for _, event := range service.GetEvents() {
	// 	registry.registerEvent(&event)
	// }

	(*registry.localNode).AddServices(service.AsMap())

	registry.logger.Infof("Registry - %s service is registered.", service.GetFullName())

	registry.broker.GetLocalBus().EmitAsync(
		"$registry.service.added",
		[]interface{}{service.Summary()})
}

// NextActionEndpoint it will find and return the Endpoint for the the given actionName.
// If multiple endpoints are found it will use the strategy to decide which one to use.
func (registry *ServiceRegistry) NextActionEndpoint(actionName string, strategy Strategy, opts ...OptionsFunc) Endpoint {
	nodeID := GetStringOption("nodeID", opts)
	if nodeID != "" {
		return registry.actions.NextEndpointFromNode(actionName, nodeID, WrapOptions(opts))
	}
	return registry.actions.NextEndpoint(actionName, strategy, WrapOptions(opts))
}

// func (registry *ServiceRegistry) regenerateLocalRawInfo(increaseSequence bool) map[string]interface{} {
// 	node := registry.localNode
// 	if increaseSequence {
// 		node.IncreaseSequence()
// 	}
// 	services := registry.services.getLocalNodeServices()
// 	node.rawInfo = map[string]interface{}{
// 		"ipList":   node.ipList,
// 		"hostname": node.hostname,
// 		"client":   node.client,
// 		"config":   node.config,
// 		"port":     node.port,
// 		"seq":      node.sequence,
// 		"services": services,
// 	}
// 	return node.rawInfo
// }
