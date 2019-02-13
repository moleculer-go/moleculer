package registry

import (
	"errors"
	"fmt"
	"time"

	"github.com/moleculer-go/moleculer/payload"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/options"
	"github.com/moleculer-go/moleculer/service"
	"github.com/moleculer-go/moleculer/strategy"

	"github.com/moleculer-go/moleculer/transit"
	"github.com/moleculer-go/moleculer/transit/pubsub"
	log "github.com/sirupsen/logrus"
)

type messageHandlerFunc func(message moleculer.Payload)

type ServiceRegistry struct {
	logger                *log.Entry
	transit               transit.Transit
	localNode             moleculer.Node
	nodes                 *NodeCatalog
	services              *ServiceCatalog
	actions               *ActionCatalog
	events                *EventCatalog
	broker                moleculer.BrokerDelegates
	strategy              strategy.Strategy
	stoping               bool
	heartbeatFrequency    time.Duration
	heartbeatTimeout      time.Duration
	offlineCheckFrequency time.Duration
}

// createTransit create a transit instance based on the config.
func createTransit(broker moleculer.BrokerDelegates) transit.Transit {
	transit := pubsub.Create(broker)
	return transit
}

// createStrategy create a strsategy instance based on the config.
func createStrategy(broker moleculer.BrokerDelegates) strategy.Strategy {
	//TODO: when new strategies are addes.. adde config check here to load the right one.
	return strategy.RoundRobinStrategy{}
}

func CreateRegistry(broker moleculer.BrokerDelegates) *ServiceRegistry {
	config := broker.Config
	transit := createTransit(broker)
	strategy := createStrategy(broker)
	registry := &ServiceRegistry{
		broker:                broker,
		transit:               transit,
		strategy:              strategy,
		logger:                broker.Logger("registry", "Service Registry"),
		actions:               CreateActionCatalog(),
		events:                CreateEventCatalog(broker.Logger("catalog", "Events")),
		services:              CreateServiceCatalog(broker.Logger("catalog", "Services")),
		nodes:                 CreateNodesCatalog(),
		localNode:             broker.LocalNode(),
		heartbeatFrequency:    config.HeartbeatFrequency,
		heartbeatTimeout:      config.HeartbeatTimeout,
		offlineCheckFrequency: config.OfflineCheckFrequency,
		stoping:               false,
	}

	registry.logger.Info("Service Registry created for broker: ", broker.LocalNode().GetID())

	broker.Bus().On("$broker.started", func(args ...interface{}) {
		registry.logger.Debug("Registry -> $broker.started event")
		if registry.localNode != nil {
			//TODO: broadcast info ? I think we do that elsewhere already..
		}
	})

	registry.setupMessageHandlers()

	return registry
}

func (registry *ServiceRegistry) KnowService(name string) bool {
	return registry.services.FindByName(name)
}

func (registry *ServiceRegistry) setupMessageHandlers() {
	messageHandler := map[string]messageHandlerFunc{
		"HEARTBEAT":  registry.filterMessages(registry.heartbeatMessageReceived),
		"DISCONNECT": registry.filterMessages(registry.disconnectMessageReceived),
		"INFO":       registry.filterMessages(registry.remoteNodeInfoReceived),
	}
	registry.broker.Bus().On("$registry.transit.message", func(args ...interface{}) {
		registry.logger.Trace("Registry -> $registry.transit.message event - args: ", args)
		command := args[0].(string)
		message := args[1].(moleculer.Payload)
		handler := messageHandler[command]
		if handler == nil {
			panic(errors.New(fmt.Sprint("Registry - $registry.transit.message event - invalid command:", command)))
		}
		handler(message)
	})
}

func (registry *ServiceRegistry) Stop() {
	registry.logger.Debug("Registry Stoping...")
	registry.stoping = true
	<-registry.transit.Disconnect()
	registry.logger.Debug("Transit Disconnected -> Registry Full Stop!")

}

// Start : start the registry background processes.
func (registry *ServiceRegistry) Start() {
	registry.logger.Debug("Registry Start() ")
	registry.stoping = false
	connected := <-registry.transit.Connect()
	if !connected {
		panic(errors.New("Could not connect to the transit. Check logs for more details."))
	}
	<-registry.transit.DiscoverNodes()

	if registry.heartbeatFrequency > 0 {
		go registry.loopWhileAlive(registry.heartbeatFrequency, registry.transit.SendHeartbeat)
	}
	if registry.heartbeatTimeout > 0 {
		go registry.loopWhileAlive(registry.heartbeatTimeout, registry.checkExpiredRemoteNodes)
	}
	if registry.offlineCheckFrequency > 0 {
		go registry.loopWhileAlive(registry.offlineCheckFrequency, registry.checkOfflineNodes)
	}
}

// HandleRemoteEvent handle when a remote event is delivered and call all the local handlers.
func (registry *ServiceRegistry) HandleRemoteEvent(context moleculer.BrokerContext) {
	name := context.EventName()
	groups := context.Groups()
	if registry.stoping {
		registry.logger.Error("HandleRemoteEvent() - registry is stoping. Discarding event -> name: ", name, " groups: ", groups)
		return
	}
	broadcast := context.IsBroadcast()
	registry.logger.Debug("HandleRemoteEvent() - name: ", name, " groups: ", groups)

	var stg strategy.Strategy
	if !broadcast {
		stg = registry.strategy
	}
	entries := registry.events.Find(name, groups, true, true, stg)
	for _, localEvent := range entries {
		localEvent.emitLocalEvent(context)
	}
}

// LoadBalanceEvent load balance an event based on the known targetNodes.
func (registry *ServiceRegistry) LoadBalanceEvent(context moleculer.BrokerContext) {
	name := context.EventName()
	params := context.Payload()
	groups := context.Groups()
	eventSig := fmt.Sprint("name: ", name, " groups: ", groups)
	registry.logger.Trace("LoadBalanceEvent() - ", eventSig, " params: ", params)

	entries := registry.events.Find(name, groups, true, false, registry.strategy)
	if entries == nil {
		msg := fmt.Sprint("Broker - no endpoints found for event: ", name, " it was discarded!")
		registry.logger.Warn(msg)
		return
	}

	for _, eventEntry := range entries {
		if eventEntry.isLocal {
			go eventEntry.emitLocalEvent(context)
		} else {
			go registry.emitRemoteEvent(context, eventEntry)
		}
	}
	registry.logger.Trace("LoadBalanceEvent() - ", eventSig, " End.")
}

func (registry *ServiceRegistry) BroadcastEvent(context moleculer.BrokerContext) {
	name := context.EventName()
	groups := context.Groups()
	eventSig := fmt.Sprint("name: ", name, " groups: ", groups)
	registry.logger.Trace("BroadcastEvent() - ", eventSig, " payload: ", context.Payload())

	entries := registry.events.Find(name, groups, false, false, nil)
	if entries == nil {
		msg := fmt.Sprint("Broker - no endpoints found for event: ", name, " it was discarded!")
		registry.logger.Warn(msg)
		return
	}

	for _, eventEntry := range entries {
		if eventEntry.isLocal {
			go eventEntry.emitLocalEvent(context)
		} else {
			go registry.emitRemoteEvent(context, eventEntry)
		}
	}
	registry.logger.Trace("BroadcastEvent() - ", eventSig, " End.")
}

// DelegateCall : invoke a service action and return a channel which will eventualy deliver the results ;).
// This call might be local or remote.
func (registry *ServiceRegistry) LoadBalanceCall(context moleculer.BrokerContext, opts ...moleculer.OptionsFunc) chan moleculer.Payload {
	actionName := context.ActionName()
	params := context.Payload()
	registry.logger.Trace("LoadBalanceCall() - actionName: ", actionName, " params: ", params, " opts: ", opts)

	actionEntry := registry.nextAction(actionName, registry.strategy, options.Wrap(opts))
	if actionEntry == nil {
		msg := fmt.Sprintf("Broker - endpoint not found for actionName: %s", actionName)
		registry.logger.Error(msg)
		panic(errors.New(msg))
	}
	registry.logger.Debug("LoadBalanceCall() - actionName: ", actionName, " target nodeID: ", actionEntry.TargetNodeID())

	if actionEntry.isLocal {
		return actionEntry.invokeLocalAction(context)
	}
	return registry.invokeRemoteAction(context, actionEntry)
}

func (registry *ServiceRegistry) emitRemoteEvent(context moleculer.BrokerContext, eventEntry *EventEntry) {
	context.SetTargetNodeID(eventEntry.TargetNodeID())
	registry.logger.Trace("Before invoking remote event: ", context.EventName(), " context.TargetNodeID: ", context.TargetNodeID(), " context.Payload(): ", context.Payload())
	registry.transit.Emit(context)
}

func (registry *ServiceRegistry) invokeRemoteAction(context moleculer.BrokerContext, actionEntry *ActionEntry) chan moleculer.Payload {
	result := make(chan moleculer.Payload)
	context.SetTargetNodeID(actionEntry.TargetNodeID())
	registry.logger.Trace("Before invoking remote action: ", context.ActionName(), " context.TargetNodeID: ", context.TargetNodeID(), " context.Payload(): ", context.Payload())

	go func() {
		actionResult := <-registry.transit.Request(context)
		registry.logger.Trace("remote request done! action: ", context.ActionName(), " results: ", actionResult)
		if registry.stoping {
			registry.logger.Error("invokeRemoteAction() - registry is stoping. Discarding action result -> name: ", context.ActionName())
			result <- payload.Create(errors.New("can't complete request! registry stoping..."))
		} else {
			result <- actionResult
		}
	}()
	return result
}

// removeServicesByNodeID
func (registry *ServiceRegistry) removeServicesByNodeID(nodeID string) {
	registry.services.RemoveByNode(nodeID)
	registry.actions.RemoveByNode(nodeID)
	registry.events.RemoveByNode(nodeID)
}

// disconnectNode remove node info (actions, events) from local registry.
func (registry *ServiceRegistry) disconnectNode(node moleculer.Node) {
	nodeID := node.GetID()
	registry.removeServicesByNodeID(nodeID)
	registry.broker.Bus().EmitAsync("$node.disconnected", []interface{}{nodeID})
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
		nodeID := node.GetID()
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

func (registry *ServiceRegistry) filterMessages(handler func(message moleculer.Payload)) func(message moleculer.Payload) {
	return func(message moleculer.Payload) {
		if registry.stoping {
			registry.logger.Error("filterMessages() - registry is stoping. Discarding message: ", message)
			return
		}
		if message.Get("sender").Exists() && message.Get("sender").String() == registry.localNode.GetID() {
			registry.logger.Debug("filterMessages() - Same host message (sender == localNodeID). discarding... ", message)
			return
		}
		handler(message)
	}
}

func (registry *ServiceRegistry) heartbeatMessageReceived(message moleculer.Payload) {
	heartbeat := message.RawMap()
	succesful := registry.nodes.HeartBeat(heartbeat)
	if !succesful {
		sender := heartbeat["sender"].(string)
		registry.transit.DiscoverNode(sender)
	}
}

// disconnectMessageReceived handles when a disconnect msg is received.
// It remove all actions/events from the sender node from the local registry.
func (registry *ServiceRegistry) disconnectMessageReceived(message moleculer.Payload) {
	sender := message.Get("sender").String()
	node, exists := registry.nodes.findNode(sender)
	registry.logger.Debug("disconnectMessageReceived() sender: ", sender, " exists: ", exists)
	if exists {
		registry.disconnectNode(node)
	}
}

// remoteNodeInfoReceived process the remote node info message and add to local registry.
func (registry *ServiceRegistry) remoteNodeInfoReceived(message moleculer.Payload) {
	nodeInfo := message.RawMap()
	nodeID := nodeInfo["sender"].(string)
	item := nodeInfo["services"]
	var services []interface{}
	if item != nil {
		services = item.([]interface{})
	}
	exists, reconnected := registry.nodes.Info(nodeInfo)
	for _, item := range services {
		serviceInfo := item.(map[string]interface{})
		updatedActions, newActions, deletedActions, updatedEvents, newEvents, deletedEvents := registry.services.updateRemote(nodeID, serviceInfo)

		for _, newAction := range newActions {
			serviceAction := service.CreateServiceAction(
				serviceInfo["name"].(string),
				newAction.Name(),
				nil,
				moleculer.ParamsSchema{})
			registry.actions.Add(nodeID, serviceAction, false)
		}

		for _, updates := range updatedActions {
			fullname := updates["name"].(string)
			registry.actions.Update(nodeID, fullname, updates)
		}

		for _, deleted := range deletedActions {
			fullname := deleted.FullName()
			registry.actions.Remove(nodeID, fullname)
		}

		for _, newEvent := range newEvents {
			serviceEvent := service.CreateServiceEvent(
				newEvent.Name(),
				serviceInfo["name"].(string),
				newEvent.Group(),
				newEvent.Handler())
			registry.events.Add(nodeID, serviceEvent, false)
		}

		for _, updates := range updatedEvents {
			name := updates["name"].(string)
			registry.events.Update(nodeID, name, updates)
		}

		for _, deleted := range deletedEvents {
			name := deleted.Name()
			registry.events.Remove(nodeID, name)
		}
	}

	var neighbours int64
	if message.Get("neighbours").Exists() {
		neighbours = message.Get("neighbours").Int64()
	}

	eventParam := []interface{}{nodeID, neighbours}
	eventName := "$node.connected"
	if exists {
		eventName = "$node.updated"
	} else if reconnected {
		eventName = "$node.reconnected"
	}
	registry.broker.Bus().EmitAsync(eventName, eventParam)
}

// AddLocalService : add a local service to the registry
// it will create endpoints for all service actions.
func (registry *ServiceRegistry) AddLocalService(service *service.Service) {
	if registry.services.Find(service.Name(), service.Version(), registry.localNode.GetID()) {
		return
	}

	nodeID := registry.localNode.GetID()
	registry.logger.Debug("AddLocalService() nodeID: ", nodeID, " service.fullname: ", service.FullName())

	registry.services.Add(nodeID, service)

	for _, action := range service.Actions() {
		registry.actions.Add(nodeID, action, true)
	}

	for _, event := range service.Events() {
		registry.events.Add(nodeID, event, true)
	}

	registry.localNode.AddService(service.AsMap())

	registry.logger.Infof("Registry - %s service is registered.", service.FullName())

	registry.broker.Bus().EmitAsync(
		"$registry.service.added",
		[]interface{}{service.Summary()})
}

// nextAction it will find and return the next action to be invoked.
// If multiple nodes that contain this action are found it will use the strategy to decide which one to use.
func (registry *ServiceRegistry) nextAction(actionName string, strategy strategy.Strategy, opts ...moleculer.OptionsFunc) *ActionEntry {
	nodeID := options.String("nodeID", opts)
	if nodeID != "" {
		return registry.actions.NextFromNode(actionName, nodeID)
	}
	return registry.actions.Next(actionName, strategy)
}
