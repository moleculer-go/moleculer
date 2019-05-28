package registry

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/moleculer-go/moleculer/middleware"

	"github.com/moleculer-go/moleculer/payload"

	"github.com/moleculer-go/moleculer"
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
	broker                *moleculer.BrokerDelegates
	strategy              strategy.Strategy
	stopping              bool
	heartbeatFrequency    time.Duration
	heartbeatTimeout      time.Duration
	offlineCheckFrequency time.Duration
	offlineTimeout        time.Duration
	nodeReceivedMutex     *sync.Mutex
}

// createTransit create a transit instance based on the config.
func createTransit(broker *moleculer.BrokerDelegates) transit.Transit {
	transit := pubsub.Create(broker)
	return transit
}

// createStrategy create a strsategy instance based on the config.
func createStrategy(broker *moleculer.BrokerDelegates) strategy.Strategy {
	//TODO: when new strategies are addes.. adde config check here to load the right one.
	return strategy.RoundRobinStrategy{}
}

func CreateRegistry(nodeID string, broker *moleculer.BrokerDelegates) *ServiceRegistry {
	config := broker.Config
	transit := createTransit(broker)
	strategy := createStrategy(broker)
	logger := broker.Logger("registry", nodeID)
	localNode := CreateNode(nodeID, true, logger.WithField("Node", nodeID))
	localNode.Unavailable()
	registry := &ServiceRegistry{
		broker:                broker,
		transit:               transit,
		strategy:              strategy,
		logger:                logger,
		localNode:             localNode,
		actions:               CreateActionCatalog(logger.WithField("catalog", "Actions")),
		events:                CreateEventCatalog(logger.WithField("catalog", "Events")),
		services:              CreateServiceCatalog(logger.WithField("catalog", "Services")),
		nodes:                 CreateNodesCatalog(logger.WithField("catalog", "Nodes")),
		heartbeatFrequency:    config.HeartbeatFrequency,
		heartbeatTimeout:      config.HeartbeatTimeout,
		offlineCheckFrequency: config.OfflineCheckFrequency,
		offlineTimeout:        config.OfflineTimeout,
		stopping:              false,
		nodeReceivedMutex:     &sync.Mutex{},
	}

	registry.logger.Debug("Service Registry created for broker: ", nodeID)

	broker.Bus().On("$broker.started", func(args ...interface{}) {
		registry.logger.Debug("Registry -> $broker.started event")
		registry.localNode.Available()
	})

	registry.setupMessageHandlers()

	return registry
}

func (registry *ServiceRegistry) KnowService(name string) bool {
	return registry.services.FindByName(name)
}

func (registry *ServiceRegistry) KnowNode(nodeID string) bool {
	_, found := registry.nodes.findNode(nodeID)
	return found
}

func (registry *ServiceRegistry) LocalNode() moleculer.Node {
	return registry.localNode
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
	registry.logger.Debug("Registry Stopping...")
	registry.stopping = true
	err := <-registry.transit.Disconnect()
	registry.localNode.Unavailable()
	if err != nil {
		registry.logger.Debug("Error trying to disconnect transit - error: ", err)
		return
	}
	registry.logger.Debug("Transit Disconnected -> Registry Full Stop!")
}

func (registry *ServiceRegistry) LocalServices() []*service.Service {
	return []*service.Service{createNodeService(registry)}
}

// Start : start the registry background processes.
func (registry *ServiceRegistry) Start() {
	registry.logger.Debug("Registry Start() ")
	registry.stopping = false
	err := <-registry.transit.Connect()
	if err != nil {
		panic(errors.New(fmt.Sprint("Could not connect to the transit. err: ", err)))
	}
	<-registry.transit.DiscoverNodes()

	registry.nodes.Add(registry.localNode)

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

func (registry *ServiceRegistry) ServiceForAction(name string) *service.Service {
	action := registry.actions.Find(name, true)
	if action != nil {
		return action.Service()
	}
	return nil
}

// HandleRemoteEvent handle when a remote event is delivered and call all the local handlers.
func (registry *ServiceRegistry) HandleRemoteEvent(context moleculer.BrokerContext) {
	name := context.EventName()
	groups := context.Groups()
	if registry.stopping {
		registry.logger.Error("HandleRemoteEvent() - registry is stopping. Discarding event -> name: ", name, " groups: ", groups)
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
		go localEvent.emitLocalEvent(context)
	}
}

// LoadBalanceEvent load balance an event based on the known targetNodes.
func (registry *ServiceRegistry) LoadBalanceEvent(context moleculer.BrokerContext) []*EventEntry {
	name := context.EventName()
	params := context.Payload()
	groups := context.Groups()
	eventSig := fmt.Sprint("name: ", name, " groups: ", groups)
	registry.logger.Trace("LoadBalanceEvent() - ", eventSig, " params: ", params)

	entries := registry.events.Find(name, groups, true, false, registry.strategy)
	if entries == nil {
		msg := fmt.Sprint("Broker - no endpoints found for event: ", name, " it was discarded!")
		registry.logger.Warn(msg)
		return nil
	}

	for _, eventEntry := range entries {
		if eventEntry.isLocal {
			go eventEntry.emitLocalEvent(context)
		} else {
			go registry.emitRemoteEvent(context, eventEntry)
		}
	}
	registry.logger.Trace("LoadBalanceEvent() - ", eventSig, " End.")
	return entries
}

func (registry *ServiceRegistry) BroadcastEvent(context moleculer.BrokerContext) []*EventEntry {
	name := context.EventName()
	groups := context.Groups()
	eventSig := fmt.Sprint("name: ", name, " groups: ", groups)
	registry.logger.Trace("BroadcastEvent() - ", eventSig, " payload: ", context.Payload())

	entries := registry.events.Find(name, groups, false, false, nil)
	if entries == nil {
		msg := fmt.Sprint("Broker - no endpoints found for event: ", name, " it was discarded!")
		registry.logger.Warn(msg)
		return nil
	}

	for _, eventEntry := range entries {
		if eventEntry.isLocal {
			go eventEntry.emitLocalEvent(context)
		} else {
			go registry.emitRemoteEvent(context, eventEntry)
		}
	}
	registry.logger.Trace("BroadcastEvent() - ", eventSig, " End.")
	return entries
}

// DelegateCall : invoke a service action and return a channel which will eventualy deliver the results ;).
// This call might be local or remote.
func (registry *ServiceRegistry) LoadBalanceCall(context moleculer.BrokerContext, opts ...moleculer.Options) chan moleculer.Payload {
	actionName := context.ActionName()
	params := context.Payload()
	registry.logger.Trace("LoadBalanceCall() - actionName: ", actionName, " params: ", params, " opts: ", opts)

	actionEntry := registry.nextAction(actionName, registry.strategy, opts...)
	if actionEntry == nil {
		msg := fmt.Sprint("Registry - endpoint not found for actionName: ", actionName)
		registry.logger.Error(msg)
		resultChan := make(chan moleculer.Payload, 1)
		resultChan <- payload.Error(msg)
		return resultChan
	}
	registry.logger.Debug("LoadBalanceCall() - actionName: ", actionName, " target nodeID: ", actionEntry.TargetNodeID())

	if actionEntry.isLocal {
		registry.broker.MiddlewareHandler("beforeLocalAction", context)
		result := <-actionEntry.invokeLocalAction(context)
		tempParams := registry.broker.MiddlewareHandler("afterLocalAction", middleware.AfterActionParams{context, result})
		actionParams := tempParams.(middleware.AfterActionParams)

		resultChan := make(chan moleculer.Payload, 1)
		resultChan <- actionParams.Result
		return resultChan
	}

	registry.broker.MiddlewareHandler("beforeRemoteAction", context)
	result := <-registry.invokeRemoteAction(context, actionEntry)
	tempParams := registry.broker.MiddlewareHandler("afterRemoteAction", middleware.AfterActionParams{context, result})
	actionParams := tempParams.(middleware.AfterActionParams)

	resultChan := make(chan moleculer.Payload, 1)
	resultChan <- actionParams.Result
	return resultChan

}

func (registry *ServiceRegistry) emitRemoteEvent(context moleculer.BrokerContext, eventEntry *EventEntry) {
	context.SetTargetNodeID(eventEntry.TargetNodeID())
	registry.logger.Trace("Before invoking remote event: ", context.EventName(), " context.TargetNodeID: ", context.TargetNodeID(), " context.Payload(): ", context.Payload())
	registry.transit.Emit(context)
}

func (registry *ServiceRegistry) invokeRemoteAction(context moleculer.BrokerContext, actionEntry *ActionEntry) chan moleculer.Payload {
	result := make(chan moleculer.Payload, 1)
	context.SetTargetNodeID(actionEntry.TargetNodeID())
	registry.logger.Trace("Before invoking remote action: ", context.ActionName(), " context.TargetNodeID: ", context.TargetNodeID(), " context.Payload(): ", context.Payload())

	go func() {
		actionResult := <-registry.transit.Request(context)
		registry.logger.Trace("remote request done! action: ", context.ActionName(), " results: ", actionResult)
		if registry.stopping {
			registry.logger.Error("invokeRemoteAction() - registry is stopping. Discarding action result -> name: ", context.ActionName())
			result <- payload.New(errors.New("can't complete request! registry stopping..."))
		} else {
			result <- actionResult
		}
	}()
	return result
}

// removeServicesByNodeID
func (registry *ServiceRegistry) removeServicesByNodeID(nodeID string) {
	svcs := registry.services.RemoveByNode(nodeID)
	if len(svcs) > 0 {
		for _, svc := range svcs {
			registry.broker.Bus().EmitAsync(
				"$registry.service.removed",
				[]interface{}{svc.Summary()})
		}
	}
	registry.actions.RemoveByNode(nodeID)
	registry.events.RemoveByNode(nodeID)
}

// disconnectNode remove node info (actions, events) from local registry.
func (registry *ServiceRegistry) disconnectNode(nodeID string) {
	node, exists := registry.nodes.findNode(nodeID)
	if !exists {
		return
	}
	registry.removeServicesByNodeID(nodeID)
	node.Unavailable()
	registry.broker.Bus().EmitAsync("$node.disconnected", []interface{}{nodeID})
	registry.logger.Warnf("Node %s disconnected ", nodeID)
}

func (registry *ServiceRegistry) checkExpiredRemoteNodes() {
	expiredNodes := registry.nodes.expiredNodes(registry.heartbeatTimeout)
	for _, node := range expiredNodes {
		registry.disconnectNode(node.GetID())
	}
}

func (registry *ServiceRegistry) checkOfflineNodes() {
	expiredNodes := registry.nodes.expiredNodes(registry.offlineTimeout)
	for _, node := range expiredNodes {
		nodeID := node.GetID()
		registry.nodes.removeNode(nodeID)
		registry.logger.Warnf("Removed offline Node: %s  from the registry because it hasn't submitted heartbeat in %d seconds.", nodeID, registry.offlineTimeout)
	}
}

// loopWhileAlive : can the delegate runction in the given frequency and stop whe  the registry is stopping
func (registry *ServiceRegistry) loopWhileAlive(frequency time.Duration, delegate func()) {
	for {
		if registry.stopping {
			break
		}
		delegate()
		time.Sleep(frequency)
	}
}

func (registry *ServiceRegistry) filterMessages(handler func(message moleculer.Payload)) func(message moleculer.Payload) {
	return func(message moleculer.Payload) {
		if registry.stopping {
			registry.logger.Warn("filterMessages() - registry is stopping. Discarding message: ", message)
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
		registry.disconnectNode(node.GetID())
	}
}

func compatibility(info map[string]interface{}) map[string]interface{} {
	_, exists := info["version"]
	if !exists {
		info["version"] = ""
	}
	return info
}

// remoteNodeInfoReceived process the remote node info message and add to local registry.
func (registry *ServiceRegistry) remoteNodeInfoReceived(message moleculer.Payload) {
	registry.nodeReceivedMutex.Lock()
	defer registry.nodeReceivedMutex.Unlock()
	nodeID := message.Get("sender").String()
	services := message.Get("services").MapArray()
	exists, reconnected := registry.nodes.Info(message.RawMap())
	for _, serviceInfo := range services {
		serviceInfo = compatibility(serviceInfo)
		svc, newService, updatedActions, newActions, deletedActions, updatedEvents, newEvents, deletedEvents := registry.services.updateRemote(nodeID, serviceInfo)

		for _, newAction := range newActions {
			serviceAction := service.CreateServiceAction(
				serviceInfo["name"].(string),
				newAction.Name(),
				nil,
				moleculer.ObjectSchema{nil})
			registry.actions.Add(serviceAction, svc, false)
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
			registry.events.Add(serviceEvent, svc, false)
		}

		for _, updates := range updatedEvents {
			name := updates["name"].(string)
			registry.events.Update(nodeID, name, updates)
		}

		for _, deleted := range deletedEvents {
			name := deleted.Name()
			registry.events.Remove(nodeID, name)
		}

		if newService {
			registry.logger.Infof("Registry - remote %s service is registered.", svc.FullName())

			registry.broker.Bus().EmitAsync(
				"$registry.service.added",
				[]interface{}{svc.Summary()})
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

// subscribeInternalEvent subscribe event listeners for internal events (e.g. $node.disconnected) using the localBus.
func (registry *ServiceRegistry) subscribeInternalEvent(event service.Event) {
	registry.broker.Bus().On(event.Name(), func(data ...interface{}) {
		params := payload.New(nil)
		if len(data) > 0 {
			params = payload.New(data[0])
		}
		brokerContext := registry.broker.BrokerContext()
		eventContext := brokerContext.ChildEventContext(event.Name(), params, nil, false)
		event.Handler()(eventContext.(moleculer.Context), params)
	})
}

// AddLocalService : add a local service to the registry
// it will create endpoints for all service actions.
func (registry *ServiceRegistry) AddLocalService(service *service.Service) {
	if registry.services.Find(service.Name(), service.Version(), registry.localNode.GetID()) {
		registry.logger.Trace("registry - AddLocalService() - Service already registered, will ignore.. service fullName: ", service.FullName())
		return
	}

	registry.services.Add(service)

	actions := service.Actions()
	events := service.Events()

	for _, action := range actions {
		registry.actions.Add(action, service, true)
	}
	for _, event := range events {
		if strings.Index(event.Name(), "$") == 0 {
			registry.subscribeInternalEvent(event)
		} else {
			registry.events.Add(event, service, true)
		}
	}
	registry.localNode.Publish(service.AsMap())
	registry.logger.Debug("Registry published local service: ", service.FullName(), " # actions: ", len(actions), " # events: ", len(events), " nodeID: ", service.NodeID())
	registry.notifyServiceAded(service.Summary())
}

// notifyServiceAded notify when a service is added to the registry.
func (registry *ServiceRegistry) notifyServiceAded(svc map[string]string) {
	if registry.broker.IsStarted() {
		registry.broker.Bus().EmitAsync(
			"$registry.service.added",
			[]interface{}{svc})
	} else {
		registry.broker.Bus().Once("$broker.started", func(...interface{}) {
			registry.broker.Bus().EmitAsync(
				"$registry.service.added",
				[]interface{}{svc})
		})
	}
}

// nextAction it will find and return the next action to be invoked.
// If multiple nodes that contain this action are found it will use the strategy to decide which one to use.
func (registry *ServiceRegistry) nextAction(actionName string, strategy strategy.Strategy, opts ...moleculer.Options) *ActionEntry {
	if len(opts) > 0 && opts[0].NodeID != "" {
		return registry.actions.NextFromNode(actionName, opts[0].NodeID)
	}
	return registry.actions.Next(actionName, strategy)
}

func (registry *ServiceRegistry) KnownEventListeners(addNode bool) []string {
	events := registry.events.list()
	result := make([]string, len(events))
	for index, event := range events {
		if addNode {
			result[index] = fmt.Sprint(event.targetNodeID, ".", event.event.ServiceName(), ".", event.event.Name())
		} else {
			result[index] = fmt.Sprint(event.event.ServiceName(), ".", event.event.Name())
		}

	}
	sort.Strings(result)
	return result
}

func (registry *ServiceRegistry) KnownNodes() []string {
	nodes := registry.nodes.list()
	result := make([]string, len(nodes))
	for index, node := range nodes {
		result[index] = node.GetID()
	}
	sort.Strings(result)
	return result
}
