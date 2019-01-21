package registry

import (
	"context"

	. "github.com/moleculer-go/moleculer/common"
	. "github.com/moleculer-go/moleculer/params"
	. "github.com/moleculer-go/moleculer/service"
)

type ActionEntry struct {
	node    *Node
	action  *ServiceAction
	isLocal bool
}

type actionsMap map[string][]*ActionEntry

type ActionCatalog struct {
	actionsByName actionsMap
	actionsByNode map[string]actionsMap
}

func CreateActionCatalog() *ActionCatalog {
	actionsByName := make(actionsMap)
	actionsByNode := make(map[string]actionsMap)
	return &ActionCatalog{actionsByName, actionsByNode}
}

func invokeRemoteAction(ctx *context.Context, actionEntry *ActionEntry) chan interface{} {
	//TODO
	return nil
}

func invokeLocalAction(ctx *context.Context, actionEntry *ActionEntry) chan interface{} {

	result := make(chan interface{})

	//Apply before middlewares here ? or at the broker level ?

	//invoke action :)
	go func() {
		handler := actionEntry.action.GetHandler()
		actionChannel := handler(*ctx, ParamsFromContext(ctx))
		result <- actionChannel
	}()

	//Apply after middlewares

	return result
}

func (actionEntry *ActionEntry) InvokeAction(ctx *context.Context) chan interface{} {
	if actionEntry.isLocal {
		return invokeLocalAction(ctx, actionEntry)
	}
	return invokeRemoteAction(ctx, actionEntry)
}

func (actionEntry *ActionEntry) IsLocal() bool {
	return actionEntry.isLocal
}

func (actionCatalog *ActionCatalog) Add(node *Node, action ServiceAction, local bool) {
	entry := &ActionEntry{node, &action, local}

	name := action.GetFullName()
	actionCatalog.actionsByName[name] = append(
		actionCatalog.actionsByName[name], entry)

	nodeID := (*node).GetID()
	if actionCatalog.actionsByNode[nodeID] == nil {
		actionCatalog.actionsByNode[nodeID] = make(actionsMap)
	}
	actionCatalog.actionsByNode[nodeID][name] = append(
		actionCatalog.actionsByNode[nodeID][name], entry)
}

func actionsToEndPoints(actions []*ActionEntry) []Endpoint {
	result := make([]Endpoint, len(actions))
	for index, action := range actions {
		var endpoint Endpoint = action
		result[index] = endpoint
	}
	return result
}

func (actionCatalog *ActionCatalog) NextEndpointFromNode(actionName string, strategy Strategy, nodeID string, opts ...OptionsFunc) Endpoint {
	mapOfActions := actionCatalog.actionsByNode[nodeID]
	actions := mapOfActions[actionName]
	return strategy.SelectEndpoint(actionsToEndPoints(actions))
}

func (actionCatalog *ActionCatalog) NextEndpoint(actionName string, strategy Strategy, opts ...OptionsFunc) Endpoint {
	actions := actionCatalog.actionsByName[actionName]
	return strategy.SelectEndpoint(actionsToEndPoints(actions))
}

func (actionCatalog *ActionCatalog) Size() int {
	return len(actionCatalog.actionsByName)
}
