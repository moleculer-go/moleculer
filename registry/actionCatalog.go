package registry

import (
	"context"

	. "github.com/moleculer-go/moleculer/common"
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

func (actionEntry *ActionEntry) InvokeAction(context *context.Context) chan interface{} {
	//TODO

	return nil
}

func (actionEntry *ActionEntry) IsLocal() bool {
	return actionEntry.isLocal
}

func (actionCatalog *ActionCatalog) Add(node *Node, action ServiceAction, local bool) {
	entry := &ActionEntry{node, &action, local}

	name := action.GetFullName()
	actionCatalog.actionsByName[name] = append(
		actionCatalog.actionsByName[name], entry)

	if actionCatalog.actionsByNode[node.id] == nil {
		actionCatalog.actionsByNode[node.id] = make(actionsMap)
	}
	actionCatalog.actionsByNode[node.id][name] = append(
		actionCatalog.actionsByNode[node.id][name], entry)
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
