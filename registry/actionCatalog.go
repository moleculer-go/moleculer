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

// func addToList(mapOfLists map[string][]*ActionEntry, key string, entry *ActionEntry) {
// 	// if mapOfLists[key] == nil {
// 	// 	mapOfLists[key] = make([]*ActionEntry)

// 	// }
// 	//check if this approach directly with append works..
// 	mapOfLists[key] = append(mapOfLists[key], entry)
// }

func (actionEntry *ActionEntry) InvokeAction(context *context.Context) chan interface{} {
	//TODO
	return nil
}

func (actionEntry *ActionEntry) IsLocal() bool {
	return actionEntry.isLocal
}

func (ationCatalog *ActionCatalog) Add(node *Node, action ServiceAction, local bool) {
	entry := &ActionEntry{node, &action, local}

	name := action.GetFullName()
	ationCatalog.actionsByName[name] = append(
		ationCatalog.actionsByName[name], entry)

	// ationCatalog.actionsByNode[node.id] = append(
	// 	ationCatalog.actionsByNode[node.id], ationCatalog.actionsByName[name])

	// addToList(ationCatalog.actionsByNode, node.id, entry)
	// addToList(ationCatalog.actionsByName, action.GetFullName(), entry)
}

func actionsToEndPoints(actions []*ActionEntry) []*Endpoint {
	result := make([]*Endpoint, len(actions))
	for index, action := range actions {
		result[index] = action
	}
	return result
}

func (actionCatalog *ActionCatalog) NextEndpointFromNode(actionName string, strategy Strategy, nodeID string, opts OptionsFunc) *Endpoint {
	mapOfActions := actionCatalog.actionsByNode[nodeID]
	actions := mapOfActions[actionName]
	return strategy.SelectEndpoint(actionsToEndPoints(actions))
}

func (actionCatalog *ActionCatalog) NextEndpoint(actionName string, strategy Strategy, opts OptionsFunc) *Endpoint {
	actions := actionCatalog.actionsByName[actionName]
	return strategy.SelectEndpoint(actionsToEndPoints(actions))
}
