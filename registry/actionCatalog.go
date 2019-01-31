package registry

import (
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/service"
	"github.com/moleculer-go/moleculer/strategy"
)

type ActionEntry struct {
	targetNodeID string
	action       *service.Action
	isLocal      bool
}

type actionsMap map[string][]ActionEntry

type ActionCatalog struct {
	actionsByName *actionsMap
}

func CreateActionCatalog() *ActionCatalog {
	actionsByName := make(actionsMap)
	return &ActionCatalog{&actionsByName}
}

func (actionEntry *ActionEntry) invokeLocalAction(context moleculer.BrokerContext) chan interface{} {
	result := make(chan interface{})

	logger := context.Logger().WithField("actionCatalog", "invokeLocalAction")
	logger.Debug("Before Invoking action: ", context.ActionName())

	go func() {
		handler := actionEntry.action.Handler()
		actionResult := handler(context.(moleculer.Context), context.Params())
		logger.Debug("local action invoked ! action: ", context.ActionName(),
			" results: ", actionResult)
		result <- actionResult
	}()

	return result
}

func (actionEntry ActionEntry) TargetNodeID() string {
	return actionEntry.targetNodeID
}

//move the logic to decide between local and remote to the registry
// func (actionEntry ActionEntry) InvokeAction(ctx moleculer.Context) chan interface{} {
// 	if actionEntry.isLocal {
// 		return actionEntry.invokeLocalAction(ctx)
// 	}
// 	(*ctx).SetTargetNodeID(actionEntry.TargetNodeID())
// 	return actionEntry.invokeRemoteAction(ctx)
// }

func (actionEntry ActionEntry) IsLocal() bool {
	return actionEntry.isLocal
}

// Add a new action to the catalog.
func (actionCatalog *ActionCatalog) Add(nodeID string, action service.Action, local bool) {
	entry := ActionEntry{nodeID, &action, local}
	name := action.FullName()
	actions := (*actionCatalog.actionsByName)

	actions[name] = append(actions[name], entry)
}

func (actionCatalog *ActionCatalog) Update(nodeID string, fullname string, updates map[string]interface{}) {
	//TODO .. the only thing that can be udpated is the Action Schema (validation) and that does not exist yet
}

func (actionCatalog *ActionCatalog) Remove(nodeID string, name string) {
	var newList []ActionEntry
	actions := (*actionCatalog.actionsByName)
	//fmt.Println("\nRemove() nodeID: ", nodeID, " name: ", name, " actions: ", actions)

	options := actions[name]
	for _, action := range options {
		if action.targetNodeID != nodeID {
			newList = append(newList, action)
		}
	}
	actions[name] = newList
}

func (actionCatalog *ActionCatalog) NextFromNode(actionName string, nodeID string) *ActionEntry {
	actions := (*actionCatalog.actionsByName)[actionName]
	for _, action := range actions {
		if action.targetNodeID == nodeID {
			return &action
		}
	}
	return nil
}

// NextEndpoint find all actions registered in this node and use the strategy to select and return the best one to be called.
func (actionCatalog *ActionCatalog) Next(actionName string, strategy strategy.Strategy) *ActionEntry {
	actions := (*actionCatalog.actionsByName)[actionName]
	nodes := make([]string, len(actions))
	for index, action := range actions {
		nodes[index] = action.targetNodeID
	}
	return actionCatalog.NextFromNode(actionName, strategy.SelectTargetNode(nodes))
}

func (actionCatalog *ActionCatalog) Size() int {
	return len((*actionCatalog.actionsByName))
}
