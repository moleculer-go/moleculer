package registry

import (
	"sync"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/payload"
	"github.com/moleculer-go/moleculer/service"
	"github.com/moleculer-go/moleculer/strategy"
	log "github.com/sirupsen/logrus"
)

type ActionEntry struct {
	targetNodeID string
	action       *service.Action
	isLocal      bool
}

type actionsMap map[string][]ActionEntry

type ActionCatalog struct {
	actionsByName actionsMap
	mutex         *sync.Mutex
}

func CreateActionCatalog() *ActionCatalog {
	actionsByName := make(actionsMap)
	mutex := &sync.Mutex{}
	return &ActionCatalog{actionsByName: actionsByName, mutex: mutex}
}

func catchActionError(context moleculer.BrokerContext, logger *log.Entry, result chan moleculer.Payload) {
	if err := recover(); err != nil {
		logger.Error("local action failed :( action: ", context.ActionName(), " error: ", err)
		result <- payload.Create(err)
	}
}

func (actionEntry *ActionEntry) invokeLocalAction(context moleculer.BrokerContext) chan moleculer.Payload {
	result := make(chan moleculer.Payload)

	logger := context.Logger().WithField("actionCatalog", "invokeLocalAction")
	logger.Debug("Before Invoking action: ", context.ActionName())

	go func() {
		defer catchActionError(context, logger, result)
		handler := actionEntry.action.Handler()
		actionResult := handler(context.(moleculer.Context), context.Payload())
		logger.Debug("local action invoked ! action: ", context.ActionName(),
			" results: ", actionResult)
		result <- payload.Create(actionResult)
	}()

	return result
}

func (actionEntry ActionEntry) TargetNodeID() string {
	return actionEntry.targetNodeID
}

func (actionEntry ActionEntry) IsLocal() bool {
	return actionEntry.isLocal
}

// Add a new action to the catalog.
func (actionCatalog *ActionCatalog) Add(nodeID string, action service.Action, local bool) {
	entry := ActionEntry{nodeID, &action, local}
	name := action.FullName()
	actions := actionCatalog.actionsByName
	actions[name] = append(actions[name], entry)
}

func (actionCatalog *ActionCatalog) Update(nodeID string, fullname string, updates map[string]interface{}) {
	//TODO .. the only thing that can be udpated is the Action Schema (validation) and that does not exist yet
}

// RemoveByNode remove actions for the given nodeID.
func (actionCatalog *ActionCatalog) RemoveByNode(nodeID string) {
	for name, actions := range actionCatalog.actionsByName {
		var actionsToKeep []ActionEntry
		for _, action := range actions {
			if action.targetNodeID != nodeID {
				actionsToKeep = append(actionsToKeep, action)
			}
		}
		actionCatalog.actionsByName[name] = actionsToKeep
	}
}

func (actionCatalog *ActionCatalog) Remove(nodeID string, name string) {
	var newList []ActionEntry
	actions := actionCatalog.actionsByName
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
	actions := actionCatalog.actionsByName[actionName]
	for _, action := range actions {
		if action.targetNodeID == nodeID {
			return &action
		}
	}
	return nil
}

// Next find all actions registered in this node and use the strategy to select and return the best one to be called.
func (actionCatalog *ActionCatalog) Next(actionName string, strategy strategy.Strategy) *ActionEntry {
	actions := actionCatalog.actionsByName[actionName]
	nodes := make([]string, len(actions))
	for index, action := range actions {
		nodes[index] = action.targetNodeID
	}
	return actionCatalog.NextFromNode(actionName, strategy.SelectTargetNode(nodes))
}

func (actionCatalog *ActionCatalog) Size() int {
	return len(actionCatalog.actionsByName)
}
