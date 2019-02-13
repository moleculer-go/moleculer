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
	actions sync.Map
}

func CreateActionCatalog() *ActionCatalog {
	actions := sync.Map{}
	return &ActionCatalog{actions: actions}
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

	list, exists := actionCatalog.actions.Load(name)
	if !exists {
		list = []ActionEntry{entry}
	} else {
		list = append(list.([]ActionEntry), entry)
	}
	actionCatalog.actions.Store(name, list)
}

func (actionCatalog *ActionCatalog) Update(nodeID string, fullname string, updates map[string]interface{}) {
	//TODO .. the only thing that can be udpated is the Action Schema (validation) and that does not exist yet
}

// RemoveByNode remove actions for the given nodeID.
func (actionCatalog *ActionCatalog) RemoveByNode(nodeID string) {
	actionCatalog.actions.Range(func(key, value interface{}) bool {
		name := key.(string)
		actions := value.([]ActionEntry)
		var toKeep []ActionEntry
		for _, action := range actions {
			if action.targetNodeID != nodeID {
				toKeep = append(toKeep, action)
			}
		}
		actionCatalog.actions.Store(name, toKeep)
		return true
	})
}

func (actionCatalog *ActionCatalog) Remove(nodeID string, name string) {
	value, exists := actionCatalog.actions.Load(name)
	if !exists {
		return
	}
	actions := value.([]ActionEntry)
	var toKeep []ActionEntry
	for _, action := range actions {
		if action.targetNodeID != nodeID {
			toKeep = append(toKeep, action)
		}
	}
	actionCatalog.actions.Store(name, toKeep)
}

func (actionCatalog *ActionCatalog) NextFromNode(actionName string, nodeID string) *ActionEntry {
	list, exists := actionCatalog.actions.Load(actionName)
	if !exists {
		return nil
	}
	actions := list.([]ActionEntry)
	for _, action := range actions {
		if action.targetNodeID == nodeID {
			return &action
		}
	}
	return nil
}

// Next find all actions registered in this node and use the strategy to select and return the best one to be called.
func (actionCatalog *ActionCatalog) Next(actionName string, stg strategy.Strategy) *ActionEntry {
	list, exists := actionCatalog.actions.Load(actionName)
	if !exists {
		return nil
	}
	actions := list.([]ActionEntry)
	nodes := make([]strategy.Selector, len(actions))
	for index, action := range actions {
		nodes[index] = action
	}
	if selected := stg.Select(nodes); selected != nil {
		entry := (*selected).(ActionEntry)
		return &entry
	}
	return nil
}
