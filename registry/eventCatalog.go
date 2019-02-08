package registry

import (
	"sync"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/service"
	"github.com/moleculer-go/moleculer/strategy"
	log "github.com/sirupsen/logrus"
)

type EventEntry struct {
	targetNodeID string
	event        *service.Event
	isLocal      bool
}

func (eventEntry *EventEntry) TargetNodeID() string {
	return eventEntry.targetNodeID
}

func (eventEntry *EventEntry) IsLocal() bool {
	return eventEntry.isLocal
}

func catchEventError(context moleculer.BrokerContext, logger *log.Entry) {
	if err := recover(); err != nil {
		logger.Error("local event failed :( event: ", context.EventName(), " error: ", err)
	}
}

func (eventEntry *EventEntry) emitLocalEvent(context moleculer.BrokerContext) {
	logger := context.Logger().WithField("eventCatalog", "emitLocalEvent")
	logger.Debug("Before Invoking event: ", context.EventName())
	defer catchEventError(context, logger)
	handler := eventEntry.event.Handler()
	handler(context.(moleculer.Context), context.Payload())
	logger.Debug("local event invoked ! event: ", context.EventName())

}

type EventCatalog struct {
	events map[string][]EventEntry
	mutex  *sync.Mutex
}

func CreateEventCatalog() *EventCatalog {
	events := make(map[string][]EventEntry)
	mutex := &sync.Mutex{}
	return &EventCatalog{events: events, mutex: mutex}
}

// Add a new event to the catalog.
func (eventCatalog *EventCatalog) Add(nodeID string, event service.Event, local bool) {
	entry := EventEntry{nodeID, &event, local}
	name := event.Name()
	eventCatalog.events[name] = append(eventCatalog.events[name], entry)
}

func matchGroup(event service.Event, groups []string) bool {
	if groups == nil || len(groups) == 0 {
		return true
	}

	for _, group := range groups {
		if event.Group() == group {
			return true
		}
	}
}

// Next find all events registered in this node and use the strategy to select and return the best one to be called.
func (eventCatalog *EventCatalog) Next(name string, strategy strategy.Strategy, groups []string) *EventEntry {
	events := eventCatalog.events[name]
	nodeGroups := make(map[string]string)
	for _, entry := range events {
		if matchGroup(entry.event, groups) {
			nodeGroups[entry.event.Group()] = event.targetNodeID
		}
	}
	nodes := make([]string, len(nodeGroups))
	idx := 0
	for group, node := range nodeGroups {
		nodes[idx] = node
		idx++
	}
	return nil
	//return eventCatalog.NextFromNode(name, strategy.SelectTargetNode(nodes))
}
