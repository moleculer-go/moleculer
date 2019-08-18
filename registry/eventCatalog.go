package registry

import (
	"fmt"
	"runtime/debug"
	"sync"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/service"
	"github.com/moleculer-go/moleculer/strategy"
	log "github.com/sirupsen/logrus"
)

type EventEntry struct {
	targetNodeID string
	service      *service.Service
	event        *service.Event
	isLocal      bool
}

func (eventEntry EventEntry) TargetNodeID() string {
	return eventEntry.targetNodeID
}

func (eventEntry *EventEntry) IsLocal() bool {
	return eventEntry.isLocal
}

func (eventEntry *EventEntry) String() string {
	return fmt.Sprint("EventEntry Node -> ", eventEntry.targetNodeID,
		" - Service: ", eventEntry.event.ServiceName(),
		" - Event Name: ", eventEntry.event.Name(),
		" - Group: ", eventEntry.event.Group())
}

func catchEventError(context moleculer.BrokerContext, logger *log.Entry) {
	if err := recover(); err != nil {
		logger.Error("Event handler failed :( event: ", context.EventName(), " error: ", err, "\n[Stack Trace]: ", string(debug.Stack()))
	}
}

func (eventEntry *EventEntry) emitLocalEvent(context moleculer.BrokerContext) {
	logger := context.Logger().WithField("eventCatalog", "emitLocalEvent")
	logger.Debug("Invoking local event: ", context.EventName())
	defer catchEventError(context, logger)
	handler := eventEntry.event.Handler()
	handler(context.(moleculer.Context), context.Payload())
	logger.Trace("After invoking local event: ", context.EventName())
}

type EventCatalog struct {
	events sync.Map
	logger *log.Entry
}

func CreateEventCatalog(logger *log.Entry) *EventCatalog {
	events := sync.Map{}
	return &EventCatalog{events: events, logger: logger}
}

// Add a new event to the catalog.
func (eventCatalog *EventCatalog) Add(event service.Event, service *service.Service, local bool) {
	entry := EventEntry{service.NodeID(), service, &event, local}
	name := event.Name()
	eventCatalog.logger.Debug("Add event name: ", name, " serviceName: ", event.ServiceName())
	list, exists := eventCatalog.events.Load(name)
	if !exists {
		list = []EventEntry{entry}
	} else {
		list = append(list.([]EventEntry), entry)
	}
	eventCatalog.events.Store(name, list)
}

func (eventCatalog *EventCatalog) Update(nodeID string, name string, updates map[string]interface{}) {
	//TODO .. the only thing that can be udpated is the Event Schema (validation) and that does not exist yet
}

func (eventCatalog *EventCatalog) listByName() map[string][]EventEntry {
	result := make(map[string][]EventEntry)
	eventCatalog.events.Range(func(key, value interface{}) bool {
		result[key.(string)] = value.([]EventEntry)
		return true
	})
	return result
}

func (eventCatalog *EventCatalog) Remove(nodeID string, name string) {
	removed := 0
	list, exists := eventCatalog.events.Load(name)
	if !exists {
		return
	}
	var newList []EventEntry
	for _, event := range list.([]EventEntry) {
		if event.targetNodeID != nodeID {
			newList = append(newList, event)
		} else {
			removed++
		}
	}
	eventCatalog.events.Store(name, newList)
}

// RemoveByNode remove events for the given nodeID.
func (eventCatalog *EventCatalog) RemoveByNode(nodeID string) {
	removed := 0
	eventCatalog.events.Range(func(key, value interface{}) bool {
		name := key.(string)
		events := value.([]EventEntry)
		var toKeep []EventEntry
		for _, event := range events {
			if event.targetNodeID != nodeID {
				toKeep = append(toKeep, event)
			} else {
				removed++
			}
		}
		eventCatalog.events.Store(name, toKeep)
		return true
	})
}

func matchGroup(event *service.Event, groups []string) bool {
	if groups == nil || len(groups) == 0 {
		return true
	}
	for _, group := range groups {
		if event.Group() == group {
			return true
		}
	}
	return false
}

func findLocal(events []EventEntry) *EventEntry {
	for _, item := range events {
		if item.IsLocal() {
			return &item
		}
	}
	return nil
}

// Find find all events registered in this node and use the strategy to select and return the best one to be called.
func (eventCatalog *EventCatalog) Find(name string, groups []string, preferLocal bool, localOnly bool, stg strategy.Strategy) []*EventEntry {
	events, exists := eventCatalog.events.Load(name)
	if !exists {
		return make([]*EventEntry, 0)
	}
	eventCatalog.logger.Trace("event: ", name, " started: ", events)

	entryGroups := make(map[string][]EventEntry)
	for _, entry := range events.([]EventEntry) {
		if localOnly && !entry.isLocal {
			continue
		}
		if matchGroup(entry.event, groups) {
			entryGroups[entry.event.Group()] = append(entryGroups[entry.event.Group()], entry)
		}
	}
	var result []*EventEntry
	for _, entries := range entryGroups {
		if local := findLocal(entries); preferLocal && local != nil {
			eventCatalog.logger.Trace("event: ", name, " found local: ", local)
			result = append(result, local)
		} else if len(entries) == 1 {
			eventCatalog.logger.Debug("event: ", name, " found a single one :)  ", entries[0])
			result = append(result, &entries[0])
		} else if len(entries) > 1 {
			if stg == nil {
				eventCatalog.logger.Debug("event: ", name, " no strategy. return all entries: ", entries)
				for _, entry := range entries {
					result = append(result, &entry)
				}
			} else {
				eventCatalog.logger.Debug("event: ", name, "using strategy to load balance between options: ", entries)
				nodes := make([]strategy.Selector, len(entries))
				for index, entry := range entries {
					nodes[index] = &entry
				}
				if selected := stg.Select(nodes); selected != nil {
					entry := (*selected).(*EventEntry)
					result = append(result, entry)
				}
			}

		}
	}

	return result
}

func (eventCatalog *EventCatalog) list() []EventEntry {
	var result []EventEntry
	eventCatalog.events.Range(func(key, value interface{}) bool {
		entries := value.([]EventEntry)
		for _, item := range entries {
			result = append(result, item)
		}
		return true
	})
	return result
}
