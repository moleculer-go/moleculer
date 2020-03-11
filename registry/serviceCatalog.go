package registry

import (
	"fmt"
	"sync"

	"github.com/moleculer-go/moleculer/service"
	log "github.com/sirupsen/logrus"
)

type ServiceCatalog struct {
	services       sync.Map
	servicesByName sync.Map
	logger         *log.Entry
}

type ServiceEntry struct {
	service *service.Service
	nodeID  string
}

func CreateServiceCatalog(logger *log.Entry) *ServiceCatalog {
	services := sync.Map{}
	servicesByName := sync.Map{}
	return &ServiceCatalog{services, servicesByName, logger}
}

// createKey creates the catalogy key used in the map
func createKey(name, version, nodeID string) string {
	return fmt.Sprintf("%s:%s:%s", nodeID, name, version)
}

// Has : Checks if a service for the given name, version and nodeID already exists in the catalog.
func (serviceCatalog *ServiceCatalog) Find(name string, version string, nodeID string) bool {
	key := createKey(name, version, nodeID)
	_, exists := serviceCatalog.services.Load(key)
	return exists
}

func (serviceCatalog *ServiceCatalog) FindByName(name string) bool {
	_, exists := serviceCatalog.servicesByName.Load(name)
	return exists
}

// Get : Return the service for the given name, version and nodeID if it exists in the catalog.
func (serviceCatalog *ServiceCatalog) Get(name string, version string, nodeID string) *service.Service {
	key := createKey(name, version, nodeID)
	item, exists := serviceCatalog.services.Load(key)
	if exists {
		svc := item.(service.Service)
		return &svc
	}
	return nil
}

// listByName list all service entriesf grouped by name.
func (serviceCatalog *ServiceCatalog) listByName() map[string][]ServiceEntry {
	result := make(map[string][]ServiceEntry)
	serviceCatalog.services.Range(func(key, value interface{}) bool {
		serviceEntry := value.(ServiceEntry)
		name := serviceEntry.service.FullName()
		entries, exists := result[name]
		if exists {
			result[name] = append(entries, serviceEntry)
		} else {
			result[name] = []ServiceEntry{serviceEntry}
		}
		return true
	})
	return result
}

// list all services in the catalog.
// func (serviceCatalog *ServiceCatalog) list() []*service.Service {
// 	var result []*service.Service
// 	serviceCatalog.services.Range(func(key, value interface{}) bool {
// 		entry := value.(ServiceEntry)
// 		result = append(result, entry.service)
// 		return true
// 	})
// 	return result
// }

// RemoveByNode remove services for the given nodeID.
func (serviceCatalog *ServiceCatalog) RemoveByNode(nodeID string) []*service.Service {
	var removed []*service.Service
	serviceCatalog.logger.Debug("RemoveByNode() nodeID: ", nodeID)
	var keysRemove []string
	var namesRemove []string
	var fullNamesRemove []string
	serviceCatalog.services.Range(func(key, value interface{}) bool {
		service := value.(ServiceEntry)
		if service.nodeID == nodeID {
			service := value.(ServiceEntry)
			removed = append(removed, service.service)
			keysRemove = append(keysRemove, key.(string))
			namesRemove = append(namesRemove, service.service.Name())
			fullNamesRemove = append(fullNamesRemove, service.service.FullName())
		}
		return true
	})
	for _, key := range keysRemove {
		serviceCatalog.services.Delete(key)
	}
	for _, name := range namesRemove {
		value, exists := serviceCatalog.servicesByName.Load(name)
		if exists {
			counter := value.(int)
			counter = counter - 1
			if counter < 0 {
				counter = 0
			}
			serviceCatalog.servicesByName.Store(name, counter)
		}
	}
	for _, name := range fullNamesRemove {
		value, exists := serviceCatalog.servicesByName.Load(name)
		if exists {
			counter := value.(int)
			counter = counter - 1
			if counter < 0 {
				counter = 0
			}
			serviceCatalog.servicesByName.Store(name, counter)
		}
	}
	return removed
}

// Add : add a service to the catalog.
func (serviceCatalog *ServiceCatalog) Add(service *service.Service) {
	nodeID := service.NodeID()
	key := createKey(service.Name(), service.Version(), nodeID)
	serviceCatalog.services.Store(key, ServiceEntry{service, nodeID})
	value, exists := serviceCatalog.servicesByName.Load(service.FullName())
	if exists {
		serviceCatalog.servicesByName.Store(service.FullName(), value.(int)+1)
		serviceCatalog.servicesByName.Store(service.Name(), value.(int)+1)
	} else {
		serviceCatalog.servicesByName.Store(service.FullName(), 1)
		serviceCatalog.servicesByName.Store(service.Name(), 1)
	}
}

func serviceActionExists(name string, actions []service.Action) bool {
	for _, action := range actions {
		if action.FullName() == name {
			return true
		}
	}
	return false
}

func serviceEventExists(name string, events []service.Event) bool {
	for _, event := range events {
		if event.Name() == name {
			return true
		}
	}
	return false
}

// updateEvents takes the remote service definition and the current service definition and calculates what events are new, updated or removed.
// add new events to the service and return new, updated and deleted events.
func (serviceCatalog *ServiceCatalog) updateEvents(serviceMap map[string]interface{}, current *service.Service) ([]map[string]interface{}, []service.Event, []service.Event) {
	var updated []map[string]interface{}
	var newEvents, deletedEvents []service.Event

	events := serviceMap["events"].(map[string]interface{})
	for _, item := range events {
		event := item.(map[string]interface{})
		name := event["name"].(string)
		if serviceEventExists(name, current.Events()) {
			updated = append(updated, event)
		} else {
			serviceEvent := current.AddEventMap(event)
			newEvents = append(newEvents, *serviceEvent)
		}
	}
	for _, event := range current.Events() {
		name := event.Name()
		_, exists := events[name]
		if !exists {
			deletedEvents = append(deletedEvents, event)
			current.RemoveEvent(name)
		}
	}
	return updated, newEvents, deletedEvents
}

// updateActions takes the remote service definition and the current service definition and calculates what actions are new, updated or removed.
// add new actions to the service and return new, updated and deleted actions.
func (serviceCatalog *ServiceCatalog) updateActions(serviceMap map[string]interface{}, current *service.Service) ([]map[string]interface{}, []service.Action, []service.Action) {
	var updatedActions []map[string]interface{}
	var newActions, deletedActions []service.Action

	actions := serviceMap["actions"].(map[string]interface{})
	for _, item := range actions {
		action := item.(map[string]interface{})
		name := action["name"].(string)
		if serviceActionExists(name, current.Actions()) {
			updatedActions = append(updatedActions, action)
		} else {
			serviceAction := current.AddActionMap(action)
			newActions = append(newActions, *serviceAction)
		}
	}
	for _, action := range current.Actions() {
		name := action.Name()
		_, exists := actions[name]
		if !exists {
			deletedActions = append(deletedActions, action)
			current.RemoveAction(name)
		}
	}
	return updatedActions, newActions, deletedActions
}

// updateRemote : update remote service info and return what actions are new, updated and deleted
func (serviceCatalog *ServiceCatalog) updateRemote(nodeID string, serviceInfo map[string]interface{}) (*service.Service, bool, []map[string]interface{}, []service.Action, []service.Action, []map[string]interface{}, []service.Event, []service.Event) {

	key := createKey(serviceInfo["name"].(string), service.ParseVersion(serviceInfo["version"]), nodeID)
	item, serviceExists := serviceCatalog.services.Load(key)

	if serviceExists {
		entry := item.(ServiceEntry)
		current := entry.service
		current.UpdateFromMap(serviceInfo)
		updatedActions, newActions, deletedActions := serviceCatalog.updateActions(serviceInfo, current)
		updatedEvents, newEvents, deletedEvents := serviceCatalog.updateEvents(serviceInfo, current)
		return current, false, updatedActions, newActions, deletedActions, updatedEvents, newEvents, deletedEvents
	}

	newService := service.CreateServiceFromMap(serviceInfo)
	newService.SetNodeID(nodeID)
	serviceCatalog.Add(newService)

	newActions := newService.Actions()
	updatedActions := make([]map[string]interface{}, 0)
	deletedActions := make([]service.Action, 0)

	newEvents := newService.Events()
	updatedEvents := make([]map[string]interface{}, 0)
	deletedEvents := make([]service.Event, 0)
	return newService, true, updatedActions, newActions, deletedActions, updatedEvents, newEvents, deletedEvents

}
