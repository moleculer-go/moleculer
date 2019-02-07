package registry

import (
	"fmt"
	"sync"

	"github.com/moleculer-go/moleculer/service"
)

type ServiceCatalog struct {
	services       map[string]*service.Service
	servicesByNode map[string][]string
	mutex          *sync.Mutex
}

func CreateServiceCatalog() *ServiceCatalog {
	services := make(map[string]*service.Service)
	servicesByNode := make(map[string][]string)
	mutex := &sync.Mutex{}
	return &ServiceCatalog{services, servicesByNode, mutex}
}

func (serviceCatalog *ServiceCatalog) getLocalNodeServices() []map[string]interface{} {
	return nil //TODO
}

// createKey creates the catalogy key used in the map
func createKey(name string, version string, nodeID string) string {
	return fmt.Sprintf("%s:%s:%s", nodeID, name, version)
}

// Has : Checks if a service for the given name, version and nodeID already exists in the catalog.
func (serviceCatalog *ServiceCatalog) Has(name string, version string, nodeID string) bool {
	key := createKey(name, version, nodeID)
	_, exists := serviceCatalog.services[key]
	return exists
}

// Get : Return the service for the given name, version and nodeID if it exists in the catalog.
func (serviceCatalog *ServiceCatalog) Get(name string, version string, nodeID string) *service.Service {
	key := createKey(name, version, nodeID)
	service := serviceCatalog.services[key]
	return service
}

// RemoveByNode remove services for the given nodeID.
func (serviceCatalog *ServiceCatalog) RemoveByNode(nodeID string) {
	keys, exists := serviceCatalog.servicesByNode[nodeID]
	if exists {
		for _, key := range keys {
			delete(serviceCatalog.services, key)
		}
		delete(serviceCatalog.servicesByNode, nodeID)
	}
}

// Add : add a service to the catalog.
func (serviceCatalog *ServiceCatalog) Add(nodeID string, service *service.Service) {
	serviceCatalog.mutex.Lock()
	defer serviceCatalog.mutex.Unlock()

	key := createKey(service.Name(), service.Version(), nodeID)
	serviceCatalog.services[key] = service
	if serviceCatalog.servicesByNode[nodeID] == nil {
		serviceCatalog.servicesByNode[nodeID] = []string{key}
	} else {
		serviceCatalog.servicesByNode[nodeID] = append(serviceCatalog.servicesByNode[nodeID], key)
	}
}

func serviceActionExists(name string, actions []service.Action) bool {
	for _, action := range actions {
		if action.Name() == name {
			return true
		}
	}
	return false
}

func actionMapExists(name string, actions []interface{}) bool {
	for _, item := range actions {
		action := item.(map[string]interface{})
		if action["name"].(string) == name {
			return true
		}
	}
	return false
}

// updateActions takes the remote service definition and the current service definition and calculates what actions are new, updated or removed.
// add new actions to the service and return new, updated and deleted actions.
func (serviceCatalog *ServiceCatalog) updateActions(serviceMap map[string]interface{}, current *service.Service) ([]map[string]interface{}, []service.Action, []service.Action) {
	var updatedActions []map[string]interface{}
	var newActions, deletedActions []service.Action

	actions := serviceMap["actions"].([]interface{})
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
		if !actionMapExists(name, actions) {
			deletedActions = append(deletedActions, action)
			current.RemoveAction(name)
		}
	}
	return updatedActions, newActions, deletedActions
}

// updateRemote : update remote service info and return what actions are new, updated and deleted
func (serviceCatalog *ServiceCatalog) updateRemote(nodeID string, serviceInfo map[string]interface{}) ([]map[string]interface{}, []service.Action, []service.Action) {
	var updatedActions []map[string]interface{}
	var newActions, deletedActions []service.Action

	key := createKey(serviceInfo["name"].(string), serviceInfo["version"].(string), nodeID)
	current, serviceExists := serviceCatalog.services[key]

	if serviceExists {
		current.UpdateFromMap(serviceInfo)
		return serviceCatalog.updateActions(serviceInfo, current)
	}

	serviceInstance := service.CreateServiceFromMap(serviceInfo)
	serviceCatalog.Add(nodeID, serviceInstance)

	newActions = serviceInstance.Actions()
	updatedActions = make([]map[string]interface{}, 0)
	deletedActions = make([]service.Action, 0)
	return updatedActions, newActions, deletedActions

}
