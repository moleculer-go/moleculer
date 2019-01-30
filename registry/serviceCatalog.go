package registry

import (
	"fmt"

	. "github.com/moleculer-go/moleculer/service"
)

type ServiceCatalog struct {
	services map[string]*Service
}

func CreateServiceCatalog() *ServiceCatalog {
	services := make(map[string]*Service)
	return &ServiceCatalog{services}
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
func (serviceCatalog *ServiceCatalog) Get(name string, version string, nodeID string) *Service {
	key := createKey(name, version, nodeID)
	service := serviceCatalog.services[key]
	return service
}

// Add : add a service to the catalog.
func (serviceCatalog *ServiceCatalog) Add(nodeID string, service *Service) {
	key := createKey(service.GetName(), service.GetVersion(), nodeID)
	serviceCatalog.services[key] = service
}

func serviceActionExists(name string, actions []ServiceAction) bool {
	for _, action := range actions {
		if action.GetName() == name {
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
func (serviceCatalog *ServiceCatalog) updateActions(service map[string]interface{}, current *Service) ([]map[string]interface{}, []ServiceAction, []ServiceAction) {
	var updatedActions []map[string]interface{}
	var newActions, deletedActions []ServiceAction

	actions := service["actions"].([]interface{})
	for _, item := range actions {
		action := item.(map[string]interface{})
		name := action["name"].(string)
		if serviceActionExists(name, current.GetActions()) {
			updatedActions = append(updatedActions, action)
		} else {
			serviceAction := current.AddActionMap(action)
			newActions = append(newActions, *serviceAction)
		}
	}
	for _, action := range current.GetActions() {
		name := action.GetName()
		if !actionMapExists(name, actions) {
			deletedActions = append(deletedActions, action)
			current.RemoveAction(name)
		}
	}
	return updatedActions, newActions, deletedActions
}

// updateRemote : update remote service info and return what actions are new, updated and deleted
func (serviceCatalog *ServiceCatalog) updateRemote(nodeID string, serviceInfo map[string]interface{}) ([]map[string]interface{}, []ServiceAction, []ServiceAction) {
	var updatedActions []map[string]interface{}
	var newActions, deletedActions []ServiceAction

	key := createKey(serviceInfo["name"].(string), serviceInfo["version"].(string), nodeID)
	current, serviceExists := serviceCatalog.services[key]

	if serviceExists {
		current.UpdateFromMap(serviceInfo)
		return serviceCatalog.updateActions(serviceInfo, current)
	}

	service := CreateServiceFromMap(serviceInfo)
	serviceCatalog.Add(nodeID, service)

	newActions = service.GetActions()
	updatedActions = make([]map[string]interface{}, 0)
	deletedActions = make([]ServiceAction, 0)
	return updatedActions, newActions, deletedActions

}
