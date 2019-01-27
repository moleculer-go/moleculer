package registry

import (
	"fmt"

	. "github.com/moleculer-go/moleculer/common"
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
func (serviceCatalog *ServiceCatalog) Add(node *Node, service *Service) {
	nodeID := (*node).GetID()
	key := createKey(service.GetName(), service.GetVersion(), nodeID)
	serviceCatalog.services[key] = service
}
