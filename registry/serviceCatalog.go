package registry

import (
	"fmt"

	. "github.com/moleculer-go/moleculer/service"
)

type ServiceEntry struct {
	service *Service
	node    *Node //not sure is required
}

type ServiceCatalog struct {
	services map[string]*ServiceEntry
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

// Add : add a service to the catalog.
func (serviceCatalog *ServiceCatalog) Add(node *Node, service *Service) {
	nodeID := node.id
	key := createKey(service.GetName(), service.GetVersion(), nodeID)
	serviceCatalog.services[key] = &ServiceEntry{service, node}
}
