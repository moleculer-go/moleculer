package registry

import (
	"sync"
	"time"

	"github.com/moleculer-go/moleculer"
	log "github.com/sirupsen/logrus"
)

// NodeCatalog catalog of nodes
type NodeCatalog struct {
	nodes  sync.Map
	logger *log.Entry
}

// CreateNodesCatalog create a node catalog
func CreateNodesCatalog(logger *log.Entry) *NodeCatalog {
	return &NodeCatalog{sync.Map{}, logger}
}

// HeartBeat delegate the heart beat to the node in question payload.sender
func (catalog *NodeCatalog) HeartBeat(heartbeat map[string]interface{}) bool {
	sender := heartbeat["sender"].(string)
	node, nodeExists := catalog.nodes.Load(sender)

	if nodeExists && (node.(moleculer.Node)).IsAvailable() {
		(node.(moleculer.Node)).HeartBeat(heartbeat)
		return true
	}
	return false
}

func (catalog *NodeCatalog) ForEachNode(forEAchFunc moleculer.ForEachNodeFunc) {
	catalog.nodes.Range(func(key, value interface{}) bool {
		return forEAchFunc(value.(moleculer.Node))
	})
}

func (catalog *NodeCatalog) list() []moleculer.Node {
	var result []moleculer.Node
	catalog.nodes.Range(func(key, value interface{}) bool {
		node := value.(moleculer.Node)
		result = append(result, node)
		return true
	})
	return result
}

// expiredNodes check nodes with  heartbeat expired based on the timeout parameter
func (catalog *NodeCatalog) expiredNodes(timeout time.Duration) []moleculer.Node {
	var result []moleculer.Node
	catalog.nodes.Range(func(key, value interface{}) bool {
		node := value.(moleculer.Node)
		if node.IsExpired(timeout) {
			result = append(result, node)
		}
		return true
	})
	return result
}

// findNode : return a Node instance from the catalog
func (catalog *NodeCatalog) findNode(nodeID string) (moleculer.Node, bool) {
	node, exists := catalog.nodes.Load(nodeID)
	if exists {
		return node.(moleculer.Node), true
	} else {
		return nil, false
	}
}

// removeNode : remove a node from the catalog
func (catalog *NodeCatalog) removeNode(nodeID string) {
	catalog.nodes.Delete(nodeID)
}

// Info : process info received about a NODE. It can be new, update to existing
func (catalog *NodeCatalog) Add(node moleculer.Node) {
	catalog.nodes.Store(node.GetID(), node)
}

// Info : process info received about a NODE. It can be new, update to existing
func (catalog *NodeCatalog) Info(info map[string]interface{}) (bool, bool, []map[string]interface{}) {
	sender := info["sender"].(string)
	node, exists := catalog.findNode(sender)
	var reconnected bool
	removedServices := []map[string]interface{}{}
	if exists {
		reconnected, removedServices = node.Update(sender, info)
	} else {
		node := CreateNode(sender, false, catalog.logger.WithField("remote-node", sender))
		node.Update(sender, info)
		catalog.Add(node)
	}
	return exists, reconnected, removedServices
}
