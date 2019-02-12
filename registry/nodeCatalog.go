package registry

import (
	"sync"
	"time"

	"github.com/moleculer-go/moleculer"
)

// NodeCatalog catalog of nodes
type NodeCatalog struct {
	nodes sync.Map
}

// CreateNodesCatalog create a node catalog
func CreateNodesCatalog() *NodeCatalog {
	nodes := sync.Map{}
	return &NodeCatalog{nodes}
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

// checkRemoteNodes : check nodes with  heartbeat expired based on the timeout parameter
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
func (catalog *NodeCatalog) Info(info map[string]interface{}) (bool, bool) {
	sender := info["sender"].(string)
	node, exists := catalog.findNode(sender)
	var reconnected bool
	if exists {
		reconnected = node.Update(info)
	} else {
		newNode := CreateNode(sender)
		catalog.nodes.Store(sender, newNode)
	}
	return exists, reconnected
}
