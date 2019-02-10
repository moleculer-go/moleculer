package registry

import (
	"sync"
	"time"

	"github.com/moleculer-go/moleculer"
)

// NodeCatalog catalog of nodes
type NodeCatalog struct {
	nodes map[string]moleculer.Node
	mutex *sync.Mutex
}

// CreateNodesCatalog create a node catalog
func CreateNodesCatalog() *NodeCatalog {
	nodes := make(map[string]moleculer.Node)
	mutex := &sync.Mutex{}
	return &NodeCatalog{nodes, mutex}
}

// HeartBeat delegate the heart beat to the node in question payload.sender
func (catalog *NodeCatalog) HeartBeat(heartbeat map[string]interface{}) bool {
	sender := heartbeat["sender"].(string)
	node, nodeExists := catalog.nodes[sender]
	if nodeExists && node.IsAvailable() {
		node.HeartBeat(heartbeat)
		return true
	}
	return false
}

// checkRemoteNodes : check nodes with  heartbeat expired based on the timeout parameter
func (catalog *NodeCatalog) expiredNodes(timeout time.Duration) []moleculer.Node {
	var result []moleculer.Node
	for _, node := range catalog.nodes {
		if node.IsExpired(timeout) {
			result = append(result, node)
		}
	}
	return result
}

// findNode : return a Node instance from the catalog
func (catalog *NodeCatalog) findNode(nodeID string) (moleculer.Node, bool) {
	catalog.mutex.Lock()
	defer catalog.mutex.Unlock()
	node, exists := catalog.nodes[nodeID]
	return node, exists
}

// removeNode : remove a node from the catalog
func (catalog *NodeCatalog) removeNode(nodeID string) {
	delete(catalog.nodes, nodeID)
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
		catalog.mutex.Lock()
		catalog.nodes[sender] = newNode
		catalog.mutex.Unlock()
	}
	return exists, reconnected
}
