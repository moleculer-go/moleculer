package registry

import (
	"time"

	. "github.com/moleculer-go/moleculer/common"
)

// NodeCatalog catalog of nodes
type NodeCatalog struct {
	nodes map[string]*Node
}

// CreateNodesCatalog create a node catalog
func CreateNodesCatalog() *NodeCatalog {
	nodes := make(map[string]*Node)
	return &NodeCatalog{nodes: nodes}
}

// HeartBeat delegate the heart beat to the node in question payload.sender
func (catalog *NodeCatalog) HeartBeat(heartbeat map[string]interface{}) bool {
	sender := heartbeat["sender"].(string)
	node, nodeExists := catalog.nodes[sender]
	if nodeExists && (*node).IsAvailable() {
		(*node).HeartBeat(heartbeat)
		return true
	}
	return false
}

// checkRemoteNodes : check nodes with  heartbeat expired based on the timeout parameter
func (catalog *NodeCatalog) expiredNodes(timeout time.Duration) []*Node {
	var result []*Node
	for _, node := range catalog.nodes {
		if (*node).IsExpired(timeout) {
			result = append(result, node)
		}
	}
	return result
}

// getNode : return a Node instance from the catalog
func (catalog *NodeCatalog) getNode(nodeID string) (*Node, bool) {
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
	node, exists := catalog.getNode(sender)
	var reconnected bool
	if exists {
		reconnected = (*node).Update(info)
	} else {
		newNode := CreateNode(sender)
		catalog.nodes[sender] = &newNode
	}
	return exists, reconnected
}

// DiscoverNodeID - should return the node id for this machine
func DiscoverNodeID() string {
	// TODO: Check moleculer JS algo for this..
	return "fixed-node-value"
}
