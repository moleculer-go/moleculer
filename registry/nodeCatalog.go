package registry

import (
	"time"

	. "github.com/moleculer-go/moleculer/common"
)

type NodeCatalog struct {
	nodes map[string]*Node
}

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

// checkRemoteNodes : if any remote node lastHeartbeat
func (catalog *NodeCatalog) expiredNodes(timeout time.Duration) []*Node {
	var result []*Node
	for _, node := range catalog.nodes {
		if (*node).IsExpired(timeout) {
			result = append(result, node)
		}
	}
	return result
}

// disconnectImpl :
func (catalog *NodeCatalog) disconnectImpl(nodeID string) {
	node := catalog.nodes[nodeID]
	if node != nil && (*node).IsAvailable() {

	}
}

// removeNode :
func (catalog *NodeCatalog) removeNode(nodeID string) {
	delete(catalog.nodes, nodeID)
}

func (catalog *NodeCatalog) Disconnect(disconnect map[string]interface{}) {

}

func (catalog *NodeCatalog) Info(info map[string]interface{}) {

}

// discoverNodeID - should return the node id for this machine
// TODO: Check moleculer JS algo for this..
func DiscoverNodeID() string {
	//TODO
	return "fixed-node-value"
}
