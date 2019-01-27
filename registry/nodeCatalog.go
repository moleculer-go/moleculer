package registry

import (
	. "github.com/moleculer-go/moleculer/common"
)

type NodeCatalog struct {
	nodes     []*Node
	nodesByID map[string]*Node
}

func CreateNodesCatalog() *NodeCatalog {
	nodesByID := make(map[string]*Node)
	return &NodeCatalog{nodesByID: nodesByID}
}

// HeartBeat delegate the heart beat to the node in question payload.sender
func (catalog *NodeCatalog) HeartBeat(heartbeat map[string]interface{}) bool {
	sender := heartbeat["sender"].(string)
	node, nodeExists := catalog.nodesByID[sender]
	if nodeExists && (*node).IsAvailable() {
		(*node).HeartBeat(heartbeat)
		return true
	}
	return false
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
