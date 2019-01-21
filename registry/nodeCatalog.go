package registry

import (
	. "github.com/moleculer-go/moleculer/common"
)

type NodeCatalog struct {
	nodes []*Node
}

func CreateNodesCatalog() *NodeCatalog {
	return &NodeCatalog{}
}

// discoverNodeID - should return the node id for this machine
// TODO: Check moleculer JS algo for this..
func DiscoverNodeID() string {
	//TODO
	return "fixed-node-value"
}
