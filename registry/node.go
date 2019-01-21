package registry

import (
	. "github.com/moleculer-go/moleculer/common"
	. "github.com/moleculer-go/moleculer/service"
)

type NodeInfo struct {
	id       string
	sequence int
	ipList   []string
	hostname string
	client   map[string]string
	config   map[string]interface{}
	port     string

	rawInfo map[string]interface{}
}

func CreateNode(id string) Node {
	return &NodeInfo{id: id}
}

func (node *NodeInfo) GetID() string {
	return node.id
}

func (node *NodeInfo) IncreaseSequence() {
	node.sequence++
}

//check if required
func (node *NodeInfo) AddService(service *Service) {

}
