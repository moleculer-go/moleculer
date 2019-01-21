package registry

import (
	. "github.com/moleculer-go/moleculer/service"
)

type Node struct {
	id       string
	sequence int
	ipList   []string
	hostname string
	client   map[string]string
	config   map[string]interface{}
	port     string

	rawInfo map[string]interface{}
}

func CreateNode(id string) *Node {
	return &Node{id: id}
}

func (node *Node) GetID() string {
	return node.id
}

type NodeCatalog struct {
	localNode *Node
}

func (node *Node) AddService(service *Service) {

}
