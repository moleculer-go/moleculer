package registry

import (
	"time"

	. "github.com/moleculer-go/moleculer/common"
	//. "github.com/moleculer-go/moleculer/service"
)

type NodeInfo struct {
	id                string
	sequence          int
	ipList            []string
	hostname          string
	client            map[string]string
	config            map[string]interface{}
	port              string
	services          []map[string]interface{}
	self              *NodeInfo
	isAvailable       bool
	cpu               int64
	cpuSequence       int64
	lastHeartBeatTime int64
	offlineSince      int64
}

func CreateNode(id string) Node {
	node := NodeInfo{id: id}
	node.self = &node
	return node
}

func (node NodeInfo) GetID() string {
	return node.id
}

func (node *NodeInfo) heartBeatImp(heartbeat map[string]interface{}) {
	if !node.isAvailable {
		node.isAvailable = true
		node.offlineSince = 0
	}
	node.cpu = heartbeat["cpu"].(int64)
	node.cpuSequence = heartbeat["cpuSeq"].(int64)
	node.lastHeartBeatTime = time.Now().Unix()
}

func (node NodeInfo) HeartBeat(heartbeat map[string]interface{}) {
	node.self.heartBeatImp(heartbeat)
}

func (node NodeInfo) IsAvailable() bool {
	return node.isAvailable
}

//TODO populate the fields services, cliente, hostname and etc...
func (node NodeInfo) ExportAsMap() map[string]interface{} {
	resultMap := make(map[string]interface{})
	resultMap["services"] = node.services
	resultMap["ipList"] = node.ipList
	resultMap["hostname"] = node.hostname
	resultMap["client"] = node.client
	resultMap["config"] = node.config
	resultMap["seq"] = node.sequence
	resultMap["cpu"] = node.cpu
	resultMap["cpuSeq"] = node.cpuSequence
	return resultMap
}

func (node NodeInfo) IncreaseSequence() {
	node.sequence++
}

//check if required
// func (node *NodeInfo) AddService(service *Service) {

// }
