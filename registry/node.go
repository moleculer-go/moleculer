package registry

import (
	"fmt"
	"net"
	"time"

	. "github.com/moleculer-go/moleculer/common"
	"github.com/moleculer-go/moleculer/version"
)

type NodeInfo struct {
	id                string
	sequence          int64
	ipList            []string
	hostname          string
	client            map[string]interface{}
	services          []map[string]interface{}
	rawInfo           map[string]interface{}
	self              *NodeInfo
	isAvailable       bool
	cpu               int64
	cpuSequence       int64
	lastHeartBeatTime int64
	offlineSince      int64
	isLocal           bool
}

func discoverIpList() []string {
	var result []string
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return make([]string, 0)
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				result = append(result, ipnet.IP.String())
			}
		}
	}
	return result
}

func discoverHostname() string {
	hostname := ""
	return hostname
}

func CreateNode(id string) Node {
	ipList := discoverIpList()
	hostname := discoverHostname()
	node := NodeInfo{
		id: id,
		client: map[string]interface{}{
			"type":        "moleculer-go",
			"version":     version.Moleculer(),
			"langVersion": version.Go(),
		},
		ipList:   ipList,
		hostname: hostname,
	}
	node.self = &node
	return node
}

func (node NodeInfo) Update(info map[string]interface{}) bool {
	return node.self.updateImpl(info)
}

func interfaceToString(list []interface{}) []string {
	result := make([]string, len(list))
	for index, item := range list {
		result[index] = item.(string)
	}
	return result
}

func (node *NodeInfo) updateImpl(info map[string]interface{}) bool {
	id := info["id"]
	if id != node.id {
		panic(fmt.Errorf("Node.Update() - the id received : %s does not match this node.id : %s", id, node.id))
	}

	reconnected := !node.isAvailable

	node.isAvailable = true
	node.lastHeartBeatTime = time.Now().Unix()
	node.offlineSince = 0

	node.rawInfo = info
	node.ipList = interfaceToString(info["ipList"].([]interface{}))
	node.hostname = info["hostname"].(string)
	node.client = info["client"].(map[string]interface{})

	items := info["services"].([]interface{})
	services := make([]map[string]interface{}, len(items))
	for index, item := range items {
		services[index] = item.(map[string]interface{})
	}
	node.services = services

	node.sequence = int64(info["seq"].(float64))
	node.cpu = int64(info["cpu"].(float64))
	node.cpuSequence = int64(info["cpuSeq"].(float64))

	return reconnected
}

func (node *NodeInfo) exportAsMapImpl() map[string]interface{} {
	resultMap := make(map[string]interface{})
	resultMap["id"] = node.id
	resultMap["services"] = node.services
	resultMap["ipList"] = node.ipList
	resultMap["hostname"] = node.hostname
	resultMap["client"] = node.client
	resultMap["seq"] = node.sequence
	resultMap["cpu"] = node.cpu
	resultMap["cpuSeq"] = node.cpuSequence
	return resultMap
}

// ExportAsMap export the node info as a map
// this map is used to publish the node info to other nodes.
func (node NodeInfo) ExportAsMap() map[string]interface{} {
	return node.self.exportAsMapImpl()
}

func (node NodeInfo) GetID() string {
	return node.id
}
func (node NodeInfo) IsExpired(timeout time.Duration) bool {
	return node.self.isExpiredImpl(timeout)
}

func (node *NodeInfo) isExpiredImpl(timeout time.Duration) bool {
	if !node.isAvailable || node.isLocal {
		return false
	}
	diff := time.Now().Unix() - node.lastHeartBeatTime
	return diff > int64(timeout.Seconds())
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

func (node *NodeInfo) addServicesImpl(service map[string]interface{}) {
	node.services = append(node.services, service)
}

func (node NodeInfo) AddServices(service map[string]interface{}) {
	node.self.addServicesImpl(service)
}

func (node NodeInfo) IsAvailable() bool {
	return node.isAvailable
}

func (node NodeInfo) IsLocal() bool {
	return node.isLocal
}

func (node NodeInfo) IncreaseSequence() {
	node.sequence++
}

//check if required
// func (node *NodeInfo) AddService(service *Service) {

// }
