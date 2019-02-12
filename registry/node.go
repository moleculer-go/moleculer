package registry

import (
	"fmt"
	"net"
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/version"
)

type Node struct {
	id                string
	sequence          int64
	ipList            []string
	hostname          string
	client            map[string]interface{}
	services          []map[string]interface{}
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
	if result == nil {
		result = []string{"0.0.0.0"}
	}
	return result
}

func discoverHostname() string {
	hostname := ""
	return hostname
}

func CreateNode(id string) moleculer.Node {
	ipList := discoverIpList()
	hostname := discoverHostname()
	services := make([]map[string]interface{}, 0)
	node := Node{
		id: id,
		client: map[string]interface{}{
			"type":        "moleculer-go",
			"version":     version.Moleculer(),
			"langVersion": version.Go(),
		},
		ipList:   ipList,
		hostname: hostname,
		services: services,
	}
	var result moleculer.Node = &node
	return result
}

func (node *Node) Update(info map[string]interface{}) bool {
	id := info["id"]
	if id != node.id {
		panic(fmt.Errorf("Node.Update() - the id received : %s does not match this node.id : %s", id, node.id))
	}

	reconnected := !node.isAvailable

	node.isAvailable = true
	node.lastHeartBeatTime = time.Now().Unix()
	node.offlineSince = 0

	node.ipList = interfaceToString(info["ipList"].([]interface{}))
	node.hostname = info["hostname"].(string)
	node.client = info["client"].(map[string]interface{})

	item := info["services"]
	items := make([]interface{}, 0)
	if item != nil {
		items = item.([]interface{})
	}
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

func interfaceToString(list []interface{}) []string {
	result := make([]string, len(list))
	for index, item := range list {
		result[index] = item.(string)
	}
	return result
}

// ExportAsMap export the node info as a map
// this map is used to publish the node info to other nodes.
func (node *Node) ExportAsMap() map[string]interface{} {
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

func (node *Node) GetID() string {
	return node.id
}
func (node *Node) IsExpired(timeout time.Duration) bool {
	return node.isExpiredImpl(timeout)
}

func (node *Node) isExpiredImpl(timeout time.Duration) bool {
	if !node.isAvailable || node.isLocal {
		return false
	}
	diff := time.Now().Unix() - node.lastHeartBeatTime
	return diff > int64(timeout.Seconds())
}

func (node *Node) HeartBeat(heartbeat map[string]interface{}) {
	if !node.isAvailable {
		node.isAvailable = true
		node.offlineSince = 0
	}
	node.cpu = int64(heartbeat["cpu"].(float64))
	node.cpuSequence = int64(heartbeat["cpuSeq"].(float64))
	node.lastHeartBeatTime = time.Now().Unix()
}

func (node *Node) addServicesImpl(service map[string]interface{}) {
	node.services = append(node.services, service)
}

func (node *Node) AddService(service map[string]interface{}) {
	node.addServicesImpl(service)
}

func (node *Node) IsAvailable() bool {
	return node.isAvailable
}

func (node *Node) IsLocal() bool {
	return node.isLocal
}

func (node *Node) IncreaseSequence() {
	node.sequence++
}

//check if required
// func (node *Node) AddService(service *Service) {

// }
