package registry

import (
	"fmt"
	"os"
	"strings"

	"net"
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/version"
	log "github.com/sirupsen/logrus"
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
	logger            *log.Entry
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
	hostname, error := os.Hostname()
	if error != nil {
		return "unknown"
	}
	return hostname
}

func CreateNode(id string, local bool, logger *log.Entry) moleculer.Node {
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
		logger:   logger,
		isLocal:  local,
	}
	var result moleculer.Node = &node
	return result
}

//Unavailable mark the node as unavailable
func (node *Node) Unavailable() {
	node.isAvailable = false
}

func (node *Node) Update(info map[string]interface{}) bool {
	id := info["id"]
	if id == nil {
		id = info["sender"]
	}
	if id != node.id {
		panic(fmt.Errorf("Node.Update() - the id received : %s does not match this node.id : %s", id, node.id))
	}
	node.logger.Debug("node.Update() info: ", info)
	reconnected := !node.isAvailable

	node.isAvailable = true
	node.lastHeartBeatTime = time.Now().Unix()
	node.offlineSince = 0

	node.ipList = interfaceToString(info["ipList"].([]interface{}))
	node.hostname = info["hostname"].(string)
	node.client = info["client"].(map[string]interface{})

	item, ok := info["services"]
	items := make([]interface{}, 0)
	if ok {
		items = item.([]interface{})
	}
	services := make([]map[string]interface{}, len(items))
	for index, item := range items {
		services[index] = item.(map[string]interface{})
	}
	node.services = services
	node.logger.Debug("node.Update() node.services: ", node.services)

	node.sequence = int64(info["seq"].(float64))

	/*
		FIXME: moleculer-js won't send cpu, cpuSeq for discovery packet
		ref: https://github.com/moleculerjs/moleculer/blob/master/docs/PROTOCOL.md#info
	 */
	if cpu, ok := info["cpu"].(float64); ok {
		node.cpu = int64(cpu)
	}

	if cpuSequence, ok := info["cpuSeq"].(float64); ok {
		node.cpuSequence = int64(cpuSequence)
	}
	/* end of FIXME */

	return reconnected
}

func interfaceToString(list []interface{}) []string {
	result := make([]string, len(list))
	for index, item := range list {
		result[index] = item.(string)
	}
	return result
}

// removeInternalServices remove internal services from the list of services.
//checks the service name if it starts with $
func (node *Node) removeInternalServices(services []map[string]interface{}) []map[string]interface{} {
	result := make([]map[string]interface{}, 0)
	for _, item := range services {
		if !(strings.Index(item["name"].(string), "$") == 0) {
			result = append(result, item)
		}
	}
	return result
}

// ExportAsMap export the node info as a map
// this map is used to publish the node info to other nodes.
func (node *Node) ExportAsMap() map[string]interface{} {
	resultMap := make(map[string]interface{})
	resultMap["id"] = node.id

	// FIXME: node should return internal services as well
	//resultMap["services"] = node.removeInternalServices(node.services)
	resultMap["services"] = node.services

	resultMap["ipList"] = node.ipList
	resultMap["hostname"] = node.hostname
	resultMap["client"] = node.client
	resultMap["seq"] = node.sequence
	resultMap["cpu"] = node.cpu
	resultMap["cpuSeq"] = node.cpuSequence
	resultMap["available"] = node.IsAvailable()

	return resultMap
}

func (node *Node) GetID() string {
	return node.id
}
func (node *Node) IsExpired(timeout time.Duration) bool {
	if node.IsLocal() || !node.IsAvailable() {
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
	if cpu, ok := heartbeat["cpu"].(float64); ok {
		node.cpu = int64(cpu)
	}
	if cpuSeq, ok := heartbeat["cpuSeq"].(float64); ok {
		node.cpuSequence = int64(cpuSeq)
	}
	node.lastHeartBeatTime = time.Now().Unix()
}

func (node *Node) Publish(service map[string]interface{}) {
	node.services = append(node.services, service)
}

func (node *Node) IsAvailable() bool {
	return node.isLocal || node.isAvailable
}

func (node *Node) IsLocal() bool {
	return node.isLocal
}

func (node *Node) IncreaseSequence() {
	node.sequence++
}
