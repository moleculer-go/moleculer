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

func (node *Node) Update(id string, info map[string]interface{}) bool {
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

	node.services = filterServices(info)
	node.logger.Debug("node.Update() node.services: ", node.services)

	node.sequence = int64Field(info, "seq", 0)
	node.cpu = int64Field(info, "cpu", 0)
	node.cpuSequence = int64Field(info, "cpuSeq", 0)

	return reconnected
}

// filterServices return all services excluding local services (example: $node)
func filterServices(info map[string]interface{}) []map[string]interface{} {
	item, ok := info["services"]
	var items []interface{}
	if ok {
		items = item.([]interface{})
	}
	services := make([]map[string]interface{}, len(items))
	for index, item := range items {
		m := item.(map[string]interface{})
		if strings.Index(m["name"].(string), "$") == -1 {
			services[index] = m
		}
	}
	return services
}

func int64Field(values map[string]interface{}, field string, def int64) int64 {
	raw, exists := values[field]
	if !exists {
		return def
	}
	fv, valid := raw.(float64)
	if !valid {
		return def
	}
	return int64(fv)
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
	resultMap["services"] = node.services // node.removeInternalServices(node.services)
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
	node.cpu = int64Field(heartbeat, "cpu", 0)
	node.cpuSequence = int64Field(heartbeat, "cpuSeq", 0)
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
