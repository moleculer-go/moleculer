package test

import (
	"time"

	"github.com/moleculer-go/moleculer"
)

type NodeMock struct {
	UpdateResult          bool
	ID                    string
	IncreaseSequenceCalls int
	HeartBeatCalls        int
	ExportAsMapResult     map[string]interface{}
	IsAvailableResult     bool
	IsExpiredResult       bool
	PublishCalls          int

	// Additional fields for missing methods
	Host        string
	IpList      []string
	Port        int
	UdpAddress  string
	Sequence    int64
	CpuSequence int64
	Cpu         int64
	IsLocalFlag bool
	Hostname    string
}

func (node *NodeMock) Update(id string, info map[string]interface{}) (bool, []map[string]interface{}) {
	return node.UpdateResult, []map[string]interface{}{}
}

func (node *NodeMock) Unavailable() {
	node.IsAvailableResult = false
}
func (node *NodeMock) Available() {
	node.IsAvailableResult = true
}

func (node *NodeMock) GetID() string {
	return node.ID
}

func (node *NodeMock) IncreaseSequence() {
	node.IncreaseSequenceCalls++
}

func (node *NodeMock) ExportAsMap() map[string]interface{} {
	return node.ExportAsMapResult
}
func (node *NodeMock) IsAvailable() bool {
	return node.IsAvailableResult
}
func (node *NodeMock) HeartBeat(heartbeat map[string]interface{}) {
	node.HeartBeatCalls++
}
func (node *NodeMock) IsExpired(timeout time.Duration) bool {
	return node.IsExpiredResult
}
func (node *NodeMock) Publish(service map[string]interface{}) {
	node.PublishCalls++
}

// Missing methods from Node interface
func (node *NodeMock) GetHost() string {
	return node.Host
}

func (node *NodeMock) GetIpList() []string {
	return node.IpList
}

func (node *NodeMock) GetPort() int {
	return node.Port
}

func (node *NodeMock) GetUdpAddress() string {
	return node.UdpAddress
}

func (node *NodeMock) GetSequence() int64 {
	return node.Sequence
}

func (node *NodeMock) GetCpuSequence() int64 {
	return node.CpuSequence
}

func (node *NodeMock) GetCpu() int64 {
	return node.Cpu
}

func (node *NodeMock) IsLocal() bool {
	return node.IsLocalFlag
}

func (node *NodeMock) UpdateMetrics() {
	// Mock implementation - no-op
}

func (node *NodeMock) GetHostname() string {
	return node.Hostname
}

func (node *NodeMock) UpdateInfo(info map[string]interface{}) []map[string]interface{} {
	// Mock implementation - return empty slice
	return []map[string]interface{}{}
}

// RegistryMock implements moleculer.Registry interface for testing
type RegistryMock struct {
	LocalNodeResult moleculer.Node
	Nodes           map[string]moleculer.Node
}

func (r *RegistryMock) GetNodeByID(nodeID string) moleculer.Node {
	if node, exists := r.Nodes[nodeID]; exists {
		return node
	}
	return nil
}

func (r *RegistryMock) AddOfflineNode(nodeID, hostname, ipAddress string, port int) moleculer.Node {
	node := &NodeMock{
		ID:       nodeID,
		Host:     ipAddress,
		Hostname: hostname,
		Port:     port,
	}
	if r.Nodes == nil {
		r.Nodes = make(map[string]moleculer.Node)
	}
	r.Nodes[nodeID] = node
	return node
}

func (r *RegistryMock) ForEachNode(fn moleculer.ForEachNodeFunc) {
	for _, node := range r.Nodes {
		if !fn(node) {
			break
		}
	}
}

func (r *RegistryMock) DisconnectNode(nodeID string) {
	delete(r.Nodes, nodeID)
}

func (r *RegistryMock) RemoteNodeInfoReceived(message moleculer.Payload) {
	// Mock implementation - no-op
}

func (r *RegistryMock) GetLocalNode() moleculer.Node {
	return r.LocalNodeResult
}

func (r *RegistryMock) GetNodeByAddress(host string) moleculer.Node {
	for _, node := range r.Nodes {
		if node.GetHost() == host {
			return node
		}
	}
	return nil
}
