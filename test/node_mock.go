package test

import "time"

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
