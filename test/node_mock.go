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
	AddServiceCalls       int
}

func (node *NodeMock) Update(info map[string]interface{}) bool {
	return node.UpdateResult
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
func (node *NodeMock) AddService(service map[string]interface{}) {
	node.AddServiceCalls++
}
