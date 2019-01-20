// common : contain common types
package common

import (
	. "github.com/moleculer-go/goemitter"
	log "github.com/sirupsen/logrus"
)

type getLoggerFunction func(name string) *log.Entry
type getLocalBusFunction func() *Emitter
type isStartedFunction func() bool

type BrokerInfo struct {
	NodeID      string
	Logger      *log.Entry
	GetLogger   getLoggerFunction
	GetLocalBus getLocalBusFunction
	IsStarted   isStartedFunction
}

type NodeInfo struct {
	ID string
}

type ServiceInfo struct {
	Name string
}

type ActionEventInfo struct {
	Name string
}
