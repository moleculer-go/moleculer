package registry_test

import (
	. "github.com/moleculer-go/goemitter"
	. "github.com/moleculer-go/moleculer/common"
	log "github.com/sirupsen/logrus"
)

var logger = log.WithField("Unit Test", "Registry Pkg")

func CreateLogger(name string) *log.Entry {
	return logger.WithField(name, "<Unit Test>")
}

func CreateBroker() *BrokerInfo {
	localBus := CreateEmitter()
	broker := &BrokerInfo{Logger: logger, GetLogger: CreateLogger, GetLocalBus: func() *Emitter { return localBus }}
	return broker
}
