package registry_test

import (
	. "github.com/moleculer-go/goemitter"
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/registry"

	log "github.com/sirupsen/logrus"
)

var logger = log.WithField("unit test pkg", "registry_test")

func CreateLogger(name string, value string) *log.Entry {
	return logger.WithField(name, value)
}

func BrokerDelegates(nodeID string) moleculer.BrokerDelegates {
	localBus := Construct()
	localNode := registry.CreateNode(nodeID, logger)
	broker := moleculer.BrokerDelegates{
		LocalNode: func() moleculer.Node {
			return localNode
		},
		Logger: CreateLogger,
		Bus: func() *Emitter {
			return localBus
		}}
	return broker
}
