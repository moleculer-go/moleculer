package transit_test

import (
	. "github.com/moleculer-go/goemitter"
	"github.com/moleculer-go/moleculer"

	"github.com/moleculer-go/moleculer/registry"
	log "github.com/sirupsen/logrus"
)

var logger = log.WithField("unit test pkg", "transit_test")

func CreateLogger(name string, value string) *log.Entry {
	return logger.WithField(name, value)
}

var localNode = registry.CreateNode("unit-test-node", true, logger)

func BrokerDelegates() *moleculer.BrokerDelegates {
	localBus := Construct()
	broker := &moleculer.BrokerDelegates{
		LocalNode: func() moleculer.Node {
			return localNode
		},
		Logger: CreateLogger,
		Bus: func() *Emitter {
			return localBus
		}}
	return broker
}
