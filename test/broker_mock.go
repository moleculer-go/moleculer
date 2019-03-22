package test

import (
	bus "github.com/moleculer-go/goemitter"
	"github.com/moleculer-go/moleculer"
	log "github.com/sirupsen/logrus"
)

var logger = log.WithField("unit test", "<root>")

func Logger(name string, value string) *log.Entry {
	return logger.WithField(name, value)
}

func DelegatesWithIdAndConfig(nodeID string, config moleculer.Config) moleculer.BrokerDelegates {
	localBus := bus.Construct()
	localNode := NodeMock{ID: nodeID}
	broker := moleculer.BrokerDelegates{
		LocalNode: func() moleculer.Node {
			return &localNode
		},
		Logger: Logger,
		Bus: func() *bus.Emitter {
			return localBus
		},
		Config: config,
	}
	return broker
}
