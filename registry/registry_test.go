package registry_test

import (
	. "github.com/moleculer-go/goemitter"
	. "github.com/moleculer-go/moleculer/common"
	. "github.com/moleculer-go/moleculer/registry"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
)

var logger = log.WithField("Unit Test", true)

func getLogger(name string) *log.Entry {
	return logger.WithField(name, true)
}

var localBus = CreateEmitter()

func getLocalBus() *Emitter {
	return localBus
}

var _ = Describe("Registry", func() {

	Describe("Create a Registry", func() {
		broker := &BrokerInfo{Logger: logger, GetLogger: getLogger, GetLocalBus: getLocalBus}
		It("Should create a registry and ...", func() {

			registry := CreateRegistry(broker)

			Expect(registry).Should(Not(BeNil()))

		})

	})

})
