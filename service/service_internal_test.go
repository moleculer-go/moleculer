package service

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
)

var logger = log.WithField("Unit Test", true)

var _ = Describe("MergeActions", func() {

	serviceSchemaOriginal := ServiceSchema{"earth", "0.2", map[string]interface{}(nil), map[string]interface{}(nil), nil, []ServiceAction(nil), []ServiceEvent(nil), nil, nil, nil}
	serviceSchemaMixin := ServiceSchema{"earth", "0.2", map[string]interface{}(nil), map[string]interface{}(nil), nil, []ServiceAction(nil), []ServiceEvent(nil), nil, nil, nil}

	It("Should merge and overwrite existing actions", func() {

		mergedServiceAction := mergeActions(serviceSchemaOriginal, &serviceSchemaMixin)

		Expect(mergedServiceAction.Actions).Should(Equal(serviceSchemaMixin.Actions))
		Expect(mergedServiceAction.Actions).Should(Not(Equal(serviceSchemaOriginal.Actions)))
	})

	It("Should merge and overwrite existing events", func() {

		mergedServiceEvent := mergeEvents(serviceSchemaOriginal, &serviceSchemaMixin)

		Expect(mergedServiceEvent.Events).Should(Equal(serviceSchemaMixin.Events))
		Expect(mergedServiceEvent.Events).Should(Not(Equal(serviceSchemaOriginal.Events)))
	})

})
