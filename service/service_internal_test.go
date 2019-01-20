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

		mergedService := mergeActions(serviceSchemaOriginal, &serviceSchemaMixin)

		Expect(mergedService.Actions).Should(Equal(serviceSchemaMixin.Actions))
		Expect(mergedService.Actions).Should(Not(Equal(serviceSchemaOriginal.Actions)))
	})

})
