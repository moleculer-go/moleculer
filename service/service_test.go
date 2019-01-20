package service_test

import (
	log "github.com/sirupsen/logrus"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/moleculer-go/moleculer/service"
)

var logger = log.WithField("Unit Test", true)

var _ = Describe("MergeActions", func() {

	serviceSchema := Service{"earth", "0.2", map[string]interface{}(nil), map[string]interface{}(nil), []ServiceAction(nil), []ServiceEvent(nil), nil, nil, nil}
	serviceSchemaOriginal := ServiceSchema{"earth", "0.2", map[string]interface{}(nil), map[string]interface{}(nil), nil, []ServiceAction(nil), []ServiceEvent(nil), nil, nil, nil}
	serviceSchemaMixin := ServiceSchema{"venus", "0.2", map[string]interface{}(nil), map[string]interface{}(nil), nil, []ServiceAction(nil), []ServiceEvent(nil), nil, nil, nil}

	It("Should merge and overwrite existing actions", func() {

		thisName := serviceSchema.GetName()
		Expect(thisName).Should(Equal(serviceSchemaOriginal.Name))
		Expect(thisName).Should(Not(Equal(serviceSchemaMixin.Name)))

	})

})
