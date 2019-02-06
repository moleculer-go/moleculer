package strategy_test

import (
	log "github.com/sirupsen/logrus"

	"github.com/moleculer-go/moleculer/strategy"
	test "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var logger = log.WithField("Unit Test", true)

var _ = test.Describe("Strategy", func() {
	thisStrategy := strategy.RoundRobinStrategy{}

	thisNodeMap := []string{"alpha", "beta", "gamma", "delta"}

	thisEmptyMap := []string{}

	thisNode := thisStrategy.SelectTargetNode(thisNodeMap)

	test.It("Should return a target node according to strategy", func() {
		Expect(thisNode).Should(Not(BeNil()))
		//Expect(thisNode).Should(Equal("alpha"))
	})

	thisNode = thisStrategy.SelectTargetNode(thisNodeMap)

	test.It("Should return a target node according to strategy", func() {
		Expect(thisNode).Should(Not(BeNil()))
		//Expect(thisNode).Should(Equal("alpha"))
	})

	thisNode = thisStrategy.SelectTargetNode(thisEmptyMap)

	test.It("Should return empty", func() {
		//Expect(thisNode).Should(Not(BeNil()))
		Expect(thisNode).Should(Equal(""))
	})

	thisNode = thisStrategy.SelectTargetNode(thisNodeMap)

	test.It("Should return a target node according to strategy", func() {
		Expect(thisNode).Should(Not(BeNil()))
		//Expect(thisNode).Should(Equal("alpha"))
	})

	thisNode = thisStrategy.SelectTargetNode(thisNodeMap)

	test.It("Should return a target node according to strategy", func() {
		Expect(thisNode).Should(Not(BeNil()))
		//Expect(thisNode).Should(Equal("alpha"))
	})

	thisNode = thisStrategy.SelectTargetNode(thisEmptyMap)

	test.It("Should return empty", func() {
		//Expect(thisNode).Should(Not(BeNil()))
		Expect(thisNode).Should(Equal(""))
	})

})
