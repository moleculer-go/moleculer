package strategy_test

import (
	log "github.com/sirupsen/logrus"

	"github.com/moleculer-go/moleculer/strategy"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var logger = log.WithField("Unit Test", true)

type SelectorImpl struct {
	targetNodeID string
}

func (sel SelectorImpl) TargetNodeID() string {
	return sel.targetNodeID
}

var _ = Describe("Strategy", func() {
	thisStrategy := strategy.RandomStrategy{}

	list := []strategy.Selector{SelectorImpl{"alpha"}, SelectorImpl{"beta"}, SelectorImpl{"gamma"}, SelectorImpl{"delta"}}

	emptyList := []strategy.Selector{}

	It("Should return a random target node according to strategy", func() {
		thisNode := thisStrategy.Select(list)
		Expect(thisNode).Should(Not(BeNil()))
	})

	It("Should return a target node according to strategy", func() {
		thisNode := thisStrategy.Select(emptyList)
		Expect(thisNode).Should(BeNil())
	})
})
