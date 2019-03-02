package pubsub

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var logLevel = "ERROR"

var _ = Describe("Registry Internals", func() {

	It("Should return the number of neighbours", func() {

		pubsub := PubSub{knownNeighbours: map[string]int64{
			"x": int64(10),
			"y": int64(10),
			"z": int64(10),
		}}

		Expect(pubsub.neighbours()).Should(BeEquivalentTo(3))
	})
})
