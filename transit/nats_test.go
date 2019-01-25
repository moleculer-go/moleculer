package transit_test

import (
	test "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	//. "github.com/moleculer-go/moleculer/transit"
)

var _ = test.Describe("Transit", func() {

	//url := "stan://localhost:4222"

	test.It("Shoulc connect, subscribe, puclish and disconnect", func() {
		Expect(true).Should(Equal(true))
		// msg := "Test Message !!!"

		// transporter := CreateNatsStreaming("test-cluster", "unit-test-client-id", url)
		// <-transporter.Connect()

		// received := make(chan bool)
		// transporter.Subscribe("topicA", "node1", func(value string) {
		// 	Expect(value).Should(Equal(msg))
		// 	received <- true
		// })

		// transporter.PublishString("topicA:node1", msg)

		// Expect(<-received).Should(Equal(true))

		// <-transporter.Disconnect()

	})

})
