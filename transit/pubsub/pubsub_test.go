package pubsub

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var logLevel = "ERROR"

var _ = Describe("Registry Internals", func() {

	It("Call once should invoke the callback only once in a given period", func() {

		counter := 0

		callback := func() {
			counter++
		}

		callOnce := setupDelayedCall(500 * time.Millisecond)
		callOnce(callback)
		time.Sleep(100 * time.Millisecond)
		callOnce(callback)
		time.Sleep(100 * time.Millisecond)
		callOnce(callback)
		time.Sleep(100 * time.Millisecond)
		callOnce(callback)
		time.Sleep(100 * time.Millisecond)
		callOnce(callback)
		time.Sleep(110 * time.Millisecond)
		Expect(counter).Should(Equal(1))

		callOnce(callback)
		Expect(counter).Should(Equal(1))

		time.Sleep(510 * time.Millisecond)
		Expect(counter).Should(Equal(2))

	})
})
