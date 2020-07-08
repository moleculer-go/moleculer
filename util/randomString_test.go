package util

import (
	"sync"

	g "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = g.Describe("Util", func() {

	g.It("ISSUE-88: Broker hang if multiple goroutines make calls", func() {
		threadsNum := 1000

		buffer := make([]string, threadsNum)

		wg := sync.WaitGroup{}
		wg.Add(1000)

		for i := 0; i < 1000; i++ {
			go func(idx int) {
				defer wg.Done()
				buffer[idx] = RandomString(12)
			}(i)
		}

		wg.Wait()

		sMap := map[string]interface{}{}
		for i := 0; i < threadsNum; i++ {
			sMap[buffer[i]] = nil
		}

		Expect(len(sMap)).Should(Equal(threadsNum))
	})
})
