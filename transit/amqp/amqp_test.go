package amqp

import (
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"sort"
	"sync"
	"time"
)

var queues = []string{
	// RPC
	"MOL.DISCONNECT.test-rpc-client",
	"MOL.DISCONNECT.test-rpc-worker1",
	"MOL.DISCONNECT.test-rpc-worker2",
	"MOL.DISCONNECT.test-rpc-worker3",
	"MOL.DISCOVER.test-rpc-client",
	"MOL.DISCOVER.test-rpc-worker1",
	"MOL.DISCOVER.test-rpc-worker2",
	"MOL.DISCOVER.test-rpc-worker3",
	"MOL.EVENT.test-rpc-client",
	"MOL.EVENT.test-rpc-worker1",
	"MOL.EVENT.test-rpc-worker2",
	"MOL.EVENT.test-rpc-worker3",
	"MOL.HEARTBEAT.test-rpc-client",
	"MOL.HEARTBEAT.test-rpc-worker1",
	"MOL.HEARTBEAT.test-rpc-worker2",
	"MOL.HEARTBEAT.test-rpc-worker3",
	"MOL.INFO.test-rpc-client",
	"MOL.INFO.test-rpc-worker1",
	"MOL.INFO.test-rpc-worker2",
	"MOL.INFO.test-rpc-worker3",
	"MOL.PING.test-rpc-client",
	"MOL.PING.test-rpc-worker1",
	"MOL.PING.test-rpc-worker2",
	"MOL.PING.test-rpc-worker3",
	"MOL.PONG.test-rpc-client",
	"MOL.PONG.test-rpc-worker1",
	"MOL.PONG.test-rpc-worker2",
	"MOL.PONG.test-rpc-worker3",
	"MOL.REQ.test-rpc-client",
	"MOL.REQ.test-rpc-worker1",
	"MOL.REQ.test-rpc-worker2",
	"MOL.REQ.test-rpc-worker3",
	"MOL.RES.test-rpc-client",
	"MOL.RES.test-rpc-worker1",
	"MOL.RES.test-rpc-worker2",
	"MOL.RES.test-rpc-worker3",
	// Emit
	"MOL.DISCONNECT.test-emit-event-pub",
	"MOL.DISCONNECT.test-emit-event-sub1",
	"MOL.DISCONNECT.test-emit-event-sub2",
	"MOL.DISCONNECT.test-emit-event-sub3",
	"MOL.DISCOVER.test-emit-event-pub",
	"MOL.DISCOVER.test-emit-event-sub1",
	"MOL.DISCOVER.test-emit-event-sub2",
	"MOL.DISCOVER.test-emit-event-sub3",
	"MOL.EVENT.test-emit-event-pub",
	"MOL.EVENT.test-emit-event-sub1",
	"MOL.EVENT.test-emit-event-sub2",
	"MOL.EVENT.test-emit-event-sub3",
	"MOL.HEARTBEAT.test-emit-event-pub",
	"MOL.HEARTBEAT.test-emit-event-sub1",
	"MOL.HEARTBEAT.test-emit-event-sub2",
	"MOL.HEARTBEAT.test-emit-event-sub3",
	"MOL.INFO.test-emit-event-pub",
	"MOL.INFO.test-emit-event-sub1",
	"MOL.INFO.test-emit-event-sub2",
	"MOL.INFO.test-emit-event-sub3",
	"MOL.PING.test-emit-event-pub",
	"MOL.PING.test-emit-event-sub1",
	"MOL.PING.test-emit-event-sub2",
	"MOL.PING.test-emit-event-sub3",
	"MOL.PONG.test-emit-event-pub",
	"MOL.PONG.test-emit-event-sub1",
	"MOL.PONG.test-emit-event-sub2",
	"MOL.PONG.test-emit-event-sub3",
	"MOL.REQ.test-emit-event-pub",
	"MOL.REQ.test-emit-event-sub1",
	"MOL.REQ.test-emit-event-sub2",
	"MOL.REQ.test-emit-event-sub3",
	"MOL.RES.test-emit-event-pub",
	"MOL.RES.test-emit-event-sub1",
	"MOL.RES.test-emit-event-sub2",
	"MOL.RES.test-emit-event-sub3",
	// Broadcast
	"MOL.REQ.test-broadcast-event-pub",
	"MOL.REQ.test-broadcast-event-sub1",
	"MOL.REQ.test-broadcast-event-sub2",
	"MOL.REQ.test-broadcast-event-sub3",
	"MOL.RES.test-broadcast-event-pub",
	"MOL.RES.test-broadcast-event-sub1",
	"MOL.RES.test-broadcast-event-sub2",
	"MOL.RES.test-broadcast-event-sub3",
	"MOL.DISCONNECT.test-broadcast-event-pub",
	"MOL.DISCONNECT.test-broadcast-event-sub1",
	"MOL.DISCONNECT.test-broadcast-event-sub2",
	"MOL.DISCONNECT.test-broadcast-event-sub3",
	"MOL.DISCOVER.test-broadcast-event-pub",
	"MOL.DISCOVER.test-broadcast-event-sub1",
	"MOL.DISCOVER.test-broadcast-event-sub2",
	"MOL.DISCOVER.test-broadcast-event-sub3",
	"MOL.EVENT.test-broadcast-event-pub",
	"MOL.EVENT.test-broadcast-event-sub1",
	"MOL.EVENT.test-broadcast-event-sub2",
	"MOL.EVENT.test-broadcast-event-sub3",
	"MOL.HEARTBEAT.test-broadcast-event-pub",
	"MOL.HEARTBEAT.test-broadcast-event-sub1",
	"MOL.HEARTBEAT.test-broadcast-event-sub2",
	"MOL.HEARTBEAT.test-broadcast-event-sub3",
	"MOL.INFO.test-broadcast-event-pub",
	"MOL.INFO.test-broadcast-event-sub1",
	"MOL.INFO.test-broadcast-event-sub2",
	"MOL.INFO.test-broadcast-event-sub3",
	"MOL.PING.test-broadcast-event-pub",
	"MOL.PING.test-broadcast-event-sub1",
	"MOL.PING.test-broadcast-event-sub2",
	"MOL.PING.test-broadcast-event-sub3",
	"MOL.PONG.test-broadcast-event-pub",
	"MOL.PONG.test-broadcast-event-sub1",
	"MOL.PONG.test-broadcast-event-sub2",
	"MOL.PONG.test-broadcast-event-sub3",
}
var exchanges = []string{
	"MOL.DISCONNECT",
	"MOL.DISCOVER",
	"MOL.HEARTBEAT",
	"MOL.INFO",
	"MOL.PING",
}

var _ = Describe("Test AMQPTransporter", func() {
	// Delete all queues and exchanges before and after suite
	BeforeSuite(func() {
		purge(queues, exchanges, true)
	})
	AfterSuite(func() {
		purge(queues, exchanges, true)
	})

	// Clear all queues between each test.
	AfterEach(func() {
		purge(queues, exchanges, false)
	})

	Describe("Test AMQPTransporter RPC with built-in balancer", func() {
		var logs []map[string]interface{}

		client := createNode("test-rpc", "client", nil)
		worker1 := createActionWorker(1, &logs)
		worker2 := createActionWorker(2, &logs)
		worker3 := createActionWorker(3, &logs)

		brokers := []*broker.ServiceBroker{client, worker1, worker2, worker3}

		callShortDelay := func() chan moleculer.Payload {
			return client.Call("test.hello", map[string]interface{}{"delay": 20})
		}

		BeforeEach(func() {
			for _, bkr := range brokers {
				bkr.Start()
			}
			time.Sleep(time.Second)
		})
		AfterEach(func() {
			for _, bkr := range brokers {
				logs = nil
				bkr.Stop()
			}
			time.Sleep(time.Second)
		})

		It("Only one node should receive any given request", func() {
			// Ensure that messages are not broadcast to individual queues.
			result := <-callShortDelay()
			Expect(result.Error()).Should(Succeed())
			Expect(logs).Should(HaveLen(2))
			Expect(filter(&logs, "receive")).Should(HaveLen(1))
			Expect(filter(&logs, "respond")).Should(HaveLen(1))
		})

		It("Should load balance requests to available nodes.", func() {
			// Ensure that messages are evenly distributed
			wg := sync.WaitGroup{}
			res := make([]int, 12)
			for i := 0; i < 12; i++ {
				wg.Add(1)
				go func(index int) {
					payload := <-callShortDelay()
					res[index] = payload.Get("worker").Int()
					wg.Done()
				}(i)
			}

			wg.Wait()

			Expect(res).Should(HaveLen(12))
			Expect(res).Should(SatisfyAll(
				ContainElement(1),
				ContainElement(2),
				ContainElement(3),
			))
			Expect(res).Should(WithTransform(
				func(workers []int) []int {
					sort.Ints(workers)
					return workers
				}, Equal([]int{
					1, 1, 1, 1,
					2, 2, 2, 2,
					3, 3, 3, 3,
				}),
			))
		})

		It("Nodes should only receive one request at a time by default", func() {
			// Ensure that prefetch is working. This relies on message acking happening after the action
			// handler runs.
			worker2.Stop()
			worker3.Stop()

			wg := sync.WaitGroup{}
			for i := 0; i < 3; i++ {
				wg.Add(1)
				go func(index int) {
					<-callShortDelay()
					wg.Done()
				}(i)
			}
			wg.Wait()

			for idx, cur := range logs {
				// All requests should be handled by singe node
				Expect(cur["worker"]).Should(Equal(1))

				// Order should go from old -> new
				if idx+1 < len(logs) {
					Expect(cur["timestamp"]).Should(BeTemporally("<=", logs[idx+1]["timestamp"].(time.Time)))
				}

				// If receive and respond don't alternate requests are concurrent
				if idx%2 == 0 {
					Expect(cur["type"]).Should(Equal("receive"))
				} else {
					Expect(cur["type"]).Should(Equal("respond"))
				}
			}
		})
	})

	Describe("Test AMQPTransporter event emit with built-in balancer", func() {
		var logs []string

		pub := createEmitWorker("pub", "emit-handler", &logs)
		sub1 := createEmitWorker("sub1", "emit-handler", &logs)
		sub2 := createEmitWorker("sub2", "emit-handler", &logs)
		sub3 := createEmitWorker("sub3", "other-handler", &logs)

		brokers := []*broker.ServiceBroker{pub, sub1, sub2, sub3}

		// Reset Flow array and start services
		BeforeEach(func() {
			logs = nil

			for _, bkr := range brokers {
				bkr.Start()
			}
			time.Sleep(time.Second)
		})

		// Stop services and clear queues
		AfterEach(func() {
			for _, bkr := range brokers {
				bkr.Stop()
			}
			logs = nil
			time.Sleep(time.Second)
		})

		It("should send emit event to only one service", func() {
			for i := 0; i < 6; i++ {
				pub.Emit("hello.world2", map[string]interface{}{"testing": true})
			}

			time.Sleep(2 * time.Second)

			Expect(logs).Should(HaveLen(12))
			Expect(logs).Should(SatisfyAll(
				ContainElement("pub"),
				// TODO: should uncomment when 'preferLocal' registry parameter will be exposed to config
				//ContainElement("sub1"),
				//ContainElement("sub2"),
				ContainElement("sub3"),
			))
			Expect(logs).Should(WithTransform(
				func(items []string) []string {
					var result []string
					for _, item := range items {
						if item == "sub3" {
							result = append(result, item)
						}
					}

					return result
				},
				HaveLen(6),
			))
		})
	})

	Describe("Test AMQPTransporter event broadcast with built-in balancer", func() {
		var logs []string

		pub := createBroadcastWorker("pub", &logs)
		sub1 := createBroadcastWorker("sub1", &logs)
		sub2 := createBroadcastWorker("sub2", &logs)
		sub3 := createBroadcastWorker("sub3", &logs)

		BeforeEach(func() {
			logs = nil

			go func() {
				time.Sleep(1500 * time.Millisecond)
				sub3.Start()
			}()

			pub.Start()
			sub1.Start()
			sub2.Start()

			time.Sleep(time.Second)
		})

		AfterEach(func() {
			pub.Stop()
			sub1.Stop()
			sub2.Stop()
			sub3.Stop()
		})

		It("Should send an event to all subscribed nodes.", func() {
			pub.Broadcast("hello.world", map[string]interface{}{"testing": true})

			time.Sleep(2 * time.Second)

			Expect(logs).Should(HaveLen(3))
			Expect(logs).Should(SatisfyAll(
				ContainElement("pub"),
				ContainElement("sub1"),
				ContainElement("sub2"),
			))
		}, 10)
	})
})
