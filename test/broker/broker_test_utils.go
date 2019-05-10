package broker

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
)

func hasService(list []moleculer.Payload, names ...string) bool {
	for _, p := range list {
		for _, name := range names {
			if p.Get("name").String() == name {
				return true
			}
		}
	}
	return false
}

func WaitNodeStop(nodeID string, bkrs ...*broker.ServiceBroker) chan error {
	res := make(chan error)
	go func() {
		start := time.Now()
		for {
			found := 0
			for _, bkr := range bkrs {
				for _, node := range bkr.KnownNodes(true) {
					if nodeID == node {
						found = found + 1
						break
					}
				}
			}
			if found == 0 {
				res <- nil
				return
			}
			if time.Since(start) > WaitTimeout {
				res <- errors.New(fmt.Sprint("WaitNode timed out! -> nodeID: ", nodeID))
				break
			}
			time.Sleep(time.Second)
		}
	}()
	return res
}

var WaitTimeout = 2 * time.Second

func WaitNode(nodeID string, bkrs ...*broker.ServiceBroker) chan error {
	res := make(chan error)
	go func() {
		start := time.Now()
		for {
			found := 0
			for _, bkr := range bkrs {
				for _, node := range bkr.KnownNodes(true) {
					if nodeID == node {
						found = found + 1
						break
					}
				}
			}
			if found == len(bkrs) {
				res <- nil
				return
			}
			if time.Since(start) > WaitTimeout {
				res <- errors.New(fmt.Sprint("WaitNode timed out! -> nodeID: ", nodeID))
				break
			}
			time.Sleep(time.Nanosecond)
		}
	}()
	return res
}

var waitBrokers = map[*broker.ServiceBroker]*[]moleculer.Payload{}

func WaitServiceStarted(bkr *broker.ServiceBroker, names ...string) chan error {
	res := make(chan error)
	serviceAdded, exists := waitBrokers[bkr]
	if !exists {
		list := []moleculer.Payload{}
		serviceAdded = &list
		waitBrokers[bkr] = serviceAdded
		addedMutex := &sync.Mutex{}
		bkr.Publish(moleculer.ServiceSchema{
			Name: "service-added-checker",
			Events: []moleculer.Event{
				moleculer.Event{
					Name: "$registry.service.added",
					Handler: func(ctx moleculer.Context, params moleculer.Payload) {
						addedMutex.Lock()
						defer addedMutex.Unlock()
						list = append(list, params)
					},
				},
			},
		})
	}

	go func() {
		start := time.Now()
		for {
			if hasService((*serviceAdded), names...) {
				res <- nil
				return
			}
			if time.Since(start) > WaitTimeout {
				res <- errors.New(fmt.Sprint("WaitServiceStarted timed out! -> names: ", names))
				break
			}
			time.Sleep(time.Nanosecond)
		}
	}()
	return res
}
