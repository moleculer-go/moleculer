package test

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/context"
)

var CounterCheckTimeout = 2 * time.Second

func Counter() CounterCheck {
	return CounterCheck{&sync.Mutex{}, make(map[string]int), make(map[string]int)}
}

type CounterCheck struct {
	mutex             *sync.Mutex
	observe           map[string]int
	observeWithPrefix map[string]int
}

func attachNodeID(ctx moleculer.Context, name string) string {
	actionContext := ctx.(*context.Context)
	localNodeID := actionContext.BrokerDelegates().LocalNode().GetID()
	return fmt.Sprint(name, "-", localNodeID)
}

func (counter *CounterCheck) Inc(ctx moleculer.Context, name string) {
	go func() {
		counter.mutex.Lock()
		if value, exists := counter.observe[name]; exists {
			counter.observe[name] = value + 1
		} else {
			counter.observe[name] = 1
		}

		prefixed := attachNodeID(ctx, name)
		if value, exists := counter.observeWithPrefix[prefixed]; exists {
			counter.observeWithPrefix[prefixed] = value + 1
		} else {
			counter.observeWithPrefix[prefixed] = 1
		}
		counter.mutex.Unlock()
	}()
}

func (counter *CounterCheck) Clear() {
	counter.mutex.Lock()
	counter.observe = make(map[string]int)
	counter.observeWithPrefix = make(map[string]int)
	counter.mutex.Unlock()
}

func (counter *CounterCheck) CheckPrefixed(name string, value int) error {
	return counter.checkAbs(&counter.observeWithPrefix, name, value)
}

func (counter *CounterCheck) Check(name string, value int) error {
	return counter.checkAbs(&counter.observe, name, value)
}

func (counter *CounterCheck) checkAbs(values *map[string]int, name string, target int) error {
	result := make(chan error)
	go func() {
		start := time.Now()
		for {
			counter.mutex.Lock()
			value, exists := (*values)[name]
			counter.mutex.Unlock()
			if exists && value >= target || target == 0 {
				result <- nil
				break
			}
			if time.Since(start) > CounterCheckTimeout {
				result <- errors.New(fmt.Sprint("counter check timed out! -> name: ", name, " target: ", target, " current: ", value))
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
	return <-result
}
