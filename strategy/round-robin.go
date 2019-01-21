package strategy

import (
	. "github.com/moleculer-go/moleculer/common"
)

type RoundRobinStrategy struct {
	counter int
}

func (strategy RoundRobinStrategy) SelectEndpoint(endpoints []Endpoint) Endpoint {
	if len(endpoints) == 0 {
		return nil
	}
	if strategy.counter >= len(endpoints) {
		strategy.counter = 0
	}
	defer func() { strategy.counter = strategy.counter + 1 }()
	return endpoints[strategy.counter]
}
