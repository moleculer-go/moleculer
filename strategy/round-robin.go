package strategy

import (
	. "github.com/moleculer-go/moleculer/endpoint"
)

type RoundRobinStrategy struct {
	counter int
}

func (strategy RoundRobinStrategy) SelectEndpoint(endpoints []*Endpoint) *Endpoint {
	if strategy.counter >= len(endpoints) {
		strategy.counter = 0
	}
	strategy.counter = strategy.counter + 1
	return endpoints[strategy.counter]
}
