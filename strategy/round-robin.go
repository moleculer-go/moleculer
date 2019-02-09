package strategy

import (
	"math/rand"
)

// RoundRobinStrategy exposes the type as a strategy option
type RoundRobinStrategy struct {
}

func (roundRobinStrategy RoundRobinStrategy) Select(nodes []Selector) *Selector {
	if len(nodes) == 0 {
		return nil
	}
	// Returns a number among the indexes up to the length
	// of the slice
	return &nodes[rand.Intn(len(nodes))]
}
