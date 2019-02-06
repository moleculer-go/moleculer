package strategy

import (
	"math/rand"
	"time"
)

// RoundRobinStrategy exposes the type as a strategy option
type RoundRobinStrategy struct {
}

// SelectTargetNode randomly selects a node out of slice
func (strategy RoundRobinStrategy) SelectTargetNode(nodes []string) string {
	// We need to seed the rand function. If we use
	// a static number the sequence is also static. Therefore
	// using now as unix number.
	rand.Seed(time.Now().UnixNano())
	if len(nodes) == 0 {
		return ""
	}

	// Returns a number among the indexes up to the length
	// of the slice
	return nodes[rand.Intn(len(nodes))]
}
