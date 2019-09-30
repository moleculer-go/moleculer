package strategy

// RoundRobinStrategy exposes the type as a strategy option
type RoundRobinStrategy struct {
	counter int
}

func NewRoundRobinStrategy() Strategy {
	return &RoundRobinStrategy{counter: -1}
}

func (roundRobinStrategy *RoundRobinStrategy) Select(nodes []Selector) *Selector {
	if len(nodes) == 0 {
		return nil
	}

	roundRobinStrategy.counter++

	if roundRobinStrategy.counter >= len(nodes) {
		roundRobinStrategy.counter = 0
	}

	return &nodes[roundRobinStrategy.counter]
}
