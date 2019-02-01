package strategy

type RoundRobinStrategy struct {
	counter int
}

func (strategy RoundRobinStrategy) SelectTargetNode(nodes []string) string {
	if len(nodes) == 0 {
		return ""
	}
	if strategy.counter >= len(nodes) {
		strategy.counter = 0
	}
	defer func() { strategy.counter = strategy.counter + 1 }()
	return nodes[strategy.counter]
}
