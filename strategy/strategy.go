package strategy

type Selector interface {
	TargetNodeID() string
}

type Strategy interface {
	Select([]Selector) *Selector
}
