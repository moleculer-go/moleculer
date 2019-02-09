package strategy

<<<<<<< HEAD
// Strategy exports the node selection function via an interface
=======
type Selector interface {
	TargetNodeID() string
}

>>>>>>> feat (event) load balance
type Strategy interface {
	Select([]Selector) *Selector
}
