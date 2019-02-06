package strategy

// Strategy exports the node selection function via an interface
type Strategy interface {
	SelectTargetNode([]string) string
}
