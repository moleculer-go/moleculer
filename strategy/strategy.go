package strategy

type Strategy interface {
	SelectTargetNode([]string) string
}
