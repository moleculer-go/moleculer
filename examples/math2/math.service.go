package main

import (
	"fmt"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
)

type MathService struct {
}

func (s MathService) Name() string {
	return "math"
}

func (s *MathService) Add(params moleculer.Payload) int {
	return params.Get("a").Int() + params.Get("b").Int()
}

func (s *MathService) Sub(a int, b int) int {
	return a - b
}

func main() {
	var bkr = broker.New(&moleculer.Config{LogLevel: "error"})
	bkr.Publish(&MathService{})
	bkr.Start()
	result := <-bkr.Call("math.add", map[string]int{
		"a": 10,
		"b": 130,
	})
	fmt.Println("result: ", result.Int())
	//$ result: 140
	bkr.Stop()
}
