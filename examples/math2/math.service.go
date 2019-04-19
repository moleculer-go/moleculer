package main

import (
	"fmt"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
	"github.com/moleculer-go/moleculer/payload"
)

type MathService struct {
}

func (s *MathService) Name() string {
	return "math"
}

func (s *MathService) Add(params moleculer.Payload) int {
	return params.Get("a").Int() + params.Get("b").Int()
}

func (s *MathService) Sub(a int, b int) int {
	return a + b
}

// example of no params action
func (svc *MathService) SeedRandom() {

}

// example of no single value params
// action name options: SetLogRate = setLogRate or setlograte or set_log_rate
// math.setLogRate
func (svc *MathService) SetLogRate(value int) {
	//svc.logRate = value
}

// example of action with context and payload
func (svc *MathService) ComplexCalc(context moleculer.Context, params moleculer.Payload) {
	context.Broadcast("math.complexCalc.done")
}

func (svc *MathService) Started(ctx moleculer.BrokerContext) {
	svc.setupEvents(ctx)

	ctx.Logger().Debug("math service started!")
}

func (svc *MathService) setupEvents(ctx moleculer.BrokerContext) {
	ctx.Subscribe("math.complexCalc.done", func(ctx moleculer.Context, params moleculer.Payload) {
		ctx.Logger().Debug("math.complexCalc.done called!")

	})
}

func (svc *MathService) Events() map[string]moleculer.EventHandler {
	return map[string]func(context moleculer.Context, params moleculer.Payload){
		"math.complexCalc.done": func(context moleculer.Context, params moleculer.Payload) {
			context.Logger().Debug("math.complexCalc.done called!")
		},
	}
}

func main() {
	var bkr = broker.New(&moleculer.Config{LogLevel: "error"})
	bkr.Publish(MathService{})
	bkr.Start()
	result := <-bkr.Call("math.add", payload.New(map[string]int{
		"a": 10,
		"b": 130,
	}))
	fmt.Println("result: ", result.Int()) //$ result: 140
	bkr.Stop()
}
