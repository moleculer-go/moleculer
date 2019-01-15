package main

import (
	"fmt"
	"github.com/moleculer-go/moleculer/broker"
	"github.com/moleculer-go/moleculer/context"
)

events := map[string]interface{}{
	"math.add.called": onAddEvent,
	"math.sub.called": onSubEvent
}

actions := map[string]interface{}{
	"add": addAction,
	"sub": subAction
}

service := broker.createService("math", actions, events)

func printEventParams(params context.params) {
	fmt.Printf("a: ")
	fmt.Printf(params.getInt("a"))
	fmt.Printf("b: ")
	fmt.Printf(params.getInt("b"))
	fmt.Printf("result: ")
	fmt.Printf(params.getInt("result"))
}

func onAddEvent(context Context, params context.params) {
	fmt.Printf("\n onAddEvent :\n")
	printEventParams(params)
}

func onSubEvent(context Context, params context.params) {
	fmt.Printf("\n onAddEvent :\n")
	printEventParams(params)
}

func addAction(context context, params context.params) {
	a := params.getNumber("a")
	b := params.getNumber("b")
	result := a + b

	context.emit("add.called", map[string]int{}{
		"a": a
		"b": b
		"result": result
	}

	return result
}

func subAction(context context, params context.params) {
	a := params.getInt("a")
	b := params.getInt("b")
	result := a - b

	context.emit("sub.called", map[string]int{}{
		"a": a
		"b": b
		"result": result
	}

	return result
}

func main() {
	broker.Start(service)
}
