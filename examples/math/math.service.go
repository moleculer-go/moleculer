package math

import (
	"fmt"

	"github.com/moleculer-go/moleculer"
)

// Create a Service Schema
func CreateServiceSchema() *moleculer.ServiceSchema {

	schema := moleculer.ServiceSchema{
		Name: "math",
		Actions: []moleculer.ServiceAction{
			{
				Name:    "add",
				Handler: addAction,
			},
			{
				Name:    "sub",
				Handler: subAction,
			},
			{
				Name:    "mult",
				Handler: multAction,
			},
		},
		Events: []moleculer.ServiceEvent{
			{
				"math.add.called",
				onAddEvent,
			},
			{
				"math.sub.called",
				onSubEvent,
			},
		},
		Started: func() {
			fmt.Println("math service started !")
		},
	}

	return &schema
}

func onAddEvent(ctx Context, params moleculer.Params) {
	fmt.Printf("\n onAddEvent :\n")
	printEventParams(params)
}

func onSubEvent(ctx Context, params moleculer.Params) {
	fmt.Printf("\n onAddEvent :\n")
	printEventParams(params)
}

func addAction(ctx Context, params moleculer.Params) interface{} {
	broker := moleculer.BrokerFromContext(&ctx)

	a := params.GetInt("a")
	b := params.GetInt("b")
	result := a + b

	defer broker.Emit("add.called", map[string]int{
		"a":      a,
		"b":      b,
		"result": result,
	})

	return result
}

func multAction(ctx Context, params moleculer.Params) interface{} {
	broker := moleculer.BrokerFromContext(&ctx)

	a := params.GetInt("a")
	b := params.GetInt("b")
	result := 0

	for i := 1; i <= b; i++ {
		actionResult := broker.Call("math.add", map[string]int{
			"a": a,
			"b": a,
		})
		intResult := actionResult.(int)
		result = result + intResult
	}

	defer broker.Emit("mult.called", map[string]int{
		"a":      a,
		"b":      b,
		"result": result,
	})

	return result
}

func subAction(ctx Context, params moleculer.Params) interface{} {
	broker := moleculer.BrokerFromContext(&ctx)

	a := params.GetInt("a")
	b := params.GetInt("b")
	result := a - b

	defer broker.Emit("sub.called", map[string]int{
		"a":      a,
		"b":      b,
		"result": result,
	})

	return result
}

func printEventParams(params moleculer.Params) {
	fmt.Printf("a: ")
	fmt.Printf(params.Get("a"))
	fmt.Printf("b: ")
	fmt.Printf(params.Get("b"))
	fmt.Printf("result: ")
	fmt.Printf(params.Get("result"))
}
