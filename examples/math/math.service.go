package math

import (
	"fmt"

	"github.com/moleculer-go/moleculer"
	. "github.com/moleculer-go/moleculer"
)

// Create a Service Schema
func CreateServiceSchema() Service {

	schema := Service{
		Name: "math",
		Actions: []Action{
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
		Events: []Event{
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

	return schema
}

func onAddEvent(ctx Context, params Params) {
	fmt.Printf("\n onAddEvent :\n")
	printEventParams(params)
}

func onSubEvent(ctx Context, params Params) {
	fmt.Printf("\n onAddEvent :\n")
	printEventParams(params)
}

func addAction(context Context, params Params) interface{} {
	a := params.Int("a")
	b := params.Int("b")
	result := a + b

	defer context.Emit("add.called", map[string]int{
		"a":      a,
		"b":      b,
		"result": result,
	})

	return result
}

func multAction(context Context, params Params) interface{} {
	a := params.Int("a")
	b := params.Int("b")
	result := 0

	for i := 1; i <= b; i++ {
		actionResult := <-context.Call(
			"math.add",
			map[string]interface{}{
				"a": a,
				"b": a,
			})
		result = result + actionResult.(int)
	}

	defer context.Emit("mult.called", map[string]int{
		"a":      a,
		"b":      b,
		"result": result,
	})

	return result
}

func subAction(context Context, params Params) interface{} {
	a := params.Int("a")
	b := params.Int("b")
	result := a - b

	defer context.Emit("sub.called", map[string]int{
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
