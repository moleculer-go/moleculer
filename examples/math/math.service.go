package math

import (
	"fmt"

	"github.com/moleculer-go/moleculer"
	. "github.com/moleculer-go/moleculer"
	log "github.com/sirupsen/logrus"
)

// Create a Service Schema
func MathServiceSchema() Service {

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
				Name:    "math.add.called",
				Handler: onAddEvent,
			},
			{
				Name:    "math.sub.called",
				Handler: onSubEvent,
			},
		},
		//Mixins: []*Mixin(helloWorldSchema),
		Created: func(service moleculer.Service, logger *log.Entry) {
			fmt.Println("math service created !")
		},
		Started: func(ctx moleculer.BrokerContext, service moleculer.Service) {
			fmt.Println("math service started !")
		},
		Stopped: func(ctx moleculer.BrokerContext, service moleculer.Service) {
			fmt.Println("math service stopped !")
		},
	}

	return schema
}

func onAddEvent(ctx Context, params Payload) {
	fmt.Printf("\n onAddEvent :\n")
	printEventParams(params)
}

func onSubEvent(ctx Context, params Payload) {
	fmt.Printf("\n onAddEvent :\n")
	printEventParams(params)
}

func addAction(context Context, params Payload) interface{} {
	context.Logger().Info("math service add action.")
	a := params.Get("a").Int()
	b := params.Get("b").Int()
	result := a + b

	context.Logger().Info("params -> a: ", a, "b: ", b, "result: ", result)

	defer context.Emit("add.called", map[string]int{
		"a":      a,
		"b":      b,
		"result": result,
	})

	return result
}

func multAction(context Context, params Payload) interface{} {
	a := params.Get("a").Int()
	b := params.Get("b").Int()
	var result int

	for i := 1; i <= b; i++ {
		actionResult := <-context.Call(
			"math.add",
			map[string]interface{}{
				"a": a,
				"b": a,
			})
		result = result + actionResult.Int()
	}

	defer context.Emit("mult.called", map[string]int{
		"a":      a,
		"b":      b,
		"result": result,
	})

	return result
}

func subAction(context Context, params Payload) interface{} {
	a := params.Get("a").Int()
	b := params.Get("b").Int()
	result := a - b

	defer context.Emit("sub.called", map[string]int{
		"a":      a,
		"b":      b,
		"result": result,
	})
	return result
}

func printEventParams(params moleculer.Payload) {
	fmt.Printf("a: ")
	fmt.Printf(params.Get("a").String())
	fmt.Printf("b: ")
	fmt.Printf(params.Get("b").String())
	fmt.Printf("result: ")
	fmt.Printf(params.Get("result").String())
}
