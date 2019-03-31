package main

import (
	"fmt"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
	"github.com/moleculer-go/moleculer/payload"
)

var mathService = moleculer.Service{
	Name: "math",
	Actions: []moleculer.Action{
		{
			Name: "add",
			Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {

				email := "sergio@hotmail.com"
				r := <-ctx.Call("profile.search", email)
				fmt.Printf(r)

				return params.Get("a").Int() + params.Get("b").Int()
			},
		},
	},
}

func main() {
	var bkr = broker.New(&moleculer.Config{LogLevel: "error"})
	bkr.AddService(mathService)
	bkr.Start()
	result := <-bkr.Call("math.add", payload.New(map[string]int{
		"a": 10,
		"b": 130,
	}))
	fmt.Println("result: ", result.Int()) //$ result: 140
	bkr.Stop()
}
