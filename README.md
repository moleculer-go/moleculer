# Moleculer Go

🚀 Progressive microservices framework for Go
<img src="https://moleculer-go-site.herokuapp.com/images/moleculer-gopher-no-bg.png" alt="Moleculer Gopher" height="65"/>
<img src="https://golang.org/doc/gopher/frontpage.png" alt="Gopher" height="65"/>

Inspired and compatible with [Moleculer JS](https://github.com/moleculerjs/moleculer)

Simple, fast, light and fun to develop with. Also easy, very easy to test ;)

[![Gitter](https://badges.gitter.im/moleculer-go/community.svg)](https://gitter.im/moleculer-go/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)
[![Drone.io Build Status](https://cloud.drone.io/api/badges/moleculer-go/moleculer/status.svg)](https://cloud.drone.io/moleculer-go/moleculer)
[![Go Report Card](https://goreportcard.com/badge/github.com/moleculer-go/moleculer)](https://goreportcard.com/report/github.com/moleculer-go/moleculer)
[![Coverage -> Coveralls](https://coveralls.io/repos/github/moleculer-go/moleculer/badge.svg?branch=master)](https://coveralls.io/github/moleculer-go/moleculer?branch=master)
[![Coverage -> Codecov](https://codecov.io/gh/moleculer-go/moleculer/branch/develop/graph/badge.svg)](https://codecov.io/gh/moleculer-go/moleculer)
<a href="https://app.fossa.com/projects/git%2Bgithub.com%2Fmoleculer-go%2Fmoleculer?ref=badge_shield" alt="FOSSA Status"><img src="https://app.fossa.com/api/projects/git%2Bgithub.com%2Fmoleculer-go%2Fmoleculer.svg?type=shield"/></a>

# Get Started

- [http://gomicro.services (Official site and Documentation)](http://gomicro.services)
- [Database examples](https://moleculer-go-site.herokuapp.com/docs/0.1/moleculer-db.html)
- [WhatsApp App](https://github.com/moleculer-go/example-whatsapp)
- [Benchmark](https://github.com/moleculer-go/benchmark)

## Example

```go
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
```

# Roadmap

![Timeline](https://moleculer-go-site.herokuapp.com/images/timeline.png)

## v0.1.0 (MVP)

Development is `complete` - Documentation is `in-progress` and benchmark is also `in-progress`.

**Contents:**

- Service Broker
- Transit and Transport
- Actions (request-reply)
- Events
- Mixins
- Load balancing for actions and events (random round-robin)
- Service registry & dynamic service discovery
- Versioned services
- Middlewares
- NATS Streaming Transporter
- JSON Serializer
- Examples :)

## v0.2.0 (Beta RC1)

- Action validators
- Support for streams
- More Load balancing implementations (cpu-usage, latency)
- Fault tolerance features (Circuit Breaker, Bulkhead, Retry, Timeout, Fallback)
- Built-in caching solution (memory, Redis)
- More transporters (gRPC, TCP, Redis, Kafka)
- More serializers (Avro, MsgPack, Protocol Buffer, Thrift)

## v0.3.0 (Beta)

- Performance and Optimization
- More DB Adaptors (SQLLite, Firebase, MySQL)
- CLI for Project Seed Generation

## v0.4.0 (Alpha)

- Event Sourcing Mixins

## v0.5.0 (Release)

# Installation

```bash
$ go get github.com/moleculer-go/moleculer
```

# Running examples

```bash

# simple moleculer db example with memory adaptor
$ go run github.com/moleculer-go/stores/examples/users

# simple moleculer db example with Mongo adaptor
$ go run github.com/moleculer-go/stores/examples/usersMongo

# simple moleculer db example with SQLite adaptor
$ go run github.com/moleculer-go/stores/examples/usersSQLite

# complex moleculer db example with population of fields by other services
$ go run github.com/moleculer-go/stores/examples/populates


```
