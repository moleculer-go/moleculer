# moleculer

🚀 Progressive microservices framework for Go - based and compatible with https://github.com/moleculerjs/moleculer

Lightning fast, lightweight, simple and fun to develop with. Also easy, very easy to test ;)

[![Gitter](https://badges.gitter.im/moleculer-go/community.svg)](https://gitter.im/moleculer-go/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)
[![Travis Build Status](https://travis-ci.org/moleculer-go/moleculer.svg?branch=master)](https://travis-ci.org/moleculer-go/moleculer)
[![Drone.io Build Status](https://cloud.drone.io/api/badges/moleculer-go/moleculer/status.svg)](https://cloud.drone.io/moleculer-go/moleculer)
[![Go Report Card](https://goreportcard.com/badge/github.com/moleculer-go/moleculer)](https://goreportcard.com/report/github.com/moleculer-go/moleculer)
[![Coverage -> Coveralls](https://coveralls.io/repos/github/moleculer-go/moleculer/badge.svg?branch=master)](https://coveralls.io/github/moleculer-go/moleculer?branch=master)
[![Coverage -> Codecov](https://codecov.io/gh/moleculer-go/moleculer/branch/develop/graph/badge.svg)](https://codecov.io/gh/moleculer-go/moleculer)
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fmoleculer-go%2Fmoleculer.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Fmoleculer-go%2Fmoleculer?ref=badge_shield)

<!--
![](https://img.shields.io/badge/performance-%2B50%25-brightgreen.svg)
![](https://img.shields.io/badge/performance-%2B5%25-green.svg)
![](https://img.shields.io/badge/performance---10%25-yellow.svg)
![](https://img.shields.io/badge/performance---42%25-red.svg)
-->

**Website**: [gomicro.services](http://gomicro.services)

**Documentation**: [Docs](http://gomicro.services/docs)

Status: In development !

# What's included (most under construction)

ALL the goodness moleculer has:

- request-reply concept
- support streams
- support event driven architecture with balancing
- built-in service registry & dynamic service discovery
- load balanced requests & events (round-robin, random, cpu-usage, latency)
- many fault tolerance features (Circuit Breaker, Bulkhead, Retry, Timeout, Fallback)
- supports middlewares
- supports versioned services
- service mixins
- built-in caching solution (memory, Redis)
- pluggable transporters (TCP, NATS, MQTT, Redis, NATS Streaming, Kafka)
- pluggable serializers (JSON, Avro, MsgPack, Protocol Buffer, Thrift)
- pluggable validator
- multiple services on a node/server
- all nodes are equal, no master/leader node

Now available in Golang.

# Installation

```
$ go get github.com/moleculer-go/moleculer
```
