# Web MapReduce [![GoDoc][godoc image]][godoc]

A MapReduce server using web browsers as workers, written in Go.

Currently a **work in progress**.

![gopher worker][gopher worker] ![gopher worker][gopher worker] ![gopher worker][gopher worker] ![gopher master][gopher master]

## Abstract

This is an implementation of a [MapReduce][mapreduce] master server,
able to utilize the free computing capabilities of web browsers as MapReduce workers.

Communication is done via [WebSockets][websockets],
which allow consistent full-duplex comminucation between master and workers.

Worker computation is done via [WebWorkers][webworkers],
which allow background processing to take place, while the main UI thread is left uninterrupted.

The server is written in Go and is highly [concurrent][concurrency],
able to make full use of all available CPUs and memory.
Every worker connection is managed by a cheap goroutine.
This makes the master able to handle *hundreds of thousands* of connections at any given moment
on a modern machine.

## Resources

Resources used in the implementation:

- [Google Research: MapReduce: Simplified Data Processing on Large Clusters](http://research.google.com/archive/mapreduce.html)
- [WebSocket RFC](https://tools.ietf.org/html/rfc6455)
- [WebRTC RFC](http://webrtc.org/)
- [Google/MR4C](https://github.com/google/mr4c)
- [Gorrila/Websocket](https://github.com/gorilla/websocket)
- [SockJS](https://github.com/sockjs/sockjs-client)
- [Gopher][go gopher] images are courtesy of [Renee French][renee french].

## TODO

- [x] Master-Worker protocol
  - [x] Protocol unit tests
- [x] Master algorithm and job management
  - [x] Algorithm unit tests
  - [x] Algorithm benchmark tests
- [x] Websocket worker management using [SockJS][sockjs]
  - [x] Unit tests
  - [ ] Websocket integration tests
- [x] Master
  - [x] Master unit tests
  - [ ] Master benchmark tests
- [x] HTTP Server
  - [x] Server unit tests
  - [ ] Website pages (HTML, CSS).
- [ ] Client (Javascript)
  - [ ] Client unit tests
  - [ ] Master-Worker integratino tests

## Develop

### Prerequisites

1. Install [Go][go]: You should probably either use [gvm][gvm] or [install version >= 1.4][go-dl] manually.
1. Clone this repo into: `$GOPATH/src/github.com/oryband/go-web-mapreduce`.

### Build

```bash
# download dependencies
$ go get -t -u -v ./...

# execute
$ go run server/server.go server/api.go server/views.go
```

## Test

```bash
# run all tests including sub-packages, and output coverage.
$ go test -v -cover ./...
```

[godoc]: https://godoc.org/github.com/oryband/go-web-mapreduce
[godoc image]: https://godoc.org/github.com/oryband/go-web-mapreduce?status.svg
[go]: https://golang.org
[go-dl]: https://golang.org/doc/install
[mapreduce]: http://en.wikipedia.org/wiki/MapReduce
[websockets]: https://developer.mozilla.org/en/docs/WebSockets
[webworkers]: https://developer.mozilla.org/en-US/docs/Web/API/Web_Workers_API
[concurrency]: https://golang.org/doc/effective_go.html#concurrency
[gopher worker]: https://golang.org/doc/gopher/ref.png
[gopher master]: https://golang.org/doc/gopher/talks.png
[renee french]: http://reneefrench.blogspot.com
[go gopher]: https://blog.golang.org/gopher
