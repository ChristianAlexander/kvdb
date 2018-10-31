# KVDB

A key/value database for the [Phoenix Golang Meetup](https://www.meetup.com/Golang-Phoenix)'s [Build a Database Server](https://www.meetup.com/Golang-Phoenix/events/255183136/) challenge.

Heavily inspired by chapter 7 of [Martin Kleppmann](https://martin.kleppmann.com/)'s [Designing Data-Intensive Applications](https://dataintensive.net/) book.

## Structure

- [`cmd`](cmd) - TCP and HTTP frontends for the DB
- [`commands`](commands) - Implementations of execuatable and undoable actions
- [`protobuf`](protobuf) - Protobuf implementations of store persistence
- [`stores`](stores) - Stuff to do with storage
- [`transactors`](transactors) - Implementation of a transaction orchestrator

## Isolation

This DB implmements serializable isolation with a 2-phase lock.

## Binary Log

This DB has a protobuf binary log for disk persistence.

## See Also

["Transactions: myths, surprises and opportunities"](https://www.youtube.com/watch?v=5ZjhNTM8XU8) - Martin Kleppmann at Strange Loop

[Command Pattern - Wikipedia](https://en.wikipedia.org/wiki/Command_pattern)

[Protocol Buffers](https://developers.google.com/protocol-buffers/)

[Varint - Go Standard Library](https://golang.org/src/encoding/binary/varint.go)
