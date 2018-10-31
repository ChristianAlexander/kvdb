# KVDB

A key/value database for the [Phoenix Golang Meetup](https://www.meetup.com/Golang-Phoenix)'s [Build a Database Server](https://www.meetup.com/Golang-Phoenix/events/255183136/) challenge.

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

