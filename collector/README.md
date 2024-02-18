# collector

>Collector is an http server.
>Receives event data, validates it, and sends it to predetermined Kafka topics.

## Flow
![flow](./docs/flow.png)

## Installation
```sh
$ PORT=5000 GIN_MODE=debug go run .
```
