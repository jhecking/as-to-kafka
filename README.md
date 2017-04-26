as-to-kafka is a proof-of-concept app that continuously reads new data from an
Aerospike cluster and publishes it to a Kafka topic.  The records and keys are
encoding using msgpack.

## Prerequisites

Building the `as-to-kafka` application requires the following libraries:

* `msgpack`: [`msgpack/msgpack-c`](https://github.com/msgpack/msgpack-c)
* `librdkafka`: [`edenhill/librdkafka`](https://github.com/edenhill/librdkafka)
* Aerospike C client, v4.1.4 or later: [`aerospike/aerospike-client-c`](https://github.com/aerospike/aerospike-client-c)

The app uses query filters with predicate expressions which requires Aerospike Server v3.12 or later.

## Usage

To start streaming record updates from Aerospike to Kafka, simply run the `as-to-kafka` application:

    ./as-to-kafka <broker> <topic> <hosts> <ns> <set>

The app takes the following command line arguments:

* `broker` - comma-separated list of Kafka bootstrap brokers (host or host:port)
* `topic` - Kafka topic to publish to
* `hosts` - comma-separated list of Aerospike cluster nodes (host or host:port)
* `ns` - Aerospike namespace to stream data from
* `set` - Set name within the Aerospike namespace

`consumer` is a simple demo app that consumes a Kafka topic and prints the
data out to the console as decoded msgpack object or hexdump.

Usage:

    ./consumer [-M] <topic>