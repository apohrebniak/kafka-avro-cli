# Kafka-avro-cli
CLI tool for writing Avro serialized messages into Kafka

### Features
* TLS support
* Confluent Schema Registry support

### Installation
You can download a prebuild `x86_64-unknown-linux-gnu` binary in the [Releases section](https://github.com/apohrebniak/kafka-avro-cli/releases) or build it from sources with `cargo build --release`

### Dependencies
To build the binary you'll need:
* the GNU toolchain
* GNU make
* pthreads
* libssl-dev

Runtime dependencies:
* libssl

### Usage
```
  USAGE:
      kafka-avro-cli produce [FLAGS] [OPTIONS] --hosts <host:port[,host:port[...]]> --topic <TOPIC>
  
  FLAGS:
          --help                    Prints help information
          --ssl.disable.validate    Do not validate broker's certificate
          --ssl                     Use SSL
          --ssl.host.validate       To validate broker's hostname
      -T, --text                    Message input is just a plain text. (JSON by default)
      -V, --version                 Prints version information
  
  OPTIONS:
      -h, --hosts <host:port[,host:port[...]]>    Kafka hosts
      -p, --payload <JSON>
              Message payload. JSON expected if '--text' flag is not present
  
          --payload-file <PATH>
              New-line delimited file. Each row is a message payload. (JSON or plain text in respect
              with `text`)
  
          --registry-url <http[s]://host:port>    Schema-registry url
      -s, --schema <SCHEMA JSON>                  Avro schema used to serialize payload
          --schema-file <PATH>
              File containing the Avro schema used to serialize payload
  
          --ssl.ca.location <PATH>
              File or directory path to CA certificate(s) for verifying the broker's key. (PEM)
  
          --ssl.keystore.location <PATH>          Path to client's keystore (PKCS#12)
          --ssl.keystore.password <PATH>          Client's keystore (PKCS#12) password
      -t, --topic <TOPIC>                         Topic name
```

### Examples
Produce simple text message:
```
kafka-avro-cli produce -h localhost:9092 -t my-topic -T -p "Hello Kafka!"
```
Produce json message with avro schema:
```
kafka-avro-cli produce -h localhost:9092 -t my-topic -p '{"msg": "Hello Kafka!"}' -s '{"name":"msg", "type": "record", "fields": [{"name": "msg", "type": "string"}]}'
```
Produce json message with avro schema and register the schema in schema-registry:
```
kafka-avro-cli produce -h localhost:9092 -t my-topic --registry-url http://localhost:8081 -p '{"msg": "Hello Kafka!"}' -s '{"name":"msg", "type": "record", "fields": [{"name": "msg", "type": "string"}]}'
```
Produce json message using already registered schema
```
kafka-avro-cli produce -h localhost:9092 -t my-topic --registry-url http://localhost:8081 -p '{"msg": "Hello Kafka!"}'
```
