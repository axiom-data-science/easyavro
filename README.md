# Beaker  [![build status](http://git.axiom/axiom/beaker/badges/master/build.svg)](http://git.axiom/axiom/beaker/commits/master)

A ~~lab assistant~~ python helper for producing and consuming `avro` schema'd Kafka topics. Simplicity and the ability to execute a function for each message consumed is the top priority. This is not designed for high throughput.

![Beaker](https://upload.wikimedia.org/wikipedia/en/5/59/Beaker_%28Muppet%29.jpg)


## Usage

#### Producer

The schemas `my-topic-key` and `my-topic-value` must be available in the schema registry.

```python
from beaker import BeakerProducer

bp = BeakerProducer(
    schema_registry_url='http://localhost:50002',
    kafka_brokers=['localhost:50001'],
    kafka_topic='my-topic'
)

# Records are (key, value) tuples
records = [
    (None, 'foo'),
    (None, 'bar'),
]
bp.produce(records)
```

Or pass in your own schemas.

```python
from beaker import BeakerProducer

bp = BeakerProducer(
    schema_registry_url='http://localhost:50002',
    kafka_brokers=['localhost:50001'],
    kafka_topic='my-topic',
    value_schema=SomeAvroSchemaObject,
    key_schema=SomeAvroSchemaObject,
)

# Records are (key, value) tuples
records = [
    (None, 'foo'),
    (None, 'bar'),
]
bp.produce(records)
```

#### Consumer

The defaults are sane. They will pull offsets from the broker and set the topic offset to `largest`. This will pull all new messagse that haven't been acknokeldged by a consumer with the same `consumer_group` (which translates to the librdkafka `group.id` setting).

```python
from beaker import BeakerConsumer

def on_recieve(key: str, value: str) -> None:
    print("Got Key:{}\nValue:{}\n".format(key, value))

bc = BeakerConsumer(
    schema_registry_url='http://localhost:50002',
    kafka_brokers=['localhost:50001'],
    consumer_group='beaker.testing',
    kafka_topic='my-topic'
)
bc.consume(on_recieve=on_recieve)
```

Or pass in your own [topic config](see https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md#topic-configuration-properties) dict.

```python
from beaker import BeakerConsumer

def on_recieve(key: str, value: str) -> None:
    print("Got Key:{}\nValue:{}\n".format(key, value))

bc = BeakerConsumer(
    schema_registry_url='http://localhost:50002',
    kafka_brokers=['localhost:50001'],
    consumer_group='beaker.testing',
    kafka_topic='my-topic',
    topic_config={'enable.auto.commit': False, 'offset.store.method': 'file'}
)
bc.consume(on_recieve=on_recieve)
```

Or pass in a value to use for the `auto.offset.reset` [topic config](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md#topic-configuration-properties) setting.

```python
from beaker import BeakerConsumer

def on_recieve(key: str, value: str) -> None:
    print("Got Key:{}\nValue:{}\n".format(key, value))

bc = BeakerConsumer(
    schema_registry_url='http://localhost:50002',
    kafka_brokers=['localhost:50001'],
    consumer_group='beaker.testing',
    kafka_topic='my-topic',
    offset='earliest'
)
bc.consume(on_recieve=on_recieve)
```

## Testing

There are only integration tests.

**Start a confluent kafka ecosystem**

```
docker run -d --net=host \
        -e BROKER_PORT=50001 \
        -e REGISTRY_PORT=50002 \
        -e RUNTESTS=0 \
        -e DISABLE=elastic,hbase \
        --name beaker-testing \
      landoop/fast-data-dev
```

#### Docker

```
docker build -t beaker .
docker run beaker
```

#### No Docker

```
py.test -s -rxs -v
```
