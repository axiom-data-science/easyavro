# EasyAvro  [![Build Status](https://travis-ci.org/axiom-data-science/easyavro.svg?branch=master)](https://travis-ci.org/axiom-data-science/easyavro)

A ~~lab assistant~~ python helper for producing and consuming `avro` schema'd Kafka topics. Simplicity and the ability to execute a function for each message consumed is the top priority. This is not designed for high throughput.

![EasyAvro](https://upload.wikimedia.org/wikipedia/en/5/59/Beaker_%28Muppet%29.jpg)


## Usage

#### Producer

The schemas `my-topic-key` and `my-topic-value` must be available in the schema registry.

```python
from easyavro import EasyAvroProducer

bp = EasyAvroProducer(
    schema_registry_url='http://localhost:50002',
    kafka_brokers=['localhost:50001'],
    kafka_topic='my-topic'
)

# Records are (key, value) tuples
records = [
    ('foo', 'foo'),
    ('bar', 'bar'),
]
bp.produce(records)
```

Or pass in your own schemas.

```python
from easyavro import EasyAvroProducer

bp = EasyAvroProducer(
    schema_registry_url='http://localhost:50002',
    kafka_brokers=['localhost:50001'],
    kafka_topic='my-topic',
    value_schema=SomeAvroSchemaObject,
    key_schema=SomeAvroSchemaObject,
)

# Records are (key, value) tuples
records = [
    ('foo', 'foo'),
    ('bar', 'bar'),
]
bp.produce(records)
```

If you don't have a key schema, just pass anything other than `None` to the
constructor and use `None` as the value of the key.

```python
from easyavro import EasyAvroProducer

bp = EasyAvroProducer(
    schema_registry_url='http://localhost:50002',
    kafka_brokers=['localhost:50001'],
    kafka_topic='my-topic',
    value_schema=SomeAvroSchemaObject,
    key_schema='not_used_because_no_keys_in_the_records',
)

# Records are (key, value) tuples
records = [
    (None, 'foo'),
    (None, 'bar'),
]
bp.produce(records)
```


#### Consumer

The defaults are sane. They will pull offsets from the broker and set the topic offset to `largest`. This will pull all new messages that haven't been acknowledged by a consumer with the same `consumer_group` (which translates to the `librdkafka` `group.id` setting).

```python
from easyavro import EasyAvroConsumer

def on_recieve(key: str, value: str) -> None:
    print("Got Key:{}\nValue:{}\n".format(key, value))

bc = EasyAvroConsumer(
    schema_registry_url='http://localhost:50002',
    kafka_brokers=['localhost:50001'],
    consumer_group='easyavro.testing',
    kafka_topic='my-topic'
)
bc.consume(on_recieve=on_recieve)
```

Or pass in your own [topic config](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md#topic-configuration-properties) dict.

```python
from easyavro import EasyAvroConsumer

def on_recieve(key: str, value: str) -> None:
    print("Got Key:{}\nValue:{}\n".format(key, value))

bc = EasyAvroConsumer(
    schema_registry_url='http://localhost:50002',
    kafka_brokers=['localhost:50001'],
    consumer_group='easyavro.testing',
    kafka_topic='my-topic',
    topic_config={'enable.auto.commit': False, 'offset.store.method': 'file'}
)
bc.consume(on_recieve=on_recieve)
```

Or pass in a value to use for the `auto.offset.reset` [topic config](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md#topic-configuration-properties) setting.

```python
from easyavro import EasyAvroConsumer

def on_recieve(key: str, value: str) -> None:
    print("Got Key:{}\nValue:{}\n".format(key, value))

bc = EasyAvroConsumer(
    schema_registry_url='http://localhost:50002',
    kafka_brokers=['localhost:50001'],
    consumer_group='easyavro.testing',
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
        --name easyavro-testing \
      landoop/fast-data-dev
```

#### Docker

```
docker build -t easyavro .
docker run easyavro
```

#### No Docker

```
py.test -s -rxs -v
```
