# EasyAvro  [![Build Status](https://travis-ci.org/axiom-data-science/easyavro.svg?branch=master)](https://travis-ci.org/axiom-data-science/easyavro)

A python helper for producing and consuming Kafka topics. Simplicity and the ability to execute a function for each message consumed is the top priority.


## Installation

```bash
conda install -c axiom-data-science easyavro
```


## Usage

### Producer

Both `EasyProducer` and `EasyAvroProducer` take in the initialization parameter `kafka_conf` to directly control the parameters passed to the librdkafka C library. These take precedence over all other parameters. See the documentation for the `config` parameter to [`Producer`](https://docs.confluent.io/current/clients/confluent-kafka-python/#producer), [`AvroProducer`](https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.avro.AvroProducer) and the [list of librdkafka properties](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md#global-configuration-properties).

```python
bp = EasyProducer(
    kafka_brokers=['localhost:4001'],
    kafka_topic='my-topic',
    kafka_conf={
        'debug': 'msg',
        'api.version.request': 'false',
        'queue.buffering.max.messages': 50000
    }
)
```

In addition to a list of records, the `produce` method also accepts a `batch` parameter which
will flush the producer after that number of records. This is useful to avoid `BufferError: Local: Queue full` errors if you are producing more messages at once than the librdkafka option `queue.buffering.max.messages`.


```python
bp = EasyAvroProducer(
    schema_registry_url='http://localhost:4002',
    kafka_brokers=['localhost:4001'],
    kafka_topic='my-topic',
    kafka_conf={
        'queue.buffering.max.messages': 1
    }
)

records = [
    (None, 'foo'),
    (None, 'bar'),
]

# This will raise an error because the number of records is
# larger than the `queue.buffering.max.messages` config option.
bp.produce(records)

# This will NOT raise an error because the producer is flushed
# every `batch` messages.
bp.produce(records, batch=1)

# Supply a `flush_timeout` parameter to avoid deadlocking the C library
# when brokers are down. Messages that are not flushed after the timeout
# are kept in an internal queue and are processed at the next flush.
# Default is 60 seconds.
bp.produce(records, batch=1, flush_timeout=10)
```

#### EasyProducer

If you are not using a Confluent SchemaRegistry and want to handle the packing of messages yourself, use the `EasyProducer` class.

```python
from easyavro import EasyProducer

bp = EasyProducer(
    kafka_brokers=['localhost:4001'],
    kafka_topic='my-topic'
)

# Records are (key, value) tuples
records = [
    ('foo', 'foo'),
    ('bar', 'bar'),
]
bp.produce(records)
```

You can use complicated keys and values.

```python
import msgpack
from easyavro import EasyProducer

bp = EasyProducer(
    kafka_brokers=['localhost:4001'],
    kafka_topic='my-topic'
)

# Records are (key, value) tuples
records = [
    ('foo', msgpack.dumps({'foo': 'foo'})),
    ('bar', msgpack.dumps({'bar': 'bar'})),
]
bp.produce(records)
```

#### EasyAvroProducer

If you are using a Confluent SchemaRegistry this helper exists to match your topic name to existing schemas in the registry. Your schemas `my-topic-key` and `my-topic-value` must be already available in the schema registry!

```python
from easyavro import EasyAvroProducer

bp = EasyAvroProducer(
    schema_registry_url='http://localhost:4002',
    kafka_brokers=['localhost:4001'],
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
    schema_registry_url='http://localhost:4002',
    kafka_brokers=['localhost:4001'],
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
    schema_registry_url='http://localhost:4002',
    kafka_brokers=['localhost:4001'],
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

### Consumer

The defaults are sane. They will pull offsets from the broker and set the topic offset to `largest`. This will pull all new messages that haven't been acknowledged by a consumer with the same `consumer_group` (which translates to the `librdkafka` `group.id` setting).

If you need to override any kafka level parameters, you may use the the `kafka_conf` (`dict`) initialization parameter on `Consumer`. It will override any of the defaults the `Consumer` uses. See the documentation for the `config` parameter to [`Consumer`](https://docs.confluent.io/current/clients/confluent-kafka-python/#consumer), [`AvroConsumer`](https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.avro.AvroConsumer) and the [list of librdkafka properties](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md#global-configuration-properties).

##### Parameters for `start` function

*  `on_receive` (`Callable[str, str]` or `Callable[List[Message]]`) - Function that is executed (in a new thread) for each retrieved message.
*  `on_receive_timeout` (`int`) - Seconds the `Consumer` will wait for the calls to `on_receive` to exit before moving on. By default it will wait forever. You should set this to a reasonable maximum number seconds your `on_receive` callback will take to prevent dead-lock when the `Consumer` is exiting and trying to cleanup its spawned threads.
*  `timeout` (`int`) - The `timeout` parameter to the `poll` function in `confluent-kafka`. Controls how long `poll` will block while waiting for messages.
*  `loop` (`bool`) - If the `Consumer` will keep looping for message or break after retrieving the first chunk message. This is useful when testing.
*  `initial_wait` (`int`)- Seconds the Consumer should wait before starting to consume. This is useful when testing.
*  `cleanup_every` (`int`) - Try to cleanup spawned thread after this many messages.
*  `num_messages` (`int`) - Consume this many messages from the topic at once. This can improve throughput when dealing with high-volume topics that can benefit from processing many messages at once.
*  `receive_messages_in_callback` (`bool`) - Instead of calling the `on_receive` callback with key/value pairs, call it with `confluent_kafka.Message` objects. This requires the user to call `message.key()` and `message.value()` on each. This gives the user access to other message attributes like `message.topic()` in the callback. **Setting this parameter to `True` is recommended for any new code.**

```python
from easyavro import EasyConsumer

def on_receive(key: str, value: str) -> None:
    print("Got Key:{}\nValue:{}\n".format(key, value))

bc = EasyConsumer(
    kafka_brokers=['localhost:4001'],
    consumer_group='easyavro.testing',
    kafka_topic='my-topic'
)
bc.start(on_receive=on_receive)
```

Or pass in your own [kafka config](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md#topic-configuration-properties) dict.

```python
from easyavro import EasyConsumer

def on_receive(key: str, value: str) -> None:
    print("Got Key:{}\nValue:{}\n".format(key, value))

bc = EasyConsumer(
    kafka_brokers=['localhost:4001'],
    consumer_group='easyavro.testing',
    kafka_topic='my-topic',
    kafka_conf={
        'enable.auto.commit': False,
        'offset.store.method': 'file'
    }
)
bc.start(on_receive=on_receive)
```

Or pass in a value to use for the `auto.offset.reset` [topic config](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md#topic-configuration-properties) setting.

```python
from easyavro import EasyConsumer

def on_receive(key: str, value: str) -> None:
    print("Got Key:{}\nValue:{}\n".format(key, value))

bc = EasyConsumer(
    kafka_brokers=['localhost:4001'],
    consumer_group='easyavro.testing',
    kafka_topic='my-topic',
    offset='earliest'
)
bc.start(on_receive=on_receive)
```

#### EasyConsumer

If you are not using a Confluent SchemaRegistry and want to handle the unpacking of messages yourself, use the `EasyConsumer` class.

```python
from easyavro import EasyConsumer

def on_receive(key: str, value: str) -> None:
    print("Got Key:{}\nValue:{}\n".format(key, value))

bc = EasyConsumer(
    kafka_brokers=['localhost:4001'],
    consumer_group='easyavro.testing',
    kafka_topic='my-topic'
)
bc.start(on_receive=on_receive)
```

You can unpack data as needed in the callback function

```python
import msgpack
from easyavro import EasyConsumer

def on_receive(key: str, value: bytes) -> None:
    print("Got Key:{}\nValue:{}\n".format(key, msgpack.loads(value)))

bc = EasyConsumer(
    kafka_brokers=['localhost:4001'],
    consumer_group='easyavro.testing',
    kafka_topic='my-topic'
)
bc.start(on_receive=on_receive)
```

You can receive a list of Message objects instead of key/value pairs

```python
import msgpack
from typing import List
from easyavro import EasyConsumer
from confluent_kafka import Message

def on_receive(messages: List[Message]) -> None:
    for m in messages:
        print(
            "Got Message - Topic:{}\nKey:{}\nValue:{}\n".format(m.topic(), m.key(), m.value()
        )

bc = EasyConsumer(
    kafka_brokers=['localhost:4001'],
    consumer_group='easyavro.testing',
    kafka_topic='my-topic',
)
bc.start(on_receive=on_receive, num_messages=5, receive_messages_in_callback=True)
```


#### EasyAvroConsumer

If you are using a Confluent SchemaRegistry this helper exists to match your topic name to existing schemas in the registry. Your schemas must already be available in the schema registry as `[topic]-key` and `[topic]-value`. Pass the `schema_registry_url` parameter to EasyAvroConsumer and the rest is
taken care of.

```python
from easyavro import EasyAvroConsumer

def on_receive(key: str, value: str) -> None:
    print("Got Key:{}\nValue:{}\n".format(key, value))

bc = EasyAvroConsumer(
    schema_registry_url='http://localhost:4002',
    kafka_brokers=['localhost:4001'],
    consumer_group='easyavro.testing',
    kafka_topic='my-topic'
)
bc.start(on_receive=on_receive)
```

## Testing

There are only integration tests.

**Start a confluent kafka ecosystem**

```
docker run -d --net=host \
        -e ZK_PORT=50000 \
        -e BROKER_PORT=4001 \
        -e REGISTRY_PORT=4002 \
        -e REST_PORT=4003 \
        -e CONNECT_PORT=4004 \
        -e WEB_PORT=4005 \
        -e RUNTESTS=0 \
        -e DISABLE=elastic,hbase \
        -e DISABLE_JMX=1 \
        -e RUNTESTS=0 \
        -e FORWARDLOGS=0 \
        -e SAMPLEDATA=0 \
        --name easyavro-testing \
      landoop/fast-data-dev:1.0.1
```

#### Docker

```
docker build -t easyavro .
docker run --net="host" easyavro
```

#### No Docker

```
conda env create environment.yml
pytest -s -rxs -v
```
