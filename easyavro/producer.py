#!python
# coding=utf-8
import logging
from typing import List, Tuple
from itertools import zip_longest, filterfalse

from avro import schema
from confluent_kafka import Producer
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient


# Monkey patch to get hashable avro schemas
# https://issues.apache.org/jira/browse/AVRO-1737
# https://github.com/confluentinc/confluent-kafka-python/issues/122
def hash_func(self):
    return hash(str(self))


schema.EnumSchema.__hash__ = hash_func
schema.RecordSchema.__hash__ = hash_func
schema.PrimitiveSchema.__hash__ = hash_func
schema.ArraySchema.__hash__ = hash_func
schema.FixedSchema.__hash__ = hash_func
schema.MapSchema.__hash__ = hash_func

L = logging.getLogger('easyavro')
L.propagate = False
L.addHandler(logging.NullHandler())


def grouper(iterable, batch_size, fillend=False, fillvalue=None):
    # Modified from https://docs.python.org/3/library/itertools.html#recipes
    # to remove None values
    # grouper('ABCDEFG', 3, fillend=True, fillvalue='x') --> ABC DEF Gxx"
    # grouper('ABCDEFG', 3, fillend=False) --> ABC DEF G"
    "Collect data into fixed-length chunks or blocks"
    args = [iter(iterable)] * batch_size
    if fillend is False:
        return ( tuple(filterfalse(lambda x: x is None, g)) for g in zip_longest(*args, fillvalue=None) )
    else:
        return zip_longest(*args, fillvalue=fillvalue)


def on_delivery(err, msg):
    if err:
        L.error(err)
    else:
        L.debug('Delivered to {} at offset {}'.format(msg.topic(), msg.offset()))


class BaseProducer:


    def produce(self, records: List[Tuple], batch=None, flush_timeout=60) -> None:

        batch = batch or len(records)

        for g, group in enumerate(grouper(records, batch)):

            for i, r in enumerate(group):
                super().produce(
                    topic=self.kafka_topic,
                    key=r[0],
                    value=r[1],
                    on_delivery=on_delivery
                )
                L.debug("{}/{} messages queued".format(i + 1, len(records)))

            L.debug("Flushing...")
            remaining = self.flush(timeout=flush_timeout)
            sent = len(group) - remaining
            L.info("Batch {} finished: {} sent, {} pending".format(g, sent, remaining))

        self.flush(timeout=flush_timeout)
        L.info("Done producing")


class EasyProducer(BaseProducer, Producer):

    def __init__(self,
                 kafka_brokers: List[str],
                 kafka_topic: str,
                 debug: bool = False,
                 kafka_conf: dict = None,
                 py_conf: dict = None) -> None:

        self.kafka_topic = kafka_topic

        conf = {
            'bootstrap.servers': ','.join(kafka_brokers),
            'client.id': self.__class__.__name__,
            'api.version.request': 'true',
        }

        if debug is True:
            conf['debug'] = 'msg'

        kafka_conf = kafka_conf or {}
        py_conf = py_conf or {}

        super().__init__(
            {**conf, **kafka_conf},
            **py_conf
        )


class EasyAvroProducer(BaseProducer, AvroProducer):

    def __init__(self,
                 schema_registry_url: str,
                 kafka_brokers: List[str],
                 kafka_topic: str,
                 value_schema: schema.Schema = None,
                 key_schema: schema.Schema = None,
                 debug: bool = False,
                 kafka_conf: dict = None,
                 py_conf: dict = None) -> None:

        self.kafka_topic = kafka_topic
        self._client = CachedSchemaRegistryClient(dict(
            url=schema_registry_url
        ))

        # Value Schema
        if value_schema is None:
            vs_name = '{}-value'.format(self.kafka_topic)
            _, value_schema, _ = self._client.get_latest_schema(vs_name)
            if value_schema is None:
                raise ValueError('Schema "{}" not found in registry'.format(vs_name))

        # Key Schema
        if key_schema is None:
            ks_name = '{}-key'.format(self.kafka_topic)
            _, key_schema, _ = self._client.get_latest_schema(ks_name)
            if key_schema is None:
                raise ValueError('Schema "{}" not found in registry'.format(ks_name))

        conf = {
            'bootstrap.servers': ','.join(kafka_brokers),
            'schema.registry.url': schema_registry_url,
            'client.id': self.__class__.__name__,
            'api.version.request': 'true',
        }

        if debug is True:
            conf['debug'] = 'msg'

        kafka_conf = kafka_conf or {}
        py_conf = py_conf or {}

        super().__init__(
            {**conf, **kafka_conf},
            default_value_schema=value_schema,
            default_key_schema=key_schema,
            **py_conf
        )
