#!python
# coding=utf-8

from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

from . import (SCHEMA_REGISTRY_URL,
               KAFKA_BROKERS,
               KAFKA_TOPIC,
               VALUE_SCHEMA,
               KEY_SCHEMA)


# Monkey patch to get hashable avro schemas
# https://issues.apache.org/jira/browse/AVRO-1737
# https://github.com/confluentinc/confluent-kafka-python/issues/122
from avro import schema
def hash_func(self):
    return hash(str(self))
schema.EnumSchema.__hash__ = hash_func
schema.RecordSchema.__hash__ = hash_func
schema.PrimitiveSchema.__hash__ = hash_func
schema.ArraySchema.__hash__ = hash_func
schema.FixedSchema.__hash__ = hash_func
schema.MapSchema.__hash__ = hash_func


class BeakerProducer(AvroProducer):

    def __init__(self, *args, **kwargs):

        reqs = [
            SCHEMA_REGISTRY_URL,
            KAFKA_BROKERS,
            KAFKA_TOPIC
        ]

        for r in reqs:
            if r not in kwargs:
                raise ValueError(
                    'Named parameter "{}" must be supplied'.format(r)
                )
            setattr(self, r, kwargs.pop(r))

        self._client = CachedSchemaRegistryClient(
            url=getattr(self, SCHEMA_REGISTRY_URL)
        )

        # Value Schema
        setattr(self, VALUE_SCHEMA, kwargs.pop(VALUE_SCHEMA, None))
        if getattr(self, VALUE_SCHEMA) is None:
            vs_name = getattr(self, KAFKA_TOPIC) + '-value'
            _, vs, _ = self._client.get_latest_schema(vs_name)
            if vs is None:
                raise ValueError('Schema "{}" not found in registry'.format(vs_name))
            setattr(self, VALUE_SCHEMA, vs)

        # Key Schema
        setattr(self, KEY_SCHEMA, kwargs.pop(KEY_SCHEMA, None))
        if getattr(self, KEY_SCHEMA) is None:
            ks_name = getattr(self, KAFKA_TOPIC) + '-key'
            _, ks, _ = self._client.get_latest_schema(ks_name)
            if vs is None:
                raise ValueError('Schema "{}" not found in registry'.format(ks_name))
            setattr(self, KEY_SCHEMA, ks)

        super().__init__(
            {
                'bootstrap.servers': ','.join(getattr(self, KAFKA_BROKERS)),
                'schema.registry.url': getattr(self, SCHEMA_REGISTRY_URL),
                'client.id': self.__class__.__name__,
                'api.version.request': 'true',
                'debug': 'msg',
            },
            default_value_schema=getattr(self, VALUE_SCHEMA),
            default_key_schema=getattr(self, KEY_SCHEMA)
        )

    def produce(self, records):
        for r in records:
            super().produce(
                topic=getattr(self, KAFKA_TOPIC),
                key=r[0],
                value=r[1]
            )
        self.flush()
