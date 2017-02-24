#!python
# coding=utf-8
import logging
from typing import List, Tuple

from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

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

L = logging.getLogger('beaker')
L.propagate = False
L.addHandler(logging.NullHandler())


class BeakerProducer(AvroProducer):

    def __init__(self,
                 schema_registry_url: str,
                 kafka_brokers: List[str],
                 kafka_topic: str,
                 value_schema: schema.Schema = None,
                 key_schema: schema.Schema = None,
                ) -> None:

        self.kafka_topic = kafka_topic
        self._client = CachedSchemaRegistryClient(
            url=schema_registry_url
        )

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

        super().__init__(
            {
                'bootstrap.servers': ','.join(kafka_brokers),
                'schema.registry.url': schema_registry_url,
                'client.id': self.__class__.__name__,
                'api.version.request': 'true',
                'debug': 'msg',
            },
            default_value_schema=value_schema,
            default_key_schema=key_schema
        )

    def produce(self, records: List[Tuple]) -> None:
        for i, r in enumerate(records):
            super().produce(
                topic=self.kafka_topic,
                key=r[0],
                value=r[1]
            )
            L.info("{}/{} messages".format(i + 1, len(records)))

        L.debug("Flushing producer...")
        self.flush()
        L.info("Done producing")
