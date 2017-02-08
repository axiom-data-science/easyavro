#!python
# coding=utf-8

from confluent_kafka import KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError

from . import (SCHEMA_REGISTRY_URL,
               KAFKA_BROKERS,
               CONSUMER_GROUP,
               KAFKA_TOPIC)


class BeakerConsumer(AvroConsumer):

    def __init__(self, *args, **kwargs):

        reqs = [
            SCHEMA_REGISTRY_URL,
            KAFKA_BROKERS,
            CONSUMER_GROUP,
            KAFKA_TOPIC
        ]

        for r in reqs:
            if r not in kwargs:
                raise ValueError(
                    'Named parameter "{}" must be supplied'.format(r)
                )
            setattr(self, r, kwargs.pop(r))

        super().__init__(
            {
                'bootstrap.servers': ','.join(getattr(self, KAFKA_BROKERS)),
                'schema.registry.url': getattr(self, SCHEMA_REGISTRY_URL),
                'client.id': self.__class__.__name__,
                'group.id': getattr(self, CONSUMER_GROUP),
                'api.version.request': 'true',
                'debug': 'fetch',
                'default.topic.config': {'auto.offset.reset': 'earliest'}
            }
        )

    def consume(self, on_recieve=None, timeout=None):

        if on_recieve is None:
            def on_recieve(k, v):
                print("Recieved message:\nKey: {}\nValue: {}".format(k, v))

        self.subscribe([getattr(self, KAFKA_TOPIC)])
        while True:
            try:
                msg = self.poll(timeout=timeout)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print("No more messages")
                        break
                    print(msg.error())

                # Call the function we passed in
                on_recieve(msg.key(), msg.value())
            except SerializerError as e:
                print("Message deserialization failed for {}: {}".format(msg, e))

        self.close()
