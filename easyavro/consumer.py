#!python
# coding=utf-8
import logging
from typing import List, Callable

from confluent_kafka import KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError

L = logging.getLogger('easyavro')
L.propagate = False
L.addHandler(logging.NullHandler())


class EasyAvroConsumer(AvroConsumer):

    def __init__(self,
                 schema_registry_url: str,
                 kafka_brokers: List[str],
                 consumer_group: str,
                 kafka_topic: str,
                 topic_config: dict = None,
                 offset: str = None) -> None:

        topic_config = topic_config or {}
        self.kafka_topic = kafka_topic

        # A simplier way to set the topic offset
        if offset is not None and 'auto.offset.reset' not in topic_config:
            topic_config['auto.offset.reset'] = offset

        super().__init__(
            {
                'bootstrap.servers': ','.join(kafka_brokers),
                'schema.registry.url': schema_registry_url,
                'client.id': self.__class__.__name__,
                'group.id': consumer_group,
                'api.version.request': 'true',
                'default.topic.config': topic_config
            }
        )

    def consume(self,
                on_recieve: Callable[[str, str], None],
                timeout: int = None,
                loop: bool = True) -> None:

        if on_recieve is None:
            def on_recieve(k, v):
                L.info("Recieved message:\nKey: {}\nValue: {}".format(k, v))

        self.subscribe([self.kafka_topic])
        L.info("Starting consumer...")
        try:
            while True:
                try:
                    msg = self.poll(timeout=timeout)
                    if msg is None:
                        continue
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            if loop is True:
                                continue
                            else:
                                break
                        else:
                            L.error(msg.error())
                    # Call the function we passed in
                    on_recieve(msg.key(), msg.value())
                except SerializerError as e:
                    L.warning('Message deserialization failed for "{}: {}"'.format(msg, e))
        except KeyboardInterrupt:
            L.info("Aborted via keyboard")

        L.debug("Closing consumer...")
        self.close()
        L.info("Done consuming")
