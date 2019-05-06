#!python
# coding=utf-8
import time
import logging
import threading
from typing import List, Callable
from datetime import datetime, timedelta

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError

L = logging.getLogger('easyavro')
L.propagate = False
L.addHandler(logging.NullHandler())


class BaseConsumer:

    def consume(self,
                on_recieve: Callable[[str, str], None] = None,
                on_recieve_timeout: int = None,
                timeout: int = None,
                loop: bool = True,
                initial_wait: int = None,
                cleanup_every: int = 1000
                ) -> None:

        if on_recieve is None:
            def on_recieve(k, v):
                L.info("Recieved message:\nKey: {}\nValue: {}".format(k, v))

        if initial_wait is not None:
            initial_wait = int(initial_wait)
            loops = 0
            started = datetime.now()
            start_delta = timedelta(seconds=initial_wait)
            while (datetime.now() - started) < start_delta:
                L.info("Starting in {} seconds".format(initial_wait - loops))
                time.sleep(1)
                loops += 1

        callback_threads = []
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
                    else:
                        # Call the function we passed in if we consumed a valid message
                        t = threading.Thread(
                            name='EasyAvro-on_recieve',
                            target=on_recieve,
                            args=(msg.key(), msg.value())
                        )
                        t.start()
                        callback_threads.append(t)
                except SerializerError as e:
                    L.warning('Message deserialization failed for "{}: {}"'.format(msg, e))
                finally:
                    # Periodically clean up threads to prevent the list of callback_threads
                    # from becoming absolutely huge on long running Consumers
                    if len(callback_threads) >= cleanup_every:
                        for x in callback_threads:
                            x.join(0)
                        callback_threads = [
                            x for x in callback_threads if x.is_alive()
                        ]
                        cleaned = cleanup_every - len(callback_threads)
                        L.info('Cleaned up {} completed threads'.format(cleaned))

        except KeyboardInterrupt:
            L.info("Aborted via keyboard")
        finally:
            L.info("Waiting for on_recieve callbacks to finish...")
            # Block for `on_recieve_timeout` for each thread that isn't finished
            [ ct.join(timeout=on_recieve_timeout) for ct in callback_threads ]
            # Now see if any threads are still alive (didn't exit after `on_recieve_timeout`)
            alive_threads = [ at for at in callback_threads if at.is_alive() ]
            for at in alive_threads:
                L.warning('{0.name}-{0.ident} never exited and is still running'.format(at))

        L.debug("Closing consumer...")
        self.close()
        L.info("Done consuming")


class EasyConsumer(BaseConsumer, Consumer):

    def __init__(self,
                 kafka_brokers: List[str],
                 consumer_group: str,
                 kafka_topic: str,
                 topic_config: dict = None,
                 offset: str = None,
                 kafka_conf: dict = None) -> None:

        topic_config = topic_config or {}
        self.kafka_topic = kafka_topic

        # A simplier way to set the topic offset
        if offset is not None and 'auto.offset.reset' not in topic_config:
            topic_config['auto.offset.reset'] = offset

        conf = {
            'bootstrap.servers': ','.join(kafka_brokers),
            'client.id': self.__class__.__name__,
            'group.id': consumer_group,
            'api.version.request': 'true',
            'default.topic.config': topic_config
        }

        kafka_conf = kafka_conf or {}

        super().__init__(
            {**conf, **kafka_conf}
        )


class EasyAvroConsumer(BaseConsumer, AvroConsumer):

    def __init__(self,
                 schema_registry_url: str,
                 kafka_brokers: List[str],
                 consumer_group: str,
                 kafka_topic: str,
                 topic_config: dict = None,
                 offset: str = None,
                 kafka_conf: dict = None) -> None:

        topic_config = topic_config or {}
        self.kafka_topic = kafka_topic

        # A simplier way to set the topic offset
        if offset is not None and 'auto.offset.reset' not in topic_config:
            topic_config['auto.offset.reset'] = offset

        conf = {
            'bootstrap.servers': ','.join(kafka_brokers),
            'schema.registry.url': schema_registry_url,
            'client.id': self.__class__.__name__,
            'group.id': consumer_group,
            'api.version.request': 'true',
            'default.topic.config': topic_config
        }

        kafka_conf = kafka_conf or {}

        super().__init__(
            {**conf, **kafka_conf}
        )
