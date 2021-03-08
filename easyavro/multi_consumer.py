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


class MultiConsumer:

    def start(self,
              on_receive: Callable[[str, str], None] = None,
              on_receive_timeout: int = None,
              timeout: int = None,
              loop: bool = True,
              num_messages: int = 1,
              initial_wait: int = None,
              cleanup_every: int = 1000
              ) -> None:

        if on_receive is None:
            def on_receive(k, v):
                L.info("Received message:\nKey: {}\nValue: {}".format(k, v))

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
        self.subscribe(self.kafka_topics)
        L.info("Starting consumer...")

        break_loop = False

        try:
            while True:

                if break_loop is True:
                    break

                try:
                    messages = self.consume(num_messages=num_messages, timeout=timeout)

                    valid_messages = []
                    for msg in messages:
                        if msg is None:
                            continue
                        if msg.error():
                            if msg.error().code() == KafkaError._PARTITION_EOF:
                                if loop is True:
                                    continue
                                else:
                                    break_loop = True
                            else:
                                L.error(msg.error())
                        else:
                            valid_messages.append(msg)

                    # If we got any valid messages call the Thread with the collection
                    if valid_messages:
                        # Call the function we passed in if we consumed valid messages
                        t = threading.Thread(
                            name='EasyAvro-on_receive',
                            target=on_receive,
                            args=(valid_messages,)
                        )
                        t.start()
                        callback_threads.append(t)
                except SerializerError as e:
                    L.warning('Message deserialization failed: {}"'.format(e))
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
            L.info("Waiting for on_receive callbacks to finish...")
            # Block for `on_receive_timeout` for each thread that isn't finished
            [ ct.join(timeout=on_receive_timeout) for ct in callback_threads ]
            # Now see if any threads are still alive (didn't exit after `on_receive_timeout`)
            alive_threads = [ at for at in callback_threads if at.is_alive() ]
            for at in alive_threads:
                L.warning('{0.name}-{0.ident} never exited and is still running'.format(at))

        L.debug("Closing consumer...")
        self.close()
        L.info("Done consuming")


class EasyMultiConsumer(MultiConsumer, Consumer):

    def __init__(self,
                 kafka_brokers: List[str],
                 consumer_group: str,
                 kafka_topics: List[str],
                 topic_config: dict = None,
                 offset: str = None,
                 kafka_conf: dict = None) -> None:

        kafka_conf = kafka_conf or {}

        topic_config = topic_config or {}
        if topic_config:
            L.warning("topic_config is deprecated. Put these info kafka_conf")
            kafka_conf.update(topic_config)

        self.kafka_topics = kafka_topics

        # A simplier way to set the topic offset
        if offset is not None and 'auto.offset.reset' not in kafka_conf:
            kafka_conf['auto.offset.reset'] = offset

        conf = {
            'bootstrap.servers': ','.join(kafka_brokers),
            'client.id': self.__class__.__name__,
            'group.id': consumer_group,
            'api.version.request': 'true',
            'enable.partition.eof': True,
        }

        super().__init__(
            {**conf, **kafka_conf}
        )


class EasyMultiAvroConsumer(MultiConsumer, AvroConsumer):

    def __init__(self,
                 schema_registry_url: str,
                 kafka_brokers: List[str],
                 consumer_group: str,
                 kafka_topics: List[str],
                 topic_config: dict = None,
                 offset: str = None,
                 kafka_conf: dict = None) -> None:

        kafka_conf = kafka_conf or {}

        topic_config = topic_config or {}
        if topic_config:
            L.warning("topic_config is deprecated. Put these info kafka_conf")
            kafka_conf.update(topic_config)

        self.kafka_topics = kafka_topics

        # A simplier way to set the topic offset
        if offset is not None and 'auto.offset.reset' not in kafka_conf:
            kafka_conf['auto.offset.reset'] = offset

        conf = {
            'bootstrap.servers': ','.join(kafka_brokers),
            'schema.registry.url': schema_registry_url,
            'client.id': self.__class__.__name__,
            'group.id': consumer_group,
            'api.version.request': 'true',
            'enable.partition.eof': True,
        }

        super().__init__(
            {**conf, **kafka_conf}
        )
