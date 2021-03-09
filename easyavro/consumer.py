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

    def deserialize(self, message):
        # Apply any decoding, like from an AvroConsumer
        if hasattr(self, '_serializer') and self._serializer:
            try:
                if message.value() is not None:
                    decoded_value = self._serializer.decode_message(message.value(), is_key=False)
                    message.set_value(decoded_value)
                if message.key() is not None:
                    decoded_key = self._serializer.decode_message(message.key(), is_key=True)
                    message.set_key(decoded_key)
            except SerializerError as e:
                raise SerializerError(
                    "Message de-serialization failed for message at {} [{}] offset {}: {}".format(
                        message.topic(),
                        message.partition(),
                        message.offset(),
                        e
                    )
                )

        return message

    def send(self, messages, on_receive, receive_messages_in_callback):
        callbacks = []

        if messages:
            if receive_messages_in_callback is True:
                # Call the function for each valid message
                t = threading.Thread(
                    name='EasyAvro-on_receive',
                    target=on_receive,
                    args=(messages, )
                )
                t.start()
                callbacks.append(t)
            else:
                for m in messages:
                    # Call the function for each valid message
                    t = threading.Thread(
                        name='EasyAvro-on_receive',
                        target=on_receive,
                        args=( m.key(), m.value() )
                    )
                    t.start()
                    callbacks.append(t)

        return callbacks

    def cleanup(self, callbacks, cleanup_every):
        # Periodically clean up threads to prevent the list of callback_threads
        # from becoming absolutely huge on long running Consumers
        if len(callbacks) >= cleanup_every:
            for x in callbacks:
                x.join(0)
            callbacks = [
                x for x in callbacks if x.is_alive()
            ]
            cleaned = cleanup_every - len(callbacks)
            L.info('Cleaned up {} completed threads'.format(cleaned))

    def start(self,
              on_receive: Callable[[str, str], None] = None,
              on_receive_timeout: int = None,
              timeout: int = None,
              loop: bool = True,
              num_messages: int = 1,
              receive_messages_in_callback: bool = False,
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
                            try:
                                msg = self.deserialize(msg)
                            except SerializerError as e:
                                L.warning('Message de-serialization failed: {}'.format(e))
                            else:
                                valid_messages.append(msg)

                    callback_threads += self.send(
                        valid_messages,
                        on_receive,
                        receive_messages_in_callback
                    )
                except KeyboardInterrupt:
                    raise
                finally:
                    self.cleanup(callback_threads, cleanup_every)

                if break_loop is True:
                    break

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


class EasyConsumer(BaseConsumer, Consumer):

    def __init__(self,
                 kafka_brokers: List[str],
                 consumer_group: str,
                 kafka_topics: List[str] = None,
                 kafka_topic: str = None,
                 topic_config: dict = None,
                 offset: str = None,
                 kafka_conf: dict = None,
                 schema_registry_url: str = None ) -> None:

        kafka_conf = kafka_conf or {}

        topic_config = topic_config or {}
        if topic_config:
            L.warning("topic_config is deprecated. Put these info kafka_conf")
            kafka_conf.update(topic_config)

        # Backwards compat
        if isinstance(kafka_topic, str):
            L.warning("Please use the `kafka_topics` argument (List[str])")
            self.kafka_topics = [kafka_topic]
        else:
            self.kafka_topics = kafka_topics

        if not isinstance(self.kafka_topics, list):
            raise ValueError("Please supply a `kafka_topics` (List[str]) parameter")

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

        if schema_registry_url is not None:
            conf['schema.registry.url'] = schema_registry_url

        super().__init__({**conf, **kafka_conf})


class EasyAvroConsumer(EasyConsumer, AvroConsumer):
    pass