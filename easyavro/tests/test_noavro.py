#!python
# coding=utf-8
import os
import json
import uuid
import time

from unittest import TestCase

import msgpack
from easyavro import EasyConsumer, EasyProducer

import logging
L = logging.getLogger('easyavro')
L.propagate = False
L.setLevel(logging.DEBUG)
L.handlers = [logging.StreamHandler()]


class TestMsgPack(TestCase):

    def setUp(self):

        self.testhost = os.environ.get('EASYAVRO_TESTING_HOST', 'localhost')

        self.topic = 'easyavro-testing-topic-noavro'

        self.bp = EasyProducer(
            kafka_brokers=['{}:4001'.format(self.testhost)],
            kafka_topic=self.topic
        )

        self.bc = EasyConsumer(
            kafka_brokers=['{}:4001'.format(self.testhost)],
            consumer_group='easyavro.testing',
            kafka_topic=self.topic,
            offset='earliest'
        )

        def on_recieve(key: str, value: bytes) -> None:
            self.recieved.append((key.decode('utf-8'), msgpack.loads(value)))
            L.info("Recieved message")
        self.recieved = []
        self.on_recieve = on_recieve

    def tearDown(self):
        del self.bp
        del self.bc

    def test_messages(self):
        # Produce
        m1 = str(uuid.uuid4())
        m2 = str(uuid.uuid4())
        m3 = str(uuid.uuid4())
        records = [
            ('ADD',     msgpack.dumps({ 'uuid': m1, 'properties': {'name': 'TEST 1' }})),
            ('UPDATE',  msgpack.dumps({ 'uuid': m1, 'properties': {'name': 'TEST 1' }})),
            ('DELETE',  msgpack.dumps({ 'uuid': m1, 'properties': {'name': 'TEST 1' }})),
            ('ADD',     msgpack.dumps({ 'uuid': m2, 'properties': {'name': 'TEST 2' }})),
            ('UPDATE',  msgpack.dumps({ 'uuid': m2, 'properties': {'name': 'TEST 2' }})),
            ('DELETE',  msgpack.dumps({ 'uuid': m2, 'properties': {'name': 'TEST 2' }})),
            ('ADD',     msgpack.dumps({ 'uuid': m3, 'properties': {'name': 'TEST 3' }})),
            ('UPDATE',  msgpack.dumps({ 'uuid': m3, 'properties': {'name': 'TEST 3' }})),
            ('DELETE',  msgpack.dumps({ 'uuid': m3, 'properties': {'name': 'TEST 3' }})),
        ]
        self.bp.produce(records)

        # Consume
        self.bc.consume(on_recieve=self.on_recieve, timeout=1, loop=False)

        loaded_records = [ (k, msgpack.loads(v)) for (k, v) in records ]

        assert self.recieved[len(records) * -1:] == loaded_records


class TestNoAvro(TestCase):

    def setUp(self):

        self.testhost = os.environ.get('EASYAVRO_TESTING_HOST', 'localhost')

        self.topic = 'easyavro-testing-topic-noavro'

        self.bp = EasyProducer(
            kafka_brokers=['{}:4001'.format(self.testhost)],
            kafka_topic=self.topic
        )

        self.bc = EasyConsumer(
            kafka_brokers=['{}:4001'.format(self.testhost)],
            consumer_group='easyavro.testing',
            kafka_topic=self.topic,
            offset='earliest'
        )

        def on_recieve(key: str, value: bytes) -> None:
            self.recieved.append((key.decode('utf-8'), value.decode('utf-8')))
            L.info("Recieved message")
        self.recieved = []
        self.on_recieve = on_recieve

    def tearDown(self):
        del self.bp
        del self.bc

    def test_messages(self):
        # Produce
        m1 = str(uuid.uuid4())
        m2 = str(uuid.uuid4())
        m3 = str(uuid.uuid4())
        records = [
            ('ADD',     json.dumps({ 'uuid': m1, 'properties': {'name': 'TEST 1' }})),
            ('UPDATE',  json.dumps({ 'uuid': m1, 'properties': {'name': 'TEST 1' }})),
            ('DELETE',  json.dumps({ 'uuid': m1, 'properties': {'name': 'TEST 1' }})),
            ('ADD',     json.dumps({ 'uuid': m2, 'properties': {'name': 'TEST 2' }})),
            ('UPDATE',  json.dumps({ 'uuid': m2, 'properties': {'name': 'TEST 2' }})),
            ('DELETE',  json.dumps({ 'uuid': m2, 'properties': {'name': 'TEST 2' }})),
            ('ADD',     json.dumps({ 'uuid': m3, 'properties': {'name': 'TEST 3' }})),
            ('UPDATE',  json.dumps({ 'uuid': m3, 'properties': {'name': 'TEST 3' }})),
            ('DELETE',  json.dumps({ 'uuid': m3, 'properties': {'name': 'TEST 3' }})),
        ]
        self.bp.produce(records)

        # Consume
        self.bc.consume(on_recieve=self.on_recieve, timeout=1, loop=False)

        assert self.recieved[len(records) * -1:] == records

    def test_initial_wait(self):
        # Produce
        m1 = str(uuid.uuid4())
        records = [
            ('ADD',     json.dumps({ 'uuid': m1, 'properties': {'name': 'TEST 1' }})),
        ]
        self.bp.produce(records)

        # Consume
        self.bc.consume(
            on_recieve=self.on_recieve,
            timeout=1,
            loop=False,
            initial_wait=5
        )
        assert self.recieved[len(records) * -1:] == records

    def test_on_recieve_timeout(self):
        # Produce
        m1 = str(uuid.uuid4())
        records = [
            ('ADD',     json.dumps({ 'uuid': m1, 'properties': {'name': 'TEST 1' }})),
        ]
        self.bp.produce(records)

        # Consume
        bc = EasyConsumer(
            kafka_brokers=['{}:4001'.format(self.testhost)],
            consumer_group='easyavro.testing',
            kafka_topic=self.topic,
            offset='earliest'
        )
        recieved = []

        def on_recieve(key: str, value: str) -> None:
            time.sleep(10)
            raise ValueError('hi')
            L.info("Recieved message")

        bc.consume(
            on_recieve=on_recieve,
            on_recieve_timeout=1,
            timeout=1,
            loop=False,
        )
        # Our callbacks take longer than the `on_recieve_timeout`
        # so we should recieve nothing even though the callback
        # might finish eventually
        assert recieved[len(records) * -1:] == []

    def test_cleanup(self):
        # Produce
        m1 = str(uuid.uuid4())
        m2 = str(uuid.uuid4())
        m3 = str(uuid.uuid4())
        records = [
            ('ADD',     json.dumps({ 'uuid': m1, 'properties': {'name': 'TEST 1' }})),
            ('UPDATE',  json.dumps({ 'uuid': m1, 'properties': {'name': 'TEST 1' }})),
            ('DELETE',  json.dumps({ 'uuid': m1, 'properties': {'name': 'TEST 1' }})),
            ('ADD',     json.dumps({ 'uuid': m2, 'properties': {'name': 'TEST 2' }})),
            ('UPDATE',  json.dumps({ 'uuid': m2, 'properties': {'name': 'TEST 2' }})),
            ('DELETE',  json.dumps({ 'uuid': m2, 'properties': {'name': 'TEST 2' }})),
            ('ADD',     json.dumps({ 'uuid': m3, 'properties': {'name': 'TEST 3' }})),
            ('UPDATE',  json.dumps({ 'uuid': m3, 'properties': {'name': 'TEST 3' }})),
            ('DELETE',  json.dumps({ 'uuid': m3, 'properties': {'name': 'TEST 3' }})),
        ]
        self.bp.produce(records)

        # Consume
        self.bc.consume(
            on_recieve=self.on_recieve,
            timeout=1,
            loop=False,
            cleanup_every=2
        )
        assert self.recieved[len(records) * -1:] == records

    def test_overflow_with_custom_config(self):
        self.bp = EasyProducer(
            kafka_brokers=['{}:4001'.format(self.testhost)],
            kafka_topic=self.topic,
            kafka_conf={
                'queue.buffering.max.messages': 1
            }
        )

        m1 = str(uuid.uuid4())
        records = [
            ('ADD',     json.dumps({ 'uuid': m1, 'properties': {'name': 'TEST 1' }})),
            ('UPDATE',  json.dumps({ 'uuid': m1, 'properties': {'name': 'TEST 1' }})),
        ]

        with self.assertRaises(BufferError):
            self.bp.produce(records)

    def test_dont_overflow_with_batch_specified(self):
        self.bp = EasyProducer(
            kafka_brokers=['{}:4001'.format(self.testhost)],
            kafka_topic=self.topic,
            kafka_conf={
                'queue.buffering.max.messages': 1
            }
        )

        m1 = str(uuid.uuid4())
        records = [
            ('ADD',     json.dumps({ 'uuid': m1, 'properties': {'name': 'TEST 1' }})),
            ('UPDATE',  json.dumps({ 'uuid': m1, 'properties': {'name': 'TEST 1' }})),
        ]

        # This should not error now
        self.bp.produce(records, batch=1)