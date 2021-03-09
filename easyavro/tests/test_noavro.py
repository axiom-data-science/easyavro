#!python
# coding=utf-8
import os
import json
import uuid
import time
from typing import List
from unittest import TestCase

import msgpack
from confluent_kafka import Message

from easyavro import EasyConsumer, EasyProducer

import logging
L = logging.getLogger('easyavro')
L.propagate = False
L.setLevel(logging.DEBUG)
L.handlers = [logging.StreamHandler()]


def psort(l):
    def sort_by_uuid(p):
        payload = p[1]
        if isinstance(payload, str):
            payload = json.loads(payload)

        if isinstance(payload, dict):
            return payload['uuid']

    return sorted(l, key=sort_by_uuid)


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
            kafka_topics=[self.topic],
            offset='earliest'
        )

        def on_receive(key: str, value: bytes) -> None:
            self.received.append((key.decode('utf-8'), msgpack.loads(value)))
            L.info("Received message")
        self.received = []
        self.on_receive = on_receive

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
        self.bc.start(
            on_receive=self.on_receive,
            timeout=1,
            loop=False
        )

        loaded_records = [ (k, msgpack.loads(v)) for (k, v) in records ]

        assert psort(self.received[len(records) * -1:]) == psort(loaded_records)


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
            kafka_topics=[self.topic],
            offset='earliest'
        )

        def on_receive(key: str, value: bytes) -> None:
            self.received.append((key.decode('utf-8'), value.decode('utf-8')))
            L.info("Received message")
        self.received = []
        self.on_receive = on_receive

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
        self.bc.start(
            on_receive=self.on_receive,
            timeout=1,
            loop=False
        )

        assert psort(self.received[len(records) * -1:]) == psort(records)

    def test_initial_wait(self):
        # Produce
        m1 = str(uuid.uuid4())
        records = [
            ('ADD',     json.dumps({ 'uuid': m1, 'properties': {'name': 'TEST 1' }})),
        ]
        self.bp.produce(records)

        # Consume
        self.bc.start(
            on_receive=self.on_receive,
            timeout=1,
            loop=False,
            initial_wait=5,
            num_messages=5
        )
        assert psort(self.received[len(records) * -1:]) == psort(records)

    def test_on_receive_timeout(self):
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
            kafka_topics=[self.topic],
            offset='earliest'
        )
        received = []

        def on_receive(key: str, value: str) -> None:
            time.sleep(10)
            raise ValueError('hi')

        bc.start(
            on_receive=on_receive,
            on_receive_timeout=1,
            timeout=1,
            loop=False,
        )
        # Our callbacks take longer than the `on_receive_timeout`
        # so we should receive nothing even though the callback
        # might finish eventually
        assert received[len(records) * -1:] == []

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

        def on_receive(messages: List[Message]) -> None:
            for m in messages:
                self.received.append((m.key().decode('utf-8'), m.value().decode('utf-8')))
                L.info("Received message")

        # Consume
        self.bc.start(
            on_receive=on_receive,
            timeout=1,
            loop=False,
            cleanup_every=2,
            num_messages=5,
            receive_messages_in_callback=True
        )
        assert psort(self.received[len(records) * -1:])  == psort(records)

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

    def test_do_not_overflow_with_batch_specified(self):
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
