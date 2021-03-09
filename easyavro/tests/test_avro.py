#!python
# coding=utf-8
import os
import uuid
import time
import json
from typing import List
from unittest import TestCase
from os.path import join as opj
from os.path import dirname as dn
from os.path import abspath as ap

from confluent_kafka import Message
from confluent_kafka.avro import CachedSchemaRegistryClient

from easyavro import EasyAvroConsumer, EasyAvroProducer, schema

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


class TestAvro(TestCase):

    def setUp(self):

        self.testhost = os.environ.get('EASYAVRO_TESTING_HOST', 'localhost')
        c = CachedSchemaRegistryClient(dict(
            url='http://{}:4002'.format(self.testhost)
        ))

        self.topic = 'easyavro-testing-topic-avro'

        rp = ap(dn(__file__))
        with open(opj(rp, 'key.avsc'), 'rt') as f:
            avro_key_schema = schema.Parse(f.read())
        with open(opj(rp, 'value.avsc'), 'rt') as f:
            avro_value_schema = schema.Parse(f.read())

        c.register(self.topic + '-key', avro_key_schema)
        c.register(self.topic + '-value', avro_value_schema)

        self.bp = EasyAvroProducer(
            schema_registry_url='http://{}:4002'.format(self.testhost),
            kafka_brokers=['{}:4001'.format(self.testhost)],
            kafka_topic=self.topic
        )

        self.bc = EasyAvroConsumer(
            schema_registry_url='http://{}:4002'.format(self.testhost),
            kafka_brokers=['{}:4001'.format(self.testhost)],
            consumer_group='easyavro.testing',
            kafka_topics=[self.topic],
            offset='earliest'
        )

        def on_receive(key: str, value: str) -> None:
            self.received.append((key, value))
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
            ('ADD',     { 'uuid': m1, 'properties': {'name': 'TEST 1' }}),
            ('UPDATE',  { 'uuid': m1, 'properties': {'name': 'TEST 1' }}),
            ('DELETE',  { 'uuid': m1, 'properties': {'name': 'TEST 1' }}),
            ('ADD',     { 'uuid': m2, 'properties': {'name': 'TEST 2' }}),
            ('UPDATE',  { 'uuid': m2, 'properties': {'name': 'TEST 2' }}),
            ('DELETE',  { 'uuid': m2, 'properties': {'name': 'TEST 2' }}),
            ('ADD',     { 'uuid': m3, 'properties': {'name': 'TEST 3' }}),
            ('UPDATE',  { 'uuid': m3, 'properties': {'name': 'TEST 3' }}),
            ('DELETE',  { 'uuid': m3, 'properties': {'name': 'TEST 3' }}),
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
            ('ADD',     { 'uuid': m1, 'properties': {'name': 'TEST 1' }}),
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
            ('ADD',     { 'uuid': m1, 'properties': {'name': 'TEST 1' }}),
        ]
        self.bp.produce(records)

        # Consume
        bc = EasyAvroConsumer(
            schema_registry_url='http://{}:4002'.format(self.testhost),
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
            ('ADD',     { 'uuid': m1, 'properties': {'name': 'TEST 1' }}),
            ('UPDATE',  { 'uuid': m1, 'properties': {'name': 'TEST 1' }}),
            ('DELETE',  { 'uuid': m1, 'properties': {'name': 'TEST 1' }}),
            ('ADD',     { 'uuid': m2, 'properties': {'name': 'TEST 2' }}),
            ('UPDATE',  { 'uuid': m2, 'properties': {'name': 'TEST 2' }}),
            ('DELETE',  { 'uuid': m2, 'properties': {'name': 'TEST 2' }}),
            ('ADD',     { 'uuid': m3, 'properties': {'name': 'TEST 3' }}),
            ('UPDATE',  { 'uuid': m3, 'properties': {'name': 'TEST 3' }}),
            ('DELETE',  { 'uuid': m3, 'properties': {'name': 'TEST 3' }}),
        ]
        self.bp.produce(records)

        def on_receive(messages: List[Message]) -> None:
            for m in messages:
                self.received.append((m.key(), m.value()))
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
        assert psort(self.received[len(records) * -1:]) == psort(records)

    def test_overflow_with_custom_config(self):
        self.bp = EasyAvroProducer(
            schema_registry_url='http://{}:4002'.format(self.testhost),
            kafka_brokers=['{}:4001'.format(self.testhost)],
            kafka_topic=self.topic,
            kafka_conf={
                'queue.buffering.max.messages': 1
            }
        )

        m1 = str(uuid.uuid4())
        records = [
            ('ADD',     { 'uuid': m1, 'properties': {'name': 'TEST 1' }}),
            ('UPDATE',  { 'uuid': m1, 'properties': {'name': 'TEST 1' }}),
        ]

        with self.assertRaises(BufferError):
            self.bp.produce(records)

    def test_do_not_overflow_with_batch_specified(self):
        self.bp = EasyAvroProducer(
            schema_registry_url='http://{}:4002'.format(self.testhost),
            kafka_brokers=['{}:4001'.format(self.testhost)],
            kafka_topic=self.topic,
            kafka_conf={
                'queue.buffering.max.messages': 1
            }
        )

        m1 = str(uuid.uuid4())
        records = [
            ('ADD',     { 'uuid': m1, 'properties': {'name': 'TEST 1' }}),
            ('UPDATE',  { 'uuid': m1, 'properties': {'name': 'TEST 1' }}),
        ]

        # This should not error now
        self.bp.produce(records, batch=1)
