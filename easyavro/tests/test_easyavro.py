#!python
# coding=utf-8
import os
import uuid
import time
from os.path import join as opj
from os.path import dirname as dn
from os.path import abspath as ap

from unittest import TestCase

from confluent_kafka.avro import CachedSchemaRegistryClient

from easyavro import EasyAvroConsumer, EasyAvroProducer, schema

import logging
L = logging.getLogger('easyavro')
L.propagate = False
L.setLevel(logging.DEBUG)
L.handlers = [logging.StreamHandler()]


class TestEasyAvro(TestCase):

    def setUp(self):

        self.testhost = os.environ.get('EASYAVRO_TESTING_HOST', 'localhost')
        c = CachedSchemaRegistryClient(url='http://{}:4002'.format(self.testhost))

        self.topic = 'easyavro-testing-topic'

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
            kafka_topic=self.topic,
            offset='earliest'
        )

        def on_recieve(key: str, value: str) -> None:
            self.recieved.append((key, value))
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
        self.bc.consume(on_recieve=self.on_recieve, timeout=1, loop=False)
        assert self.recieved[len(records) * -1:] == records

    def test_initial_wait(self):
        # Produce
        m1 = str(uuid.uuid4())
        records = [
            ('ADD',     { 'uuid': m1, 'properties': {'name': 'TEST 1' }}),
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
            ('ADD',     { 'uuid': m1, 'properties': {'name': 'TEST 1' }}),
        ]
        self.bp.produce(records)

        # Consume
        bc = EasyAvroConsumer(
            schema_registry_url='http://{}:4002'.format(self.testhost),
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
        self.bc.consume(
            on_recieve=self.on_recieve,
            timeout=1,
            loop=False,
            cleanup_every=2
        )
        assert self.recieved[len(records) * -1:] == records
