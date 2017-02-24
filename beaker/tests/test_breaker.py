#!python
# coding=utf-8
import uuid
from os.path import join as opj
from os.path import dirname as dn
from os.path import abspath as ap

from unittest import TestCase

from confluent_kafka.avro import CachedSchemaRegistryClient

from beaker import BeakerConsumer, BeakerProducer, schema

import logging
L = logging.getLogger('beaker')
L.propagate = False
L.setLevel(logging.DEBUG)
L.handlers = [logging.StreamHandler()]


class TestBeaker(TestCase):

    def setUp(self):
        c = CachedSchemaRegistryClient(url='http://localhost:50002')

        self.topic = 'beaker-testing-topic'

        rp = ap(dn(__file__))
        with open(opj(rp, 'key.avsc'), 'rt') as f:
            avro_key_schema = schema.Parse(f.read())
        with open(opj(rp, 'value.avsc'), 'rt') as f:
            avro_value_schema = schema.Parse(f.read())

        c.register(self.topic + '-key', avro_key_schema)
        c.register(self.topic + '-value', avro_value_schema)

    def test_messages(self):

        # Produce
        bp = BeakerProducer(
            schema_registry_url='http://localhost:50002',
            kafka_brokers=['localhost:50001'],
            kafka_topic=self.topic
        )
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
        bp.produce(records)

        # Consume
        bc = BeakerConsumer(
            schema_registry_url='http://localhost:50002',
            kafka_brokers=['localhost:50001'],
            consumer_group='beaker.testing',
            kafka_topic=self.topic
        )
        recieved = []

        def on_recieve(key: str, value: str) -> None:
            recieved.append((key, value))
            L.info("Recieved message")

        bc.consume(on_recieve=on_recieve)
        assert len(recieved) == 9
