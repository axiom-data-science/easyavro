#!python
# coding=utf-8
from .consumer import EasyAvroConsumer
from .producer import EasyAvroProducer, schema

__version__ = "2.2.0"


__all__ = [
    'EasyAvroConsumer',
    'EasyAvroProducer',
    'schema'
]
