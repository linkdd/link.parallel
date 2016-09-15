# -*- coding: utf-8 -*-

from aloe import step, world

from link.parallel.core import MapReduceMiddleware
from b3j0f.utils.path import lookup
import json


# make sure drivers are registered
lookup('link.kvstore.core')
lookup('link.riak.driver')
lookup('link.parallel.drivers.ipython')
lookup('link.parallel.drivers.process')


def mapfunc(mapper, item):
    mapper.emit(item['key'], item['value'])


def reducefunc(reducer, key, values):
    return (key, sum(values))


@step(r'I use the middleware "([^"]*)"')
def use_middleware(step, uri):
    world.mid = MapReduceMiddleware.get_middleware_by_uri(uri)


@step(r'I process the values "([^"]*)" in "([^"]*)"')
def process_values(step, identifier, filename):
    with open(filename) as f:
        dataset = json.load(f)

    world.result = world.mid(identifier, mapfunc, reducefunc, dataset)


@step(r'I get the value in "([^"]*)"')
def get_value(step, filename):
    with open(filename) as f:
        result = json.load(f)

    assert dict(world.result) == result
