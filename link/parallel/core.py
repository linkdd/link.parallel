# -*- coding: utf-8 -*-

from link.middleware.core import Middleware, register_middleware

from link.parallel.driver import Driver
from link.parallel.mapper import Mapper
from link.parallel.reducer import Reducer


@register_middleware
class MapReduceMiddleware(Middleware):

    __constraints__ = [Driver]
    __protocols__ = ['mapreduce']

    def __init__(self, store_uri=None, *args, **kwargs):
        super(MapReduceMiddleware, self).__init__(*args, **kwargs)

        self.store_uri = store_uri
        self._store = Middleware.get_middleware_by_uri(self.store_uri)

    def reduced_keys(self):
        keys = set()

        for key in self._store:
            realkey, _ = self._store[key]
            keys.add(realkey)

        return keys

    def __call__(self, mapper, reducer, inputs):
        self.get_child_middleware().map(
            Mapper('_'.join(self.path), self.store_uri, mapper),
            inputs
        )

        result = self.get_child_middleware().map(
            Reducer(self.store_uri, reducer),
            self.reduced_keys()
        )

        for key in self._store:
            print(key)
            del self._store[key]

        return result

    def __del__(self):
        self._store.disconnect()
