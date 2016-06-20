# -*- coding: utf-8 -*-

from link.middleware.core import Middleware
from b3j0f.task import gettask

from link.parallel.driver import Driver
from link.parallel.mapper import Mapper
from link.parallel.reducer import Reducer


class MapReduceMiddleware(Middleware):

    __constraints__ = [Driver]
    __protocols__ = ['mapreduce']

    def __init__(
        self,
        backend,
        mapcb=None,
        reducecb=None,
        store_uri=None,
        *args, **kwargs
    ):
        super(MapReduceMiddleware, self).__init__(*args, **kwargs)

        if mapcb is None or reducecb is None:
            raise TypeError(
                'MapReduceMiddleware must have a map and a reduce callback'
            )

        self._backend = backend

        if not callable(mapcb):
            mapcb = gettask(mapcb)

        if not callable(reducecb):
            reducecb = gettask(reducecb)

        self.mapcb = mapcb
        self.reducecb = reducecb
        self.store_uri = store_uri

        self._store = Middleware.get_middleware_by_uri(self.store_uri)

    def reduced_keys(self):
        keys = set()

        for key in self._store:
            realkey, _ = self._store[key]
            keys.add(realkey)

        return keys

    def __call__(self, inputs):
        self._backend.map(
            Mapper('_'.join(self.path), self.store_uri, self.mapcb),
            inputs
        )

        result = self._backend.map(
            Reducer(self.store_uri, self.reducecb),
            self.reduced_keys()
        )

        for key in self._store:
            del self._store[key]

        return result

    def __del__(self):
        self._store.disconnect()
