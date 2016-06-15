# -*- coding: utf-8 -*-

from link.middleware.core import Middleware
from b3j0f.task import gettask

from link.parallel.driver import Driver
from link.parallel.mapper import Mapper
from link.parallel.reducer import Reducer


class MapReduceMiddleware(Middleware):

    __protocols__ = ['mapreduce']

    def __init__(
        self,
        backend,
        mapcb=None,
        reducecb=None,
        *args, **kwargs
    ):
        super(MapReduceMiddleware, self).__init__(*args, **kwargs)

        if mapcb is None or reducecb is None:
            raise TypeError(
                'MapReduceMiddleware must have a map and a reduce callback'
            )

        if not isinstance(backend, Driver):
            raise TypeError(
                'Backend must be a Driver, got: {0}'.format(
                    backend.__class__.__name__
                )
            )

        self._backend = backend

        if not callable(mapcb):
            mapcb = gettask(mapcb)
        if not callable(reducecb):
            reducecb = gettask(reducecb)

        self.mapcb = mapcb
        self.reducecb = reducecb

        self._stores = {}

    def __call__(self, inputs):
        self._backend.map(
            Mapper('_'.join(self.path), self._stores, self.mapcb),
            inputs
        )

        result = self._backend.map(
            Reducer(self.reducecb),
            self._stores.items()
        )

        for storename, store in self._stores.items():
            for key in store:
                del store[key]

            store.disconnect()

            del self._stores[storename]
            del store

        return result
