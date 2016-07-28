# -*- coding: utf-8 -*-

from b3j0f.utils.ut import UTCase
from mock import MagicMock, patch
from unittest import main

from link.parallel.reducer import Reducer


class DummyStore(dict):
    def disconnect(self):
        raise NotImplementedError()

    def __getitem__(self, key):
        if key == 'error':
            raise KeyError(key)

        return super(DummyStore, self).__getitem__(key)


class TestReducer(UTCase):
    def setUp(self):
        self.store = DummyStore()
        self.store.disconnect = MagicMock()

        patcher = patch('link.parallel.reducer.Middleware')

        self.midcls = patcher.start()
        self.addCleanup(patcher.stop)

        self.midcls.get_middleware_by_uri = MagicMock(return_value=self.store)

        self.store_uri = 'STORE-URI'
        self.expected = 'EXPECTED'
        self.callback = MagicMock(return_value=self.expected)

        self.reducer = Reducer(self.store_uri, self.callback)

    def test_call(self):
        self.store['some key 1'] = ('KEY', 'VAL1')
        self.store['some key 2'] = ('KEY', 'VAL2')
        self.store['some key 3'] = ('KEY', 'VAL3')
        self.store['some key 4'] = ('KEY', 'VAL4')
        self.store['error'] = None

        key = 'KEY'

        values = []

        for lkey in self.store:
            if lkey != 'error':
                values.append(self.store[lkey][1])

        result = self.reducer(key)

        self.midcls.get_middleware_by_uri.assert_called_with(self.store_uri)
        self.store.disconnect.assert_called_with()
        self.callback.assert_called_with(self.reducer, key, values)
        self.assertEqual(result, self.expected)


if __name__ == '__main__':
    main()
