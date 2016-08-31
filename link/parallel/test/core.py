# -*- coding: utf-8 -*-

from b3j0f.utils.ut import UTCase
from mock import MagicMock, patch
from unittest import main

from link.parallel.core import MapReduceMiddleware
from link.parallel.reducer import Reducer
from link.parallel.mapper import Mapper
from link.parallel.driver import Driver


class DummyStore(object):
    def __init__(self, *args, **kwargs):
        super(DummyStore, self).__init__(*args, **kwargs)

        self.data = {}

    def disconnect(self):
        raise NotImplementedError()

    def __getitem__(self, key):
        if key == 'error':
            raise KeyError(key)

        return self.data[key]

    def __setitem__(self, key, val):
        self.data[key] = val

    def __delitem__(self, key):
        if key == 'error':
            raise KeyError(key)

        del self.data[key]

    def __iter__(self):
        return iter(self.keys())

    def keys(self):
        return list(self.data.keys())


class TestCore(UTCase):
    def setUp(self):
        self.store = DummyStore()
        self.store.disconnect = MagicMock()

        self.store['some key 1'] = ('id1', 'key1', 'VAL1')
        self.store['some key 2'] = ('id1', 'key1', 'VAL2')
        self.store['some key 3'] = ('id1', 'key1', 'VAL3')
        self.store['some key 4'] = ('id1', 'key1', 'VAL4')
        self.store['error'] = None
        self.store['some key 5'] = ('id1', 'key2', 'VAL1')
        self.store['some key 6'] = ('id1', 'key2', 'VAL2')
        self.store['some key 7'] = ('id1', 'key2', 'VAL3')
        self.store['some key 8'] = ('id1', 'key2', 'VAL4')

        self.store['some key 9'] = ('id2', 'key3', 'VAL1')

        patcher1 = patch('link.parallel.core.Middleware')
        patcher2 = patch('link.parallel.core.Mapper')
        patcher3 = patch('link.parallel.core.Reducer')

        self.midcls = patcher1.start()

        self.mapcls = patcher2.start()
        self.mapcls.return_value.__class__ = Mapper

        self.reducecls = patcher3.start()
        self.reducecls.return_value.__class__ = Reducer

        self.addCleanup(patcher1.stop)
        self.addCleanup(patcher2.stop)
        self.addCleanup(patcher3.stop)

        self.midcls.get_middleware_by_uri = MagicMock(return_value=self.store)

        self.store_uri = 'STORE-URI'
        self.path = ['test', 'mr']

        self.mid = MapReduceMiddleware(self.store_uri, path=self.path)

    def test_call(self):
        expected = 'RESULT'

        drv = MagicMock()
        drv.__class__ = Driver
        drv.map = MagicMock(return_value=expected)

        self.mid.set_child_middleware(drv)

        mapper = 'MAPPER'
        reducer = 'REDUCER'
        inputs = 'INPUTS'
        reduced_keys = 'REDUCED-KEYS'

        self.mid.reduced_keys = MagicMock(return_value=reduced_keys)

        result = self.mid('id1', mapper, reducer, inputs)

        self.assertEqual(result, expected)
        self.midcls.get_middleware_by_uri.assert_called_with(self.store_uri)
        self.store.disconnect.assert_called_with()

        self.assertEqual(len(drv.map.call_args_list), 2)

        mappercall = drv.map.call_args_list[0]
        mapperargs, mapperkwargs = mappercall

        self.assertEqual(len(mapperargs), 2)
        self.assertEqual(mapperkwargs, {})

        self.assertIsInstance(mapperargs[0], Mapper)
        self.mapcls.assert_called_with(
            'id1',
            'test_mr',
            self.store_uri,
            mapper
        )
        self.assertEqual(mapperargs[1], inputs)

        self.mid.reduced_keys.assert_called_with('id1', self.store)

        reducercall = drv.map.call_args_list[1]
        reducerargs, reducerkwargs = reducercall

        self.assertEqual(len(reducerargs), 2)
        self.assertEqual(reducerkwargs, {})

        self.assertIsInstance(reducerargs[0], Reducer)
        self.reducecls.assert_called_with('id1', self.store_uri, reducer)
        self.assertEqual(reducerargs[1], reduced_keys)

        keys = self.store.keys()

        self.assertItemsEqual(keys, ['some key 9', 'error'])

    def test_reduced_keys(self):
        result = self.mid.reduced_keys('id1', self.store)

        self.assertEqual(result, set(['key1', 'key2']))


if __name__ == '__main__':
    main()
