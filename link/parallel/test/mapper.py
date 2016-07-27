# -*- coding: utf-8 -*-

from b3j0f.utils.ut import UTCase
from mock import MagicMock, patch
from unittest import main

from link.parallel.mapper import Mapper


class DummyStore(dict):
    def disconnect(self):
        pass


class TestMapper(UTCase):
    def setUp(self):
        self.store = DummyStore()
        self.store.disconnect = MagicMock()

        patcher1 = patch('link.parallel.mapper.Middleware')
        patcher2 = patch('link.parallel.mapper.uuid4')

        self.midcls = patcher1.start()
        self.uuid4 = patcher2.start()

        self.addCleanup(patcher1.stop)
        self.addCleanup(patcher2.stop)

        self.midcls.get_middleware_by_uri = MagicMock(return_value=self.store)

        self.uuid = 'UUID'
        self.prefix = 'PREFIX'
        self.store_uri = 'STORE-URI'
        self.callback = MagicMock()

        self.uuid4.configure_mock(return_value=self.uuid)
        self.mapper = Mapper(self.prefix, self.store_uri, self.callback)

    def test_emit(self):
        self.mapper.store = self.store

        self.mapper.emit('KEY', 'VALUE')

        expected = '{0}_{1}_KEY'.format(self.uuid, self.prefix)

        self.assertIn(expected, self.store)
        self.assertEqual(self.store[expected], ('KEY', 'VALUE'))

    def test_call(self):
        data = 'DATA'

        self.mapper(data)

        self.midcls.get_middleware_by_uri.assert_called_with(self.store_uri)
        self.callback.assert_called_with(self.mapper, data)
        self.store.disconnect.assert_called_with()


if __name__ == '__main__':
    main()
