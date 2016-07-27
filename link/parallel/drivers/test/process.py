# -*- coding: utf-8 -*-

from b3j0f.utils.ut import UTCase
from mock import MagicMock
from unittest import main

from link.parallel.drivers.process import MultiProcessingDriver, handler
import dill


class TestMultiProcessingDriver(UTCase):
    def setUp(self):
        self.drv = MultiProcessingDriver()
        self.drv._pool = MagicMock()

        self.callback = lambda doc: doc
        self.expected = [1, 2, 3, 4]

        self.drv._pool.map = MagicMock(return_value=self.expected)

    def test_map(self):
        packets = [
            dill.dumps((self.callback, inputdata))
            for inputdata in self.expected
        ]

        result = list(self.drv.map(self.callback, self.expected))

        self.drv._pool.map.assert_called_with(handler, packets)
        self.assertEqual(result, self.expected)

    def test_handler(self):
        packet = dill.dumps((self.callback, self.expected[0]))

        result = handler(packet)

        self.assertEqual(result, self.expected[0])


if __name__ == '__main__':
    main()
