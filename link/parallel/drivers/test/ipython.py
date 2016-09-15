# -*- coding: utf-8 -*-

from b3j0f.utils.ut import UTCase
from mock import MagicMock, patch
from unittest import main

from link.parallel.drivers.ipython import IPythonDriver


class TestIPythonDriver(UTCase):
    def setUp(self):
        patcher = patch('link.parallel.drivers.ipython.Client')
        self.client = patcher.start()
        self.addCleanup(patcher.stop)

        self.client.__getitem__ = MagicMock(return_value=MagicMock())

        self.drv = IPythonDriver()
        self.drv._view = MagicMock()

        self.callback = lambda doc: doc
        self.expected = [1, 2, 3, 4]

        self.drv._view.map_sync = MagicMock(return_value=self.expected)

    def test_map(self):
        result = list(self.drv.map(self.callback, self.expected))

        self.drv._view.map_sync.assert_called_with(
            self.callback,
            self.expected
        )
        self.assertEqual(result, self.expected)


if __name__ == '__main__':
    main()
