# -*- coding: utf-8 -*-

from b3j0f.utils.ut import UTCase
from unittest import main

from link.parallel.driver import Driver


class TestBaseDriver(UTCase):
    def test_map(self):
        drv = Driver()

        callback = lambda doc: doc
        expected = [1, 2, 3, 4]
        result = list(drv.map(callback, expected))

        self.assertEqual(result, expected)


if __name__ == '__main__':
    main()
