# -*- coding: utf-8 -*-

from link.parallel.driver import Driver

from IPython.parallel import Client


class IPythonDriver(Driver):

    __protocols__ = ['ipython']

    def __init__(self, *args, **kwargs):
        super(IPythonDriver, self).__init__(*args, **kwargs)

        self._client = Client()
        self._view = self._client[:]

    def map(self, callback, inputs):
        return self._view.map(callback, inputs)
