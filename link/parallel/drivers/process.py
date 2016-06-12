# -*- coding: utf-8 -*-

from link.parallel.driver import Driver

from multiprocessing import Pool, cpu_count


class MultiProcessingDriver(Driver):

    __protocols__ = ['multiprocessing']

    def __init__(self, workers=cpu_count(), *args, **kwargs):
        super(MultiProcessingDriver, self).__init__(*args, **kwargs)

        self.workers = workers
        self._pool = Pool(self.workers)

    def map(self, callback, inputs):
        return self._pool.map(callback, inputs)
