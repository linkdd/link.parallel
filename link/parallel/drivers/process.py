# -*- coding: utf-8 -*-

from link.parallel.driver import Driver

from multiprocessing import Pool, cpu_count

import dill


def handler(packet):
    callback, inputdata = dill.loads(packet)
    return callback(inputdata)


class MultiProcessingDriver(Driver):

    __protocols__ = ['multiprocessing']

    def __init__(self, workers=cpu_count(), *args, **kwargs):
        super(MultiProcessingDriver, self).__init__(*args, **kwargs)

        self.workers = workers
        self._pool = Pool(self.workers)

    def map(self, callback, inputs):
        packets = [
            dill.dumps((callback, inputdata))
            for inputdata in inputs
        ]

        return self._pool.map(handler, packets)
