# -*- coding: utf-8 -*-


class Mapper(object):
    def __init__(self, partitions, callback, *args, **kwargs):
        super(Mapper, self).__init__(*args, **kwargs)

        self.partitions = partitions
        self.callback = callback

    def emit(self, key, value):
        self.partitions.append_to_key(key, value)

    def __call__(self, data):
        self.callback(self, data)
