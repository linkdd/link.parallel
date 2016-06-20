# -*- coding: utf-8 -*-

from link.middleware.core import Middleware
from uuid import uuid4


class Mapper(object):
    def __init__(self, prefix, store_uri, callback, *args, **kwargs):
        super(Mapper, self).__init__(*args, **kwargs)

        self.prefix = prefix
        self.store_uri = store_uri
        self.callback = callback

    def emit(self, key, value):
        h = '{0}_{1}_{2}'.format(
            uuid4(),
            self.prefix,
            key
        )

        self.store[h] = (key, value)

    def __call__(self, data):
        self.store = Middleware.get_middleware_by_uri(self.store_uri)
        self.callback(self, data)
        self.store.disconnect()
