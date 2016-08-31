# -*- coding: utf-8 -*-

from link.middleware.core import Middleware
from uuid import uuid4


class Mapper(object):
    def __init__(self, identifier, prefix, store_uri, cb, *args, **kwargs):
        super(Mapper, self).__init__(*args, **kwargs)

        self.identifier = identifier
        self.prefix = prefix
        self.store_uri = store_uri
        self.callback = cb

    def emit(self, key, value):
        h = '{0}_{1}_{2}'.format(
            uuid4(),
            self.prefix,
            key
        )

        self.store[h] = (self.identifier, key, value)

    def __call__(self, data):
        self.store = Middleware.get_middleware_by_uri(self.store_uri)
        self.callback(self, data)
        self.store.disconnect()
