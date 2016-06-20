# -*- coding: utf-8 -*-

from link.middleware.core import Middleware


class Reducer(object):
    def __init__(self, store_uri, callback, *args, **kwargs):
        super(Reducer, self).__init__(*args, **kwargs)

        self.store_uri = store_uri
        self.callback = callback

    def receive(self, expectedkey):
        for key in self.store:
            realkey, val = self.store[key]

            if realkey == expectedkey:
                yield val

    def __call__(self, key):
        self.store = Middleware.get_middleware_by_uri(self.store_uri)
        result = self.callback(self, key, self.receive(key))
        self.store.disconnect()
        return result
