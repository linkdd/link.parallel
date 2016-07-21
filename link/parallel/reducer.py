# -*- coding: utf-8 -*-

from link.middleware.core import Middleware


class Reducer(object):
    def __init__(self, store_uri, callback, *args, **kwargs):
        super(Reducer, self).__init__(*args, **kwargs)

        self.store_uri = store_uri
        self.callback = callback

    def __call__(self, key):
        store = Middleware.get_middleware_by_uri(self.store_uri)
        values = []

        for local_key in store:
            try:
                realkey, val = store[local_key]

                if realkey == key:
                    values.append(val)

            except KeyError:
                pass

        store.disconnect()

        return self.callback(self, key, values)

