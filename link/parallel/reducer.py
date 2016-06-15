# -*- coding: utf-8 -*-


class Reducer(object):
    def __init__(self, callback, *args, **kwargs):
        super(Reducer, self).__init__(*args, **kwargs)

        self.callback = callback

    def receive(self, store):
        for key in store:
            yield store[key]

    def __call__(self, key, store):
        self.callback(self, key, self.receive(store))
