# -*- coding: utf-8 -*-

from link.middleware.core import Middleware


class Driver(Middleware):

    __protocols__ = ['parallel']

    def map(self, callback, inputs):
        return map(callback, inputs)
