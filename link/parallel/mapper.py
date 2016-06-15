# -*- coding: utf-8 -*-

from b3j0f.conf import Configurable, category, Parameter
from link.middleware.core import Middleware

from link.parallel import CONF_BASE_PATH

from six.moves.urllib.parse import quote_plus


@Configurable(
    paths='{0}/mapper.conf'.format(CONF_BASE_PATH),
    conf=category(
        'MAPPER',
        Parameter(name='kvstore_template')
    )
)
class Mapper(object):
    def __init__(self, prefix, stores, callback, *args, **kwargs):
        super(Mapper, self).__init__(*args, **kwargs)

        self.prefix = prefix
        self.stores = stores
        self.callback = callback

    def emit(self, key, value):
        h = hash(value)
        storename = '{0}_{1}'.format(self.prefix, key)

        if storename not in self.stores:
            self.stores[storename] = Middleware.get_middleware_by_uri(
                self.kvstore_template.format(
                    path=quote_plus(storename)
                )
            )

        self.stores[storename][h] = value

    def __call__(self, data):
        self.callback(self, data)
