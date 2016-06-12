# -*- coding: utf-8 -*-

from b3j0f.conf import Configurable, category, Parameter
from link.middleware.core import Middleware
from link.parallel import CONF_BASE_PATH

from link.dbrequest.assignment import A
from link.dbrequest.comparison import C
from link.dbrequest.expression import E


@Configurable(
    paths='{0}/partitionner.conf'.format(CONF_BASE_PATH),
    conf=category(
        'PARTITIONNER',
        Parameter(name='store', parser=Middleware.get_middleware_by_uri)
    )
)
class PartitionStore(object):
    def __init__(self, name, *args, **kwargs):
        super(PartitionStore, self).__init__(*args, **kwargs)

        self.name = name

    def append_to_key(self, key, value):
        self.store.create(
            A('store', self.name),
            A('key', key),
            A('value', value)
        )

    def items(self):
        result = self.store.all().filter(
            C('store') == self.name
        ).group_by(
            E('key')
        )

        return iter(result)

    def clear(self):
        self.store.all().filter(C('store') == self.name).delete()
