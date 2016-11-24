import aiotarantool
from aiokts.stores.base import BaseStore


class TarantoolStore(BaseStore):
    def __init__(self, config):
        tnt = aiotarantool.connect(config['host'], config['port'])
        super().__init__(tnt)
