import aiotarantool
from aiokts.stores.base import BaseStore


class TarantoolStore(BaseStore):
    def __init__(self, config):
        tnt = aiotarantool.connect(config['host'], config['port'], config.get('user'), config.get('password'))
        super().__init__(tnt)
