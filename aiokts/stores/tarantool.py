import aiotarantool
from datetime import datetime

from aiokts.stores.base import BaseStore
import logging

logger = logging.getLogger('tarantool')


class TarantoolStore(BaseStore):
    def __init__(self, config):
        logger.debug(config)
        tnt = aiotarantool.Connection(
            config['host'],
            config['port'],
            user=config.get('user'), password=config.get('password'),
            connect_now=True
        )
        super().__init__(tnt)

    def call(self, name, *args):
        start = datetime.now()
        import os
        logger.debug('Tarantool call begin: {}, pid: {}, user: {}'.format(name, os.getpid(), self.user))

        result = super().__getattribute__('call')(name, *args)
        end = datetime.now()
        delta = end - start
        delta = delta.seconds + (delta.microseconds / (10 ** 6))
        logger.debug('Tarantool call end: {}, time: {}'.format(name, delta))

        return result

    def __getattribute__(self, name):
        if name != 'call':
            return super().__getattribute__(name)
        else:
            def wrap(name, *args):
                return TarantoolStore.call(self, name, *args)

            return wrap
