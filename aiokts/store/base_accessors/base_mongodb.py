import asyncio

import motor.motor_asyncio
from pymongo.errors import AutoReconnect, ConnectionFailure

from aiokts.store.base_accessors import BaseAccessor, ConfigurationError


class BaseMongoDbAccessor(BaseAccessor):
    DEFAULT_PORT = 27017

    def __init__(self, config, type, store, loop=None):
        super().__init__(config, type, store, loop=loop)
        self._conn = None

    async def _connect(self):
        self._conn = motor.motor_asyncio.AsyncIOMotorClient(
            self._build_connection_string(),
            io_loop=self.loop
        )
        try:
            await self.wait_db()
        except AutoReconnect as e:
            self.logger.error("Couldn't connect to db %s", self.fingerprint)
            await self.wait_db()

    async def _disconnect(self):
        if self._conn is not None:
            self._conn = None

    @property
    def db_name(self):
        return self.config.get('db')

    @property
    def db(self):
        return self._conn[self.db_name] if self._conn is not None else None

    async def ping(self):
        try:
            await self._conn.admin.command({'ping': 1})
            return True
        except ConnectionFailure:
            return False

    async def wait_db(self):
        pong = False
        while not pong:
            pong = await self.ping()
            if not pong:
                self.logger.warning('%s is unavailable. Waiting.',
                                    self.fingerprint)
                await asyncio.sleep(1.0, loop=self.loop)
        return self.db

    def check_config(self):
        super().check_config()
        if self.db_name is None:
            raise ConfigurationError('db is required for MongoDbConnector')

    def _build_connection_string(self):
        if self.has_credentials:
            s = 'mongodb://%s:%s@%s:%d/%s'
            args = (self.username, self.password, self.host, self.port,
                    self.db_name)
        else:
            s = 'mongodb://%s:%d/%s'
            args = (self.host, self.port, self.db_name)
        s %= args
        return s


async def mongo_wait_connected_on_coro(coro, *args, **kwargs):
    retry = 0
    while True:
        retry += 1
        try:
            res = await coro(*args, **kwargs)
            if retry > 1:
                print('Restored mongo connection in {}'.format(coro))
            return res
        except AutoReconnect:
            print('Waiting for mongo connection in {}'.format(coro))
            await asyncio.sleep(1.0)
