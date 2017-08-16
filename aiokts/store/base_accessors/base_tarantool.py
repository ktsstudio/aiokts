import asyncio

import asynctnt

from aiokts.store.base_accessors import BaseAccessor


class BaseTarantoolAccessor(BaseAccessor):
    DEFAULT_PORT = 3301

    def __init__(self, config, type, store, loop=None):
        super().__init__(config, type, store, loop=loop)
        self._conn = asynctnt.Connection(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            loop=self.loop
        )

    async def _connect(self):
        await self._conn.connect()

    async def _disconnect(self):
        if self._conn:
            await self._conn.disconnect()

    @property
    def conn(self):
        return self._conn
