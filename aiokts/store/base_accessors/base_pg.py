import asyncpg
import asyncpgsa
from aiokts.store.base_accessors import BaseAccessor


class BasePgAccessor(BaseAccessor):
    DEFAULT_PORT = 5432

    def __init__(self, config, type, store, loop=None):
        super().__init__(config, type, store, loop=loop)
        self._pool = None

    @property
    def db_name(self):
        return self.config['db']

    async def acquire(self):
        return await self._pool.acquire()

    async def release(self, conn):
        return await self._pool.release(conn)

    async def _connect(self):
        pool_min_size = self.config.get('pool_min', 1)
        pool_max_size = self.config.get('pool_max', 1)
        self._pool = await asyncpg.create_pool(host=self.host,
                                               port=self.port,
                                               user=self.username,
                                               password=self.password,
                                               database=self.db_name,
                                               loop=self.loop,
                                               min_size=pool_min_size,
                                               max_size=pool_max_size)

    async def _disconnect(self):
        return await self._pool.close()

    def compile_q(self, q):
        return asyncpgsa.compile_query(q)

    async def _execute_operation(self, operation, query, conn=None,
                                 *args, **kwargs):
        q, q_args = self.compile_q(query)
        if conn:
            return await getattr(conn, operation)(str(q), *q_args)
        else:
            async with self._pool.acquire() as conn:
                return await getattr(conn, operation)(str(q), *q_args)

    async def execute(self, q, conn=None, *args, **kwargs):
        return await self._execute_operation("execute", q, conn,
                                             *args, **kwargs)

    async def fetchrow(self, q, conn=None, *args, **kwargs):
        return await self._execute_operation("fetchrow", q, conn,
                                             *args, **kwargs)

    async def exists(self, q, conn=None, *args, **kwargs):
        return bool(await self.fetch(q, conn, *args, **kwargs))

    async def fetch(self, q, conn=None, *args, **kwargs):
        return await self._execute_operation("fetch", q, conn,
                                             *args, **kwargs)

    @staticmethod
    def transaction(conn, **kwargs):
        return conn.transaction(**kwargs)
