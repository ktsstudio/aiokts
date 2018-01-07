import asyncio
import builtins

import asyncio_redis
from aiokts.store.base_accessors import BaseAccessor, BaseAccessorException
from aiokts.util.json_utils import json_dumps, json_loads


class AccessorNotConnectedError(BaseAccessorException):
    pass


class BaseRedisAccessor(BaseAccessor):
    DEFAULT_PORT = 6379

    def __init__(self, config, type, store, loop=None):
        super().__init__(config, type, store, loop)
        self._db = int(self.config.get('db', 0))
        self._conn = None

    @property
    def conn(self):
        return self._conn

    async def get_conn(self, conn_required=False, *args, **kwargs):
        if conn_required and not (await self.check_status()):
            raise AccessorNotConnectedError(
                '{} not connected'.format(self.fingerprint))
        return self.conn

    @property
    def db(self):
        return self._db

    async def _connect(self):
        pool_size = int(self.config.get('poolsize', 1))

        self._conn = await asyncio_redis.Pool.create(
            host=self.host,
            port=self.port,
            poolsize=pool_size,
            db=self.db,
            loop=self.loop)

    async def _disconnect(self):
        self._conn.close()
        await asyncio.sleep(0.0, loop=self.loop)
        self._conn = None

    async def check_status(self):
        connected_count = self._conn.connections_connected
        if not isinstance(connected_count, int):
            connected_count = 0
        if connected_count > 0:
            res = await self.conn.ping()
            return res.lower() == 'pong'
        return False

    async def keys(self, pattern, *, conn=None) -> list:
        conn = conn if conn else self._conn
        res = await conn.keys(pattern)
        res = await res.aslist()
        return res

    async def expire(self, key, seconds: int, *, conn=None) -> int:
        conn = conn if conn else self._conn
        return await conn.expire(key, seconds)

    async def set_raw(self, key, value, expire=None, *, conn=None):
        conn = conn if conn else self._conn
        return await conn.set(key, value, expire)

    async def get_raw(self, key, *, conn=None):
        conn = conn if conn else self._conn
        return await conn.get(key)

    async def set(self, key, value, expire=None, *, conn=None):
        if isinstance(value, bool):
            value = str(int(value))
        elif isinstance(value, dict) or isinstance(value, list):
            value = json_dumps(value)
        else:
            value = str(value)
        return await self.set_raw(key, value, expire, conn=conn)

    async def get(self, key, type=str, *, conn=None):
        value = await self.get_raw(key, conn=conn)
        if value is None:
            return None

        if type == bool:
            return bool(int(value))
        elif type == dict or type == list:
            return json_loads(value)
        else:
            return type(value)

    async def delete(self, keys, *, conn=None) -> int:
        conn = conn if conn else self._conn
        return await conn.delete(keys)

    async def incr(self, key, *, conn=None) -> int:
        conn = conn if conn else self._conn
        return await conn.incr(key)

    async def incrby(self, key, increment=1, *, conn=None):
        conn = conn if conn else self._conn
        return await conn.incrby(key, increment)

    async def decr(self, key, *, conn=None) -> int:
        conn = conn if conn else self._conn
        return await conn.decr(key)

    async def llen(self, key, *, conn=None) -> int:
        conn = conn if conn else self._conn
        return await conn.llen(key)

    async def lrange(self, key, start: int = 0, stop: int = -1, *, conn=None) -> list:
        conn = conn if conn else self._conn
        res = await conn.lrange(key, start, stop)
        if res is None:
            return None
        res = await res.aslist()
        return res

    async def lpush(self, key, values, *, conn=None) -> int:
        conn = conn if conn else self._conn
        return await conn.lpush(key, values)

    async def rpush(self, key, values, *, conn=None) -> int:
        conn = conn if conn else self._conn
        return await conn.rpush(key, values)

    async def hgetall(self, key, *, conn=None) -> dict:
        conn = conn if conn else self._conn
        res = await conn.hgetall(key)
        if res is None:
            return None
        res = await res.asdict()
        return res

    async def hmset(self, key, values: dict, conn=None):
        conn = conn if conn else self._conn
        return await conn.hmset(key, values)

    async def sadd(self, key, members: list, *, conn=None) -> int:
        conn = conn if conn else self._conn
        return await conn.sadd(key, members)

    async def sunion(self, keys: list, *, conn=None) -> builtins.set:
        conn = conn if conn else self._conn
        res = await conn.sunion(keys)
        if res is None:
            return None
        res = await res.asset()
        return res

    async def zadd(self, key, values: dict, *, conn=None) -> int:
        conn = conn if conn else self._conn
        return await conn.zadd(key, values)

    async def zrem(self, key, members: list, *, conn=None) -> int:
        conn = conn if conn else self._conn
        return await conn.zrem(key, members)

    async def zcard(self, key, *, conn=None) -> int:
        conn = conn if conn else self._conn
        return await conn.zcard(key)

    async def zscore(self, key, member, *, conn=None) -> (float, type(None)):
        assert member is not None
        conn = conn if conn else self._conn
        return await conn.zscore(key, str(member))

    async def zrange(self, key, start: int = 0, stop: int = -1, *, conn=None) -> dict:
        conn = conn if conn else self._conn
        res = await conn.zrange(key, start, stop)
        if res is None:
            return None
        res = await res.asdict()
        return res

    async def zrange_aslist(self, key, start: int = 0, stop: int = -1, *,
                      conn=None) -> list:
        conn = conn if conn else self._conn
        res = await conn.zrange(key, start, stop)
        if res is None:
            return None

        res_list = []
        for f in res:
            key, value = await f
            res_list.append(key)
        return res_list

    async def zrevrangebyscore(self, key,
                         max: asyncio_redis.ZScoreBoundary = asyncio_redis.ZScoreBoundary.MAX_VALUE,
                         min: asyncio_redis.ZScoreBoundary = asyncio_redis.ZScoreBoundary.MIN_VALUE,
                         offset: int = 0, limit: int = -1,
                         *, conn=None) -> dict:
        conn = conn if conn else self._conn
        res = await conn.zrevrangebyscore(key, max, min, offset, limit)
        if res is None:
            return None
        res = await res.asdict()
        return res

    async def zrevrangebyscore_list(self, key,
                              max: asyncio_redis.ZScoreBoundary = asyncio_redis.ZScoreBoundary.MAX_VALUE,
                              min: asyncio_redis.ZScoreBoundary = asyncio_redis.ZScoreBoundary.MIN_VALUE,
                              offset: int = 0, limit: int = -1,
                              *, conn=None) -> list:
        conn = conn if conn else self._conn
        res = await conn.zrevrangebyscore(key, max, min, offset, limit)
        if res is None:
            return None

        res_list = []
        for f in res:
            key, value = await f
            res_list.append((key, value))

        return res_list

    async def zremrangebyrank(self, name, min, max, conn=None):
        conn = conn if conn else self._conn
        return await conn.zremrangebyrank(name, min, max)

    async def exists(self, name, conn=None):
        conn = conn if conn else self._conn
        return await conn.exists(name)
