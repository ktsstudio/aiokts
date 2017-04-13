import asyncio
import asyncio.futures
import weakref

import pymysql
from io import StringIO
from aiomysql.sa import create_engine
import sqlalchemy as sa

from aiokts.store.base_accessors import BaseAccessor
from aiokts.store.base_accessors.mysql_utils import *


class BaseMySQLAccessor(BaseAccessor):
    DEFAULT_PORT = 3306

    def __init__(self, config, type, store, loop=None):
        super().__init__(config, type, store, loop=loop)
        self.engine = None

        self.connect_timeout = float(self.config.get('connect_timeout', 5.0))
        self.reconnect_timeout = float(
            self.config.get('reconnect_timeout', 1.0))
        self.request_timeout = float(self.config.get('request_timeout', 15.0))
        self.trx_commit_timeout = float(
            self.config.get('trx_commit_timeout', 60.0))

    async def _connect(self):
        while True:
            try:
                def mysql_create_engine(self):
                    return create_engine(
                        minsize=int(self.config.get('pool_size', 10)),
                        maxsize=int(self.config.get('pool_size', 10)),
                        host=self.host,
                        port=self.port,
                        user=self.username,
                        password=self.password,
                        db=self.config['db'],
                        charset='utf8mb4',
                        loop=self.loop,
                        autocommit=True
                    )

                def create_engine_finished(f: asyncio.Future):
                    if f.cancelled():
                        return
                    exc = f.exception()  # need to retrieve exception

                mysql_create_engine_coro = asyncio.ensure_future(
                    mysql_create_engine(self), loop=self.loop)
                mysql_create_engine_coro.add_done_callback(
                    create_engine_finished)
                self.engine = await asyncio.wait_for(
                    mysql_create_engine_coro, self.connect_timeout,
                    loop=self.loop)
                if self.engine is None:
                    await asyncio.sleep(self.reconnect_timeout, loop=self.loop)
                    continue
                setattr(self.engine, '_connector_', weakref.ref(self))
                break
            except Exception as e:
                self.logger.error(
                    '%s Cannot connect to MySQL '
                    '(reconnecting in %s seconds): %s|%s',
                    self.fingerprint, self.reconnect_timeout, repr(e), str(e))
                await asyncio.sleep(self.reconnect_timeout, loop=self.loop)

    async def _disconnect(self):
        if self.engine is not None:
            self.engine.close()
            await self.engine.wait_closed()

    async def ping(self):
        try:
            async with self.acquire() as conn:
                ping = await conn.connection.ping()
        except Exception as e:
            ping = False
        if ping is None:
            ping = True
        return ping

    @property
    def dsn(self):
        return 'mysql+pymysql://{}:{}@{}:{}/{}'.format(
            self.username, self.password, self.host, self.port,
            self.config.get('db')
        )

    @property
    def fingerprint(self):
        return '[{}://{}:{}]'.format(self.type, self.host, self.port)

    def _check_engine(self):
        if self.engine is None:
            self.logger.error('%s MySQL not connected', self.fingerprint)
            raise MySQLNotConnectedException('MySQL not connected')

    @property
    async def conn(self):
        self._check_engine()

        def coro_finished(f: asyncio.Future):
            if f.cancelled():
                self.logger.warning('acquiring connection is cancelled')
                return
            e = f.exception()
            if e is not None and not isinstance(e,
                                                pymysql.err.OperationalError):
                self.logger.exception(
                    'Exception happened while acquiring connection: %s',
                    str(e), exc_info=e)

        coro = asyncio.ensure_future(self.engine.acquire())
        coro.add_done_callback(coro_finished)
        try:
            res = await asyncio.wait_for(asyncio.shield(coro),
                                         self.request_timeout,
                                         loop=self.loop)
        except asyncio.futures.TimeoutError as e:
            raise MySQLTimeoutError('Timeout error') from e
        return res

    def get_conn(self, *args, **kwargs):
        return self.conn

    def acquire(self):
        return MySQLConnectionCoroContextManager(self, self.conn)

    def release(self, conn):
        self._check_engine()
        return self.engine.release(conn)

    @mysql_connected_check
    def __iter__(self):
        conn = yield from self.conn
        return MySQLConnectionContextManager(self, conn)

    @supply_mysql_conn
    @mysql_connected_check
    async def execute(self, query, *multiparams, **params):
        """
            Используется для неявной передачи conn в одном месте
        """
        conn = params.pop('conn')
        if conn is None:
            raise MySQLNotConnectedException('MySQL not connected')

        def coro_finished(f: asyncio.Future):
            if f.cancelled():
                self.logger.warning(
                    'execute of query is cancelled. q: %s [%s, %s]',
                    query, multiparams, params)
                return
            e = f.exception()
            if e is not None and not isinstance(e,
                                                pymysql.err.OperationalError):
                self.logger.error(
                    'Exception happened while '
                    'executing query \'%s\': %s', query, str(e),
                    exc_info=e)

        coro = asyncio.ensure_future(
            conn.execute(query, *multiparams, **params),
            loop=self.loop
        )
        coro.add_done_callback(coro_finished)
        try:
            res = await coro
            return res
        except asyncio.futures.TimeoutError as e:
            raise MySQLTimeoutError('Timeout error') from e
        except pymysql.InternalError as e:
            err_code, err_msg = e.args
            if err_code == MySQLError.DEADLOCK:
                self.logger.error(
                    'Deadlock happened while executing query %s: %s',
                    query, e.args)
                raise DeadlockError() from e
            raise e

    @mysql_connected_check
    async def begin(self, conn, *args, **kwargs):
        if conn is None:
            raise MySQLNotConnectedException('MySQL not connected')

        def coro_finished(f: asyncio.Future):
            if f.cancelled():
                self.logger.warning('begin of trx is cancelled')
                return
            e = f.exception()
            if e is not None and not isinstance(e,
                                                pymysql.err.OperationalError):
                self.logger.error(
                    'Exception happened while beginning trx: %s',
                    str(e), exc_info=e)

        coro = asyncio.ensure_future(conn.begin(*args, **kwargs),
                                     loop=self.loop)
        coro.add_done_callback(coro_finished)
        try:
            res = await coro
            return res
        except asyncio.futures.TimeoutError as e:
            raise MySQLTimeoutError('Timeout error') from e

    @mysql_connected_check
    async def commit(self, trx, *args, **kwargs):
        if trx is None:
            raise ValueError('trx must be not None')

        def coro_finished(f: asyncio.Future):
            if f.cancelled():
                self.logger.warning('commit of trx is cancelled')
                return
            e = f.exception()
            if e is not None and not isinstance(e,
                                                pymysql.err.OperationalError):
                self.logger.error(
                    'Exception happened while committing trx: %s', str(e),
                    exc_info=e)

        coro = asyncio.ensure_future(trx.commit(*args, **kwargs),
                                     loop=self.loop)
        coro.add_done_callback(coro_finished)
        try:
            res = await coro
        except asyncio.futures.TimeoutError as e:
            raise MySQLTimeoutError('Timeout error') from e

        return res

    @mysql_connected_check
    async def rollback(self, trx, *args, **kwargs):
        if trx is None:
            raise ValueError('trx must be not None')

        def coro_finished(f: asyncio.Future):
            if f.cancelled():
                self.logger.warning('rollback of trx is cancelled')
                return
            e = f.exception()
            if e is not None and not isinstance(e,
                                                pymysql.err.OperationalError):
                self.logger.error(
                    'Exception happened while running rollback trx: %s',
                    str(e), exc_info=e)

        coro = asyncio.ensure_future(trx.rollback(*args, **kwargs),
                                     loop=self.loop)
        coro.add_done_callback(coro_finished)
        try:
            res = await coro
        except asyncio.futures.TimeoutError as e:
            raise MySQLTimeoutError('Timeout error') from e

        return res

    @supply_persist_conn_trx
    @mysql_connected_check
    async def execute_trx(self, query, *multiparams, **params):
        """
            Используется для неявной передачи conn в одном месте
        """
        return await self.execute(query, *multiparams, **params)

    @supply_mysql_conn
    @mysql_connected_check
    async def fetch_all(self, query, *multiparams, **params):
        res = await self.execute(query, *multiparams, **params)
        return await res.fetchall()

    @supply_mysql_conn
    @mysql_connected_check
    async def fetch_by_chunks(self, query, *multiparams, **params):
        chunks_size = params.pop('chunks_size', 50)
        res = await self.execute(query, *multiparams, **params)
        res_list = []
        res_list_part = await res.fetchmany(size=chunks_size)
        while len(res_list_part) > 0:
            res_list += res_list_part
            res_list_part = await res.fetchmany(size=chunks_size)
        return res_list

    @supply_mysql_conn
    @mysql_connected_check
    async def fetch_first(self, query, *multiparams, **params):
        res = await self.execute(query, *multiparams, **params)
        return await res.first()

    @staticmethod
    def dump_sql(func, bind=False):
        @functools.wraps(func)
        def func_wrapper(*args, **kwargs):
            out = StringIO()

            def dump(sql, *multiparams, **params):
                out.write('{};\n'.format(
                    str(sql.compile(dialect=dump.dialect)).strip()))

            engine = sa.create_engine('mysql://', strategy='mock',
                                      executor=dump)
            dump.dialect = engine.dialect

            if bind:
                func(*args, bind=engine, **kwargs)
            else:
                func(engine, *args, **kwargs)

            return out.getvalue()

        return func_wrapper
