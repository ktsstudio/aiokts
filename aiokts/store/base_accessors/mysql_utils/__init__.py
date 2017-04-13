import asyncio
import enum
import functools

import pymysql

from aiokts.store.base_accessors import BaseAccessorException


class MySQLAccessorException(BaseAccessorException):
    pass


class MySQLNotConnectedException(MySQLAccessorException):
    pass


class MySQLTimeoutError(MySQLAccessorException):
    pass


class MySQLError:
    DUPLICATE_KEY = 1062
    DEADLOCK = 1213


class DeadlockError(Exception):
    """
        Это исключение кидается execute'ом в случае возникновения дедлока, чтобы
        вызывающий код мог порестартить транзакцию
    """
    pass


class MySQLConnectionContextManager:
    """Context manager.

    This enables the following idiom for acquiring and releasing a
    connection around a block:

        with (yield from engine) as conn:
            cur = yield from conn.cursor()

    while failing loudly when accidentally using:

        with engine:
            <block>
    """

    __slots__ = ('_connector', '_conn')

    def __init__(self, connector, conn):
        self._connector = connector
        self._conn = conn

    def __enter__(self):
        assert self._conn is not None
        return self._conn

    def __exit__(self, *args):
        try:
            self._connector.release(self._conn)
        finally:
            self._connector = None
            self._conn = None

    # if PY_35:  # pragma: no branch
    #     @asyncio.coroutine
    #     def __aenter__(self):
    #         self._conn = yield from self._coro
    #         return self._conn
    #
    #     @asyncio.coroutine
    #     def __aexit__(self, exc_type, exc, tb):
    #         try:
    #             yield from self._pool.release(self._conn)
    #         finally:
    #             self._pool = None
    #             self._conn = None


class MySQLConnectionCoroContextManager:
    """Context manager.

    This enables the following idiom for acquiring and releasing a
    connection around a block:

        with (yield from engine) as conn:
            cur = yield from conn.cursor()

    while failing loudly when accidentally using:

        with engine:
            <block>
    """

    __slots__ = ('_connector', '_conn', '_coro')

    def __init__(self, connector, coro):
        self._connector = connector
        self._coro = coro
        self._conn = None

    async def __aenter__(self):
        self._conn = await self._coro
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        try:
            self._connector.release(self._conn)
        finally:
            self._connector = None
            self._conn = None


def mysql_connected_check(f):
    @functools.wraps(f)
    async def wrap(self, *args, **kwargs):
        try:
            return await f(self, *args, **kwargs)
        except pymysql.err.OperationalError as e:
            self.logger.error('%s Cannot connect to MySQL: %s',
                              self.fingerprint, str(e))
            raise MySQLNotConnectedException(str(e)) from e

    return wrap


def supply_mysql_conn(method):
    @functools.wraps(method)
    async def wrap(self, *args, **kwargs):
        conn = kwargs.get('conn')
        self_conn = conn is None

        if hasattr(self, 'persist'):
            connector = self.persist
        elif hasattr(self, 'get_conn'):
            connector = self
        else:
            raise AttributeError('No connector found')

        try:
            if self_conn:
                conn = await connector.get_conn(mode=Mode.any)
                kwargs['conn'] = conn
            try:
                if asyncio.iscoroutinefunction(method):
                    return await method(self, *args, **kwargs)
                else:
                    return await asyncio.coroutine(method)(
                        self, *args, **kwargs)
            finally:
                if self_conn:
                    connector.release(conn)
        except pymysql.err.OperationalError as e:
            self.logger.error('%s Cannot connect to MySQL: %s',
                              connector.fingerprint, str(e))
            raise MySQLAccessorException(str(e)) from e

    return asyncio.coroutine(wrap)


def supply_persist_conn_trx(method):
    @functools.wraps(method)
    async def wrap(self, *args, **kwargs):
        conn = kwargs.get('conn')
        self_conn = conn is None

        if hasattr(self, 'persist'):
            connector = self.persist
        elif hasattr(self, 'get_conn'):
            connector = self
        else:
            raise AttributeError('No connector found')

        try:
            max_attempts = 3
            for attempt in range(1, max_attempts + 1):
                if attempt > 1:
                    self.logger.warning('Retry #%s transaction for func=%s',
                                        attempt - 1, method)

                trans = None
                if self_conn:
                    conn = await connector.get_conn(mode=Mode.rw)

                    conn.__connector__ = connector
                    trans = await connector.begin(conn=conn)
                    kwargs['conn'] = conn
                try:
                    if asyncio.iscoroutinefunction(method):
                        result = await method(self, *args, **kwargs)
                    else:
                        result = await asyncio.coroutine(method)(
                            self, *args, **kwargs)
                except DeadlockError:
                    if self_conn and conn.in_transaction:
                        await connector.rollback(trans)
                    if not self_conn:
                        raise
                    if attempt >= max_attempts:
                        raise
                except Exception as e:
                    if self_conn and conn.in_transaction:
                        await connector.rollback(trans)
                    raise e
                else:
                    if self_conn and conn.in_transaction:
                        await connector.commit(trans)
                    return result
                finally:
                    if self_conn and not getattr(conn, '__released__', False):
                        connector.release(conn)

        except pymysql.err.OperationalError as e:
            self.logger.error('%s OperationalError: %s',
                              connector.fingerprint, str(e))
            raise MySQLAccessorException(str(e)) from e

    return asyncio.coroutine(wrap)


class Mode(enum.Enum):
    rw = 'rw'
    ro = 'ro'
    any = 'any'


@asyncio.coroutine
def persist_conn_commit(conn):
    if conn is not None and conn.in_transaction:
        if hasattr(conn, '__connector__'):
            connector = conn.__connector__
            if connector is not None:
                trx = conn._transaction
                yield from connector.commit(trx)


@asyncio.coroutine
def persist_conn_rollback(conn):
    if conn is not None and conn.in_transaction:
        if hasattr(conn, '__connector__'):
            connector = conn.__connector__
            if connector is not None:
                trx = conn._transaction
                yield from connector.rollback(trx)


def persist_conn_release(conn):
    if conn is not None:
        connector = conn.__connector__
        if connector is not None:
            connector.release(conn)
            conn.__released__ = True
