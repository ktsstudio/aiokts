import functools


def with_pg_connection(method):
    @functools.wraps(method)
    async def wrap(self, *args, **kwargs):
        if kwargs.get("conn"):
            return await method(self, *args, **kwargs)

        conn = await self.store.pg.acquire()
        kwargs["conn"] = conn
        try:
            return await method(self, *args, **kwargs)
        finally:
            await self.store.pg.release(conn)
    return wrap


def with_pg_transaction(method):
    @functools.wraps(method)
    async def wrap(self, *args, **kwargs):
        conn = kwargs.get("conn")
        created_conn = False
        if not conn:
            created_conn = True
            conn = await self.store.pg.acquire()
            kwargs["conn"] = conn

        transaction = conn.transaction()
        try:
            await transaction.start()
            res = await method(self, *args, **kwargs)
            await transaction.commit()
            return res
        except:
            await transaction.rollback()
            raise
        finally:
            if created_conn:
                await self.store.pg.release(conn)
    return wrap
