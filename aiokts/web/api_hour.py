import asyncio
import logging
import weakref

import api_hour

from aiokts.store import Store
from aiokts.web.application import KtsHttpApplication


class KtsContainerHttpApplication(KtsHttpApplication):
    def __init__(self, config, container=None, **kwargs):
        super().__init__(**kwargs)

        self.config = config
        self._container = weakref.ref(container) if container else None

    @property
    def container(self):
        return self._container() if self._container else None


class KtsWebContainer(api_hour.Container):
    ACCESS_LOG_FORMAT = '%a -> %r %s %b [%Tfs]'

    STORE_NEED = None

    def __init__(self, application=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(__name__)
        self._store = self.make_store()
        self._store_connect_coro = None

        self._application = None
        self.application = application

    @property
    def application(self):
        return self._application

    @application.setter
    def application(self, app):
        if app is not None:
            self._application = app
            self.servers['http'] = app

    async def make_servers(self, sockets):
        handlers = {}
        if 'http' in self.servers:
            handler = self.servers['http'].make_handler(
                logger=self.worker.log,
                keep_alive=self.worker.cfg.keepalive,
                access_log=self.worker.log.access_log,
                access_log_format=self.ACCESS_LOG_FORMAT
            )
            for sock in sockets:
                srv = await self.loop.create_server(handler, sock=sock.sock)
                handlers[srv] = handler
        return handlers

    async def start(self):
        # no await - no need to wait when everything is connected
        asyncio.ensure_future(self.store_connect(), loop=self.loop)
        await super().start()

    async def stop(self):
        await self.store_disconnect()
        await super().stop()

    @property
    def store(self):
        return self._store

    def make_store(self):
        return Store(self.config['store'],
                     need=self.STORE_NEED, loop=self.loop)

    async def store_connect(self):
        if self._store is None:
            return

        self.logger.info('Starting store...')

        def on_finish(f):
            self.logger.info('Store is fully connected')
            self._store_connect_coro = None

        self._store_connect_coro = asyncio.ensure_future(
            self.store.connect(),
            loop=self.loop
        )
        self._store_connect_coro.add_done_callback(on_finish)
        return await self._store_connect_coro

    async def store_disconnect(self):
        if self._store is None:
            return
        self.logger.info('Stopping store...')

        if self._store_connect_coro is not None:
            self._store_connect_coro.cancel()
            self._store_connect_coro = None

        def on_finish(f):
            self.logger.info('Store is fully stopped')

        coro = asyncio.ensure_future(
            self.store.disconnect(),
            loop=self.loop
        )
        coro.add_done_callback(on_finish)
        return await coro
