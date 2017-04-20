import abc
import asyncio
import logging
import logging.config
import weakref

from aiokts.store import Store


class Command:
    def __init__(self, manager=None):
        self.logger = logging.getLogger(
            'CmdLogger_{}'.format(self.__class__.__name__))
        self._manager = weakref.ref(manager) if manager is not None else None

    @property
    def manager(self):
        return self._manager() if self._manager is not None else None

    @property
    def config(self):
        return self.manager.config

    @abc.abstractmethod
    def run(self, *args):
        pass


class AsyncCommand(Command):
    def __init__(self, manager=None, loop=None):
        super().__init__(manager)
        if loop is None:
            loop = asyncio.get_event_loop()
        self.loop = loop

    @abc.abstractmethod
    @asyncio.coroutine
    def run_async(self, *args):
        pass

    def run(self, *args):
        self.loop.run_until_complete(self.run_async(*args))
        return 0


class StoreCommand(AsyncCommand):
    WAIT_TIMEOUT = 3.0
    STORE_CONNECT = True
    STORE_NEED = None

    def __init__(self, manager=None, store_config=None, loop=None):
        super().__init__(manager, loop)
        self.store_config = store_config
        if self.store_config is None:
            self.store_config = self.config.get("store", {})
        self.store = self.make_store()

    def make_store(self):
        if self.manager.store_cls is None:
            return None
        return self.manager.store_cls(self.store_config,
                                      need=self.STORE_NEED,
                                      loop=self.loop)

    async def _store_connect(self):
        await asyncio.wait_for(self.store.connect(),
                               self.WAIT_TIMEOUT,
                               loop=self.loop)

    async def _store_disconnect(self):
        await self.store.disconnect()

    def run(self, *args):
        if self.store is not None and self.STORE_CONNECT:
            self.loop.run_until_complete(self._store_connect())
        try:
            result = super().run(*args)
        finally:
            if self.store is not None and self.STORE_CONNECT:
                self.loop.run_until_complete(self._store_disconnect())
        return result
