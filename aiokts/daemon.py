import abc
import asyncio
import logging
import logging.config

from aiokts.store import Store


class Daemon(object):
    def __init__(self, *, loop=None, **kwargs):
        self.name = self.__class__.__name__
        self.logger = logging.getLogger('Daemon[{}]'.format(self.name))
        self.loop = loop
        self._stopped = False
        self._run_task = None

    @property
    def is_stopped(self):
        return self._stopped

    @property
    def is_running(self):
        return not self._stopped

    @property
    def config(self):
        return {}

    def check(self):
        pass

    async def handle_exception(self, e):
        pass

    async def start(self):
        self.check()
        self.logger.info('Starting daemon %s', self.name)
        try:
            self._run_task = asyncio.ensure_future(self.run(), loop=self.loop)
            await self._run_task
        except asyncio.futures.CancelledError:
            pass
        except Exception as e:
            logging.exception(e)
            await self.handle_exception(e)

    async def stop(self):
        self.logger.info('Stopping %s daemon', self.name)
        if asyncio.iscoroutine(self.cancel) \
                or asyncio.iscoroutinefunction(self.cancel):
            await self.cancel()
        else:
            self.cancel()
        if self._run_task is not None:
            self._run_task.cancel()
            self._run_task = None
        self._stopped = True
        logging.info('Stopped %s daemon', self.name)

    @abc.abstractmethod
    async def run(self):
        raise NotImplementedError()

    def cancel(self):
        pass


class BaseStoreDaemon(Daemon):
    STORE_NEED = []

    def __init__(self, store_config=None, **kwargs):
        super().__init__(**kwargs)

        self.store_config = store_config or self.config.get("store", {})
        self.store = self.make_store()
        self._store_connect_coro = None

    def make_store(self):
        return Store(self.store_config,
                     need=self.STORE_NEED,
                     loop=self.loop)

    def _on_store_connected(self, f):
        self.logger.info('Store connected')
        self._store_connect_coro = None

    def start(self):
        if self.store is not None:
            self._store_connect_coro = asyncio.ensure_future(
                self.store.connect(),
                loop=self.loop
            )
            self._store_connect_coro.add_done_callback(
                self._on_store_connected)
        return super().start()

    async def stop(self):
        await super().stop()
        if self.store is not None:
            if self._store_connect_coro:
                self._store_connect_coro.cancel()
                self._store_connect_coro = None
            await self.store.disconnect()
            self.logger.info('Store disconnected')


def main(daemon):
    loop = asyncio.get_event_loop()

    try:
        loop.run_until_complete(daemon.start())
        loop.run_until_complete(daemon.stop())
        return 0
    except KeyboardInterrupt:
        loop.run_until_complete(daemon.stop())
        return 0
    except:
        return 1
