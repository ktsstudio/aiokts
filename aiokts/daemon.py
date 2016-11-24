import abc
import asyncio
import logging
import logging.config
import traceback

from aiokts.storeset import StoreSet


class Daemon(object):
    def __init__(self, loop=None, **kwargs):
        self.name = self.__class__.__name__
        self.loop = loop
        self._stopped = False
        self._run_task = None
        self.stores = StoreSet(kwargs.get('stores', {}))

    @property
    def is_stopped(self):
        return self._stopped

    @property
    def is_running(self):
        return not self._stopped

    @asyncio.coroutine
    def start(self):
        def on_run_finished(future):
            if not future.cancelled():
                exc = future.exception()
                if exc:
                    future.print_stack()
                    logging.error('Daemon {} exception'.format(self.__class__.__name__))
                self._stopped = True
            else:
                logging.info('Daemon {} cancelled'.format(self.__class__.__name__))
                asyncio.ensure_future(self.stop(), loop=self.loop)

        try:
            self._run_task = asyncio.ensure_future(self.run(), loop=self.loop)
            yield from self._run_task
        except asyncio.futures.CancelledError:
            pass
        except Exception as e:
            logging.exception(e)

    @asyncio.coroutine
    def stop(self):
        logging.info('Stopping {} daemon'.format(self.name))
        if asyncio.iscoroutine(self.cancel) or asyncio.iscoroutinefunction(self.cancel):
            yield from self.cancel()
        else:
            self.cancel()
        if self._run_task is not None:
            self._run_task.cancel()
            self._run_task = None
        self._stopped = True
        logging.info('Stopped {} daemon'.format(self.name))

    @abc.abstractmethod
    @asyncio.coroutine
    def run(self):
        raise NotImplementedError()

    def cancel(self):
        pass


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
