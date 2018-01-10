import asyncio

import logging


class StoreException(Exception):
    pass


class Store(object):
    def __init__(self, config, need=None, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        self.loop = loop
        self.logger = logging.getLogger('store')
        self.config = self.check_config(config)
        self._need = None
        self.need(need)

        self._accessors = None
        self.init_store()

        self.__connect_coros = []

    def need(self, need=None):
        if need is not None:
            self._need = set(need) if need is not None else set()
        return self._need

    def get_extra_location(self):
        return []

    def init_store(self):
        from aiokts.store import base_accessors

        self._accessors = {}

        # Importing BaseAccessor class
        base_accessor_class = vars(base_accessors).get('BaseAccessor')

        # Importing all modules required in self._need and collecting map
        # (module_name -> accessor_type)
        if self._need is None:
            return

        for accessor_type in self._need:
            accessor_type_found = None
            locations = [base_accessors] + self.get_extra_location()
            for location in locations:
                if accessor_type_found is True:
                    break
                module_name = '{}.{}'.format(location.__name__, accessor_type)
                try:
                    module = __import__(module_name, fromlist='dummy')
                    accessor_type_found = True
                except ImportError as e:
                    if module_name in str(e):
                        accessor_type_found = False
                        continue
                    else:
                        raise

                registered_accessors = set()
                for accessor_class in \
                        self._classes_in_module(module, base_accessor_class):
                    if accessor_type not in self._accessors \
                            and accessor_class not in registered_accessors:
                        try:
                            conf = self.config.get(accessor_type)
                            self._accessors[accessor_type] = \
                                accessor_class(config=conf,
                                               type=accessor_type,
                                               store=self,
                                               loop=self.loop)
                            registered_accessors.add(accessor_class)
                        except Exception as e:
                            self.logger.error('Error with %s accessor: %s',
                                              accessor_type, str(e))
                            raise e

            if not accessor_type_found:
                raise StoreException(
                    'Accessor with type {} not found'.format(accessor_type))

    def _classes_in_module(self, module, base):
        md = module.__dict__
        classes = []
        for _, c in module.__dict__.items():
            if isinstance(c, type) \
                    and c.__module__ == module.__name__ \
                    and issubclass(c, base) \
                    and c is not base:
                classes.append(c)
        return classes

    def __getattribute__(self, name):
        try:
            return super().__getattribute__(name)
        except AttributeError:
            if name in self._accessors:
                return self._accessors[name]
            else:
                raise StoreException(
                    'Accessor \'{}\' not found in Store. '
                    'Probably not specified in need'.format(name))

    async def wait_connected(self):
        coros = []
        for accessor_type, c in self._accessors.items():
            coro = asyncio.ensure_future(c.wait_connected(), loop=self.loop)
            coros.append(coro)

        if len(coros) > 0:
            await asyncio.wait(coros, loop=self.loop)

    def check_config(self, config):
        return config

    async def connect(self):
        try:
            all_success = True

            def on_all_connected(f):
                nonlocal all_success

                if all_success:
                    self.logger.info('Connected to all')
                else:
                    self.logger.info('Connected to all with errors')

                self.__connect_coros = None

            def on_one_connected(f):
                nonlocal all_success

                cancelled = f.cancelled()
                exc = f.exception()
                if not cancelled and not exc:
                    return

                f_accessor_type = getattr(f, '_accessor_type', 'UNK_TYPE')

                if cancelled:
                    self.logger.error(
                        'Connection to [%s] is cancelled', f_accessor_type)
                    all_success = False
                elif exc:
                    self.logger.error(
                        'Exception while connecting to [%s] : %s',
                        f_accessor_type, str(exc), exc_info=exc)
                    all_success = False

            coros = []

            if self._accessors:
                for accessor_type, c in self._accessors.items():
                    coro = asyncio.ensure_future(c.connect(), loop=self.loop)
                    coro.add_done_callback(on_one_connected)
                    coro._accessor_type = accessor_type

                    coros.append(coro)

            if not coros:
                self.logger.info('No connectors to connect to')
                return

            self.__connect_coros = coros

            wait_coro = asyncio.ensure_future(
                asyncio.wait(self.__connect_coros,
                             loop=self.loop,
                             return_when=asyncio.ALL_COMPLETED),
                loop=self.loop
            )
            wait_coro.add_done_callback(on_all_connected)
            await wait_coro
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.logger.exception('Error while connecting')

    async def disconnect(self):
        try:
            # all_success = True
            #
            # def on_all_disconnected(f):
            #     nonlocal all_success
            #
            #     if all_success:
            #         self.logger.info('Disconnected from all')
            #     else:
            #         self.logger.info('Disconnected from all with errors')
            #
            # def on_one_disconnected(f):
            #     cancelled = f.cancelled()
            #     exc = f.exception()
            #     if not cancelled and not exc:
            #         return
            #
            #     f_accessor_type = getattr(f, '_accessor_type', 'UNK_TYPE')
            #
            #     if cancelled:
            #         self.logger.error('Disconnection from [%s] is cancelled',
            #                           f_accessor_type)
            #     elif exc:
            #         self.logger.error(
            #             'Exception while disconnecting from [%s]: %s',
            #             f_accessor_type, str(exc), exc_info=exc
            #         )

            # For some funky reason, asyncio.wait crashes Daemon on exit
            # coros = set()
            if self._accessors:
                for accessor_type, c in self._accessors.items():
                    await c.disconnect()
                    # coro = c.disconnect()
                    # coro.add_done_callback(on_one_disconnected)
                    # coro._accessor_type = accessor_type
                    # coros.add(coro)

            # if not coros:
            #     self.logger.info('No connectors to disconnect from')
            #     return

            # wait_coro = asyncio.ensure_future(
            #     asyncio.wait(coros,
            #                  loop=self.loop,
            #                  return_when=asyncio.ALL_COMPLETED),
            #     loop=self.loop
            # )
            # wait_coro.add_done_callback(on_all_disconnected)
        except Exception as e:
            self.logger.error(e)
