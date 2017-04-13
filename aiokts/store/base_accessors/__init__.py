import abc
import logging
import re
import weakref
from asyncio.locks import Event


class ConfigurationError(Exception):
    pass


class BaseAccessorException(Exception):
    pass


class BaseAccessor(object):
    DEFAULT_HOST = '127.0.0.1'
    DEFAULT_PORT = None
    CHECK_CONFIG = True

    DEFAULT_USERNAME = None
    DEFAULT_PASSWORD = None

    def __init__(self, config, type, store, loop=None):
        super(BaseAccessor, self).__init__()
        self.config = config
        self.type = type
        self._store = weakref.ref(store)
        self.loop = loop
        self.logger = logging.getLogger('accessor[{}]'.format(self.type))

        self.connected = False
        self.connecting = False
        self.disconnecting = False
        self._connected_event = Event(loop=self.loop)
        self._host = None
        self._port = None
        self._username = None
        self._password = None

        if self.CHECK_CONFIG:
            self.check_config()

    @property
    def store(self):
        return self._store() if self._store is not None else None

    async def connect(self):
        if not self.connecting:
            self.connecting = True
            if not self.connected:
                self.logger.info('Connecting to db %s', self.fingerprint)
                await self._connect()
                self.connected = True
                self._connected_event.set()
                self.logger.info('Connected to db %s', self.fingerprint)
            self.connecting = False

    async def disconnect(self):
        if not self.disconnecting:
            self.disconnecting = True
            if self.connected:
                self.logger.info('Disconnecting from db %s', self.fingerprint)
                await self._disconnect()
                self.connected = False
                self._connected_event.clear()
                self.logger.info('Disconnected from db %s', self.fingerprint)
            self.disconnecting = False

    def wait_connected(self):
        return self._connected_event.wait()

    @property
    def fingerprint(self):
        return '[{}://{}:{}]'.format(self.type, self.host, self.port)

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def username(self):
        return self._username

    @property
    def password(self):
        return self._password

    @property
    def has_credentials(self):
        return self.username is not None and self.password is not None

    def check_config(self):
        assert self.type is not None

        if self.config is None:
            raise ConfigurationError(
                'config is required for a connector {}'.format(
                    self.__class__.__name__)
            )

        self._username = self.config.get('username', self.DEFAULT_USERNAME)
        self._password = self.config.get('password', self.DEFAULT_PASSWORD)
        self._host = self.config.get('host', self.DEFAULT_HOST)
        self._port = self.config.get('port', self.DEFAULT_PORT)

        if (self.username is not None and self.password is None) or \
                (self.username is None and self.password is not None):
            raise ConfigurationError(
                'Either both username and password should be defined or none')

    @abc.abstractmethod
    async def _connect(self):
        pass

    @abc.abstractmethod
    async def _disconnect(self):
        pass
