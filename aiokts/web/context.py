import logging
import uuid
import weakref

from aiohttp.helpers import reify

__all__ = [
    'ContextDataObject',
    'Context'
]


class ContextLogger(logging.Logger):
    def __init__(self, ctx, name='ctxLogger', parent=logging.root):
        super(ContextLogger, self).__init__(name)
        self._ctx = ctx
        self.parent = parent

    @property
    def ctx(self):
        return self._ctx

    def _log(self, level, msg, args, exc_info=None,
             extra=None, stack_info=False):
        msg = '{}{}{}'.format(self.ctx.log_prepend, msg, self.ctx.log_append)
        return super(ContextLogger, self)._log(level, msg, args, exc_info,
                                               extra, stack_info)


class ContextDataObject(object):
    def __repr__(self):
        return '<ContextDataObject keys_count:{}>'.format(len(self.__dict__))

    def clear(self):
        self.__dict__.clear()

    def __getattr__(self, item):
        return None


class Context(object):
    CONTEXT_DATA_OBJECT_CLS = ContextDataObject

    __slots__ = [
        '_request',
        '_view',
        'hash',
        'logger',
        '_data',
        '_cache',
        '__weakref__',
    ]

    def __init__(self, request):
        self._request = weakref.ref(request) if request is not None else None
        self.hash = self._generate_hash()
        self.logger = ContextLogger(self)
        self._data = self.CONTEXT_DATA_OBJECT_CLS()

        self._cache = {}

    @property
    def request(self):
        return self._request() if self._request is not None else None

    def log_request(self):
        if self._request is not None:
            if not self.request.query_string:
                q = self.request.query_string
            else:
                q = '?' + self.request.query_string
            self.logger.info('Request {method} {path}{query}'.format(
                method=self.request.method,
                path=self.request.path,
                query=q
            ))

    @staticmethod
    def _generate_hash():
        h = uuid.uuid4().hex
        h = h[:10]
        return h

    @property
    def data(self):
        return self._data

    @reify
    def log_prepend(self):
        """
            This property can return a string that will be prepended to
            a log string (to the start)

            In base Context it has @reify decorator from aiohttp to cache
            the result indefinitely, but if you subclass Context you are
            allowed to use whatever @property decorator you want
        """
        return '[ctx:{}] '.format(self.hash)

    @reify
    def log_append(self):
        """
            This property can return a string that will be appended to
            a log string (to the end)

            In base Context it has @reify decorator from aiohttp to cache
            the result indefinitely, but if you subclass Context you are
            allowed to use whatever @property decorator you want
        """
        return ''
