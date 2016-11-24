from aiohttp import web
from aiohttp.log import web_logger
from aiohttp.web import RequestHandlerFactory

from aiokts.storeset import StoreSet


class HttpApplication(web.Application):
    def __init__(self, **kwargs):
        super().__init__(
            logger=kwargs.get('logger', web_logger),
            loop=kwargs.get('loop'),
            router=kwargs.get('router'),
            handler_factory=kwargs.get('handler_factory', RequestHandlerFactory),
            middlewares=kwargs.get('middlewares', ()),
            debug=kwargs.get('debug', False)
        )
        self.stores = StoreSet(kwargs.get('stores', {}))

    def copy(self):
        pass