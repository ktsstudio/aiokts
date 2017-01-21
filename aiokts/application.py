import warnings
import weakref

from aiohttp import web
from aiohttp.log import web_logger

from aiokts.context import Context
from aiokts.request import KtsRequest
from aiokts.server import KtsServer
from aiokts.storeset import StoreSet

sentinel = object()


class KtsHttpApplication(web.Application):
    ROUTES = []
    SERVER_CLS = KtsServer
    
    def __init__(self, **kwargs):
        super().__init__(
            logger=kwargs.get('logger', web_logger),
            loop=kwargs.get('loop'),
            router=kwargs.get('router'),
            middlewares=kwargs.get('middlewares', ()),
            debug=kwargs.get('debug', False)
        )
        self.stores = StoreSet(kwargs.get('stores', {}))
        
        for route in self.ROUTES:
            method, path, view_cls = route
            self.router.add_route(method, path, view_cls)

    def _make_request(self, message, payload, protocol,
                      _cls=KtsRequest):
        req = super()._make_request(message, payload, protocol, _cls=_cls)
        ctx = self.make_context(req)
        ctx.log_request()
        req.set_context(ctx)
        return req
        
    def make_context(self, request):
        return Context(request)
    
    def make_handler(self, *, secure_proxy_ssl_header=None, **kwargs):
        """
            Copy of aiohttp.web_server.Server.make_handler
            with an option to define a cls for server
        """
        debug = kwargs.pop('debug', sentinel)
        if debug is not sentinel:
            warnings.warn(
                "`debug` parameter is deprecated. "
                "Use Application's debug mode instead", DeprecationWarning)
            if debug != self.debug:
                raise ValueError(
                    "The value of `debug` parameter conflicts with the debug "
                    "settings of the `Application` instance. The "
                    "application's debug mode setting should be used instead "
                    "as a single point to setup a debug mode. For more "
                    "information please check "
                    "http://aiohttp.readthedocs.io/en/stable/"
                    "web_reference.html#aiohttp.web.Application"
                )
        self.freeze()
        self._secure_proxy_ssl_header = secure_proxy_ssl_header
        return self.SERVER_CLS(self._handle, request_factory=self._make_request,
                               debug=self.debug, loop=self.loop, **kwargs)
    

class KtsContainerHttpApplication(KtsHttpApplication):
    def __init__(self, config, container=None, **kwargs):
        super().__init__(**kwargs)
        
        self.config = config
        self._container = weakref.ref(container) if container else None
    
    @property
    def container(self):
        return self._container() if self._container else None
