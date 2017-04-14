import warnings
import weakref

from aiohttp import web
from aiohttp.log import web_logger

from aiokts.context import Context
from aiokts.request import KtsRequest
from aiokts.server import KtsServer

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

        for route in self.ROUTES:
            method, path, view_cls = route
            self.router.add_route(method, path, view_cls)

    def _make_request(self, message, payload, protocol, writer, task,
                      _cls=KtsRequest):
        req = super()._make_request(message, payload, protocol, writer, task,
                                    _cls=_cls)
        ctx = self.make_context(req)
        ctx.log_request()
        req.set_context(ctx)
        return req

    def make_context(self, request):
        return Context(request)

    def make_handler(self, *, loop=None,
                     secure_proxy_ssl_header=None, **kwargs):
        self._set_loop(loop)
        self.freeze()

        kwargs['debug'] = self.debug
        if self._handler_args:
            for k, v in self._handler_args.items():
                kwargs[k] = v

        self._secure_proxy_ssl_header = secure_proxy_ssl_header
        return self.SERVER_CLS(self._handle,
                               request_factory=self._make_request,
                               loop=self.loop, **kwargs)
