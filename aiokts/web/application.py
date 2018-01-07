from aiohttp import web

from aiokts.web.context import Context
from aiokts.web.request import KtsRequest
from aiokts.web.server import KtsServer

sentinel = object()


class KtsHttpApplication(web.Application):
    IMMEDIATE_REQUEST_LOG = True
    ROUTES = []

    def __init__(self, **kwargs):
        kwargs['debug'] = kwargs.get('debug', False)
        middlewares = kwargs.get('middlewares')
        if 'middlewares' not in kwargs:
            middlewares = ()
        else:
            middlewares = tuple(middlewares)
            kwargs.pop('middlewares')

        middlewares = (self.make_middlewares() or ()) + middlewares
        super().__init__(**kwargs, middlewares=middlewares)

        for route in self.ROUTES:
            method, path, view_cls = route
            self.router.add_route(method, path, view_cls)

    def _make_request(self, message, payload, protocol, writer, task,
                      _cls=KtsRequest):
        req = super()._make_request(message, payload, protocol, writer, task,
                                    _cls=_cls)
        ctx = self.make_context(req)
        if self.IMMEDIATE_REQUEST_LOG:
            ctx.log_request(True)
        req.set_context(ctx)
        return req

    def make_context(self, request):
        return Context(request)

    def make_server(self, *, server_cls=KtsServer, **kwargs):
        return server_cls(**kwargs)

    def make_middlewares(self):
        return ()

    def make_handler(self, *, loop=None,
                     secure_proxy_ssl_header=None, **kwargs):
        self._set_loop(loop)
        self.freeze()

        kwargs['debug'] = self.debug
        if self._handler_args:
            for k, v in self._handler_args.items():
                kwargs[k] = v

        self._secure_proxy_ssl_header = secure_proxy_ssl_header
        return self.make_server(handler=self._handle,
                                request_factory=self._make_request,
                                loop=self.loop, **kwargs)
