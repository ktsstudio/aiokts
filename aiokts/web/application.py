from aiohttp import web

from aiokts.web.context import Context
from aiokts.web.handler import KtsAccessLogger
from aiokts.web.request import KtsRequest
from aiokts.web.server import KtsServer

sentinel = object()


class KtsHttpApplication(web.Application):
    ROUTES = []

    def __init__(self, **kwargs):
        kwargs['debug'] = kwargs.get('debug', False)
        super().__init__(**kwargs)

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

    def make_server(self, *, server_cls=KtsServer, **kwargs):
        return server_cls(**kwargs)

    def _make_handler(self, *, loop=None,
                      access_log_class=KtsAccessLogger,
                      **kwargs):
        self._set_loop(loop)
        self.freeze()

        kwargs['debug'] = self.debug
        if self._handler_args:
            for k, v in self._handler_args.items():
                kwargs[k] = v

        return self.make_server(handler=self._handle,
                                request_factory=self._make_request,
                                access_log_class=access_log_class,
                                loop=self.loop, **kwargs)
