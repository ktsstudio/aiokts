import asyncio
import json

from aiohttp import web
from aiohttp.web_exceptions import HTTPNotFound, HTTPException, HTTPMethodNotAllowed

from aiokts.context import Context
from aiokts.error import ServerError
from aiokts.response import ApiErrorResponse
from aiokts.util.arguments import ArgumentException


class BaseView(web.View):
    CONTEXT_CLS = Context

    def __init__(self, request):
        super().__init__(request)
        self.app = self.request.app
        self.ctx = self.request.ctx
        self.request_data = None

    @property
    def logger(self):
        return self.ctx.logger if self.ctx is not None else None

    @property
    def stores(self):
        return self.app.stores

    @property
    def loop(self):
        return self.app.loop

    @asyncio.coroutine
    def __iter__(self):
        try:
            yield from self._parse_request()
            yield from self.pre_handle()
            return (yield from super(BaseView, self).__iter__())
        except Exception as e:
            res = self.handle_exception(e)
            return res

    async def _parse_request(self):
        if self.request.method == 'GET':
            source = self.request.url.query
        else:
            if self.request.content_type.startswith('application/json'):
                try:
                    source = await self.request.json()
                except json.JSONDecodeError:
                    raise ServerError(ServerError.BAD_REQUEST(message='Body must be a valid json'))
            else:
                source = await self.request.post()
        self.request_data = source

    async def pre_handle(self):
        pass

    def handle_exception(self, e):
        if isinstance(e, ServerError):
            error = e.error
            if error.http_code is None:
                status = 500
            else:
                status = error.http_code
            self.logger.error('[{}] Error: {}'.format(repr(error.code), error.message))

            return ApiErrorResponse(message=error.message,
                                    data=error.payload,
                                    http_status=status,
                                    code=error.code)
        elif isinstance(e, ArgumentException):
            self.logger.warning('ArgumentException: {}'.format(e.message))
            return ApiErrorResponse(message='Field {} is invalid'.format(e.field) if e.field is not None else '',
                                    system_message=e.message,
                                    http_status=400,
                                    code='bad_request',
                                    data=dict(field=e.field))
        elif isinstance(e, HTTPException):
            if e.status_code >= 500:
                self.logger.exception('HTTPException: {}'.format(str(e)), exc_info=e)
            e.headers.pop('Content-Type')
            e.headers.pop('Content-Length')
            return ApiErrorResponse(message=e.reason,
                                    http_status=e.status_code,
                                    headers=e.headers)
        elif isinstance(e, Exception):
            self.logger.exception('Exception: {}'.format(str(e)), exc_info=e)
            return ApiErrorResponse(message='Internal Server Error')
        else:
            self.logger.error('{} is not an exception'.format(repr(e)))


class ActionBaseView(BaseView):
    CONTEXT_CLS = Context

    def __init__(self, request):
        super().__init__(request)

    @property
    def store(self):
        return self.app.store

    @property
    def default_methods(self):
        """
            Dict with keys for HTTP Methods (GET, POST, ...)
            and values are dict with mapping action => func
            i.e.
            {
                "GET": self.default_get,
                "POST": self.default_post
            }
        """
        return {}

    @property
    def methods(self):
        """
            Dict with keys for HTTP Methods (GET, POST, ...)
            and values are dict with mapping action => func
            i.e.
            {
                "GET": {
                    "home": self.home
                },
                "POST": {
                    "post": self.post
                }
            }
        """
        return {}

    async def before_action(self):
        pass

    async def after_action(self):
        pass

    @asyncio.coroutine
    def __iter__(self):
        try:
            yield from self._parse_request()
            yield from self.pre_handle()

            action_title = self.request.match_info['method']

            if action_title is None:
                raise HTTPNotFound()

            method = self.request.method.upper()
            methods = self.methods
            if methods is None:
                methods = {}

            actions = methods.get(method, {})

            def_method = self.default_methods.get(method)
            if action_title in actions:
                executing_method = actions.get(action_title)
            elif def_method is not None:
                executing_method = def_method
            else:
                executing_method = None

            if executing_method is not None:
                yield from self.before_action()
                result = yield from executing_method()
                yield from self.after_action()
                return result
            else:
                allowed_methods = []
                for k, k_actions in methods.items():
                    if action_title in k_actions:
                        allowed_methods.append(k)

                if allowed_methods:
                    raise HTTPMethodNotAllowed(method, allowed_methods=allowed_methods)

                raise HTTPNotFound()
        except Exception as e:
            res = self.handle_exception(e)
            return res
