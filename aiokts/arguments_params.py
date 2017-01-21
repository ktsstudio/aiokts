import functools
import json

from aiokts.error import ServerError
from aiokts.util.arguments import check_arguments


def arguments_params(arglist=None):
    if arglist is None:
        arglist = {}

    def _arguments(func):
        @functools.wraps(func)
        async def inner(self):
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
            args = check_arguments(arglist, source, cast_type=True)
            return await func(self, **args)

        inner._has_arguments_ = True
        inner.arglist = arglist
        return inner

    return _arguments


def arguments_params_get(arglist=None):
    if arglist is None:
        arglist = {}

    def _arguments(func):
        @functools.wraps(func)
        def inner(self):
            args = check_arguments(arglist, self.request.url.query, cast_type=True)
            return func(self, **args)

        inner._has_arguments_ = True
        inner.arglist = arglist
        return inner

    return _arguments


def arguments_params_post(arglist=None):
    if arglist is None:
        arglist = {}

    def _arguments(func):
        @functools.wraps(func)
        async def inner(self):
            data = await self.request.post()
            args = check_arguments(arglist, data, cast_type=True)
            return await func(self, **args)

        inner._has_arguments_ = True
        inner.arglist = arglist
        return inner

    return _arguments


def arguments_params_json(arglist=None):
    if arglist is None:
        arglist = {}
        
    def _arguments(func):
        @functools.wraps(func)
        async def inner(self):
            try:
                data = await self.request.json()
            except json.JSONDecodeError:
                raise ServerError(ServerError.BAD_REQUEST(message='Body must be a valid json'))
            args = check_arguments(arglist, data, cast_type=True)
            return await func(self, **args)

        inner._has_arguments_ = True
        inner.arglist = arglist
        return inner

    return _arguments
