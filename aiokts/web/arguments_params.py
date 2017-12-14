import functools
import json

from aiokts.util.arguments import check_arguments
from aiokts.web.error import ServerError


def arguments_params(arglist=None):
    if arglist is None:
        arglist = {}

    def _arguments(func):
        @functools.wraps(func)
        async def inner(self, *args, **kwargs):
            if self.request.method == 'GET':
                source = self.request.url.query
            else:
                if self.request.content_type.startswith('application/json'):
                    try:
                        source = await self.request.json()
                    except json.JSONDecodeError:
                        raise ServerError(ServerError.BAD_REQUEST(
                            message='Body must be a valid json'))
                else:
                    source = await self.request.post()
            checked_args = check_arguments(arglist, source, cast_type=True)
            kwargs.update(checked_args)
            return await func(self, *args, **kwargs)

        inner._has_arguments_ = True
        inner.arglist = arglist
        return inner

    return _arguments


def arguments_params_get(arglist=None):
    if arglist is None:
        arglist = {}

    def _arguments(func):
        @functools.wraps(func)
        def inner(self, *args, **kwargs):
            checked_args = check_arguments(
                arglist, self.request.url.query, cast_type=True)
            kwargs.update(checked_args)
            return func(self, *args, **kwargs)

        inner._has_arguments_ = True
        inner.arglist = arglist
        return inner

    return _arguments


def arguments_params_post(arglist=None):
    if arglist is None:
        arglist = {}

    def _arguments(func):
        @functools.wraps(func)
        async def inner(self, *args, **kwargs):
            data = await self.request.post()
            checked_args = check_arguments(arglist, data, cast_type=True)
            kwargs.update(checked_args)
            return await func(self, *args, **kwargs)

        inner._has_arguments_ = True
        inner.arglist = arglist
        return inner

    return _arguments


def arguments_params_json(arglist=None):
    if arglist is None:
        arglist = {}

    def _arguments(func):
        @functools.wraps(func)
        async def inner(self, *args, **kwargs):
            try:
                data = await self.request.json()
            except json.JSONDecodeError:
                raise ServerError(ServerError.BAD_REQUEST(
                    message='Body must be a valid json'))
            checked_args = check_arguments(arglist, data, cast_type=True)
            kwargs.update(checked_args)
            return await func(self, *args, **kwargs)

        inner._has_arguments_ = True
        inner.arglist = arglist
        return inner

    return _arguments
