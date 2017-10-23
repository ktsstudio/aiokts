import functools
import json

from aiohttp import MultipartReader

from aiokts.util.arguments import check_arguments, check_argument
from aiokts.util.argumentslib import MultipartStringArg, MultipartFileArg
from aiokts.web.error import ServerError


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
                        raise ServerError(ServerError.BAD_REQUEST(
                            message='Body must be a valid json'))
                else:
                    source = await self.request.post()
            args = await check_arguments(arglist, source, cast_type=True)
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
        async def inner(self):
            args = await check_arguments(
                arglist, self.request.url.query, cast_type=True)
            return await func(self, **args)

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
            args = await check_arguments(arglist, data, cast_type=True)
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
                raise ServerError(ServerError.BAD_REQUEST(
                    message='Body must be a valid json'))
            args = await check_arguments(arglist, data, cast_type=True)
            return await func(self, **args)

        inner._has_arguments_ = True
        inner.arglist = arglist
        return inner

    return _arguments


def arguments_params_multipart(arglist=None):
    if arglist is None:
        arglist = {}

    def _arguments(func):
        @functools.wraps(func)
        async def inner(self):
            reader = MultipartReader.from_response(self.request)
            args = {}
            while True:
                part = await reader.next()
                if part is None:
                    break

                if part.name in arglist.keys():
                    arg_definition = arglist[part.name]
                    arg = await check_argument(arg_name=part.name,
                                               arg_definition=arg_definition,
                                               kwargs={part.name: part},
                                               cast_type=True)

                    # TODO: check sizes or leave it to nginx?

                    args[part.name] = arg

            return await func(self, **args)

        inner._has_arguments_ = True
        inner.arglist = arglist
        return inner

    return _arguments