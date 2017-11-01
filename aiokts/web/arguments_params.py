import functools
import json
import os

from aiohttp import MultipartReader

from aiokts.util.arguments import check_arguments, check_argument, \
    ArgumentException
from aiokts.util.arguments_async import check_arguments_async, \
    check_argument_async
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
            args = await check_arguments_async(arglist, source, cast_type=True)
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
            args = await check_arguments_async(
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
            args = await check_arguments_async(arglist, data, cast_type=True)
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
            args = await check_arguments_async(arglist, data, cast_type=True)
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

            size = 0
            max_size = self.app._client_max_size
            while True:
                part = await reader.next()
                if part is None:
                    break

                if part.name in arglist:
                    arg_definition = arglist[part.name]
                    arg = await check_argument_async(arg_name=part.name,
                                                     arg_definition=arg_definition,
                                                     kwargs={part.name: part},
                                                     cast_type=True)

                    if isinstance(arg_definition, MultipartStringArg):
                        size += len(arg.encode('utf-8'))
                    elif isinstance(arg_definition, MultipartFileArg):
                        f = arg.file
                        old_file_position = f.tell()
                        f.seek(0, os.SEEK_END)
                        size += f.tell()
                        f.seek(old_file_position, os.SEEK_SET)

                    if 0 < max_size < size:
                        raise ValueError('Maximum request body size exceeded')

                    args[part.name] = arg

            for arg_name in filter(lambda a: arglist[a].required is True,
                                   arglist):
                if arg_name not in args:
                    raise ArgumentException(field=arg_name,
                                            message='`{}` argument is required'
                                                    .format(arg_name))

            return await func(self, **args)

        inner._has_arguments_ = True
        inner.arglist = arglist
        return inner

    return _arguments