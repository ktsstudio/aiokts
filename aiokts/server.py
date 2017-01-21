from aiohttp.web_server import Server

from aiokts.handler import KtsRequestHandler


class KtsServer(Server):
    def __call__(self):
        return KtsRequestHandler(
            self, loop=self._loop,
            **self._kwargs)
