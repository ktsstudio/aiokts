from aiohttp import web
from aiohttp.log import access_logger

from aiokts.web.handler import KtsAccessLogger


def run_app(app, *, host=None, port=None, path=None, sock=None,
            shutdown_timeout=60.0, ssl_context=None,
            print=print, backlog=128, access_log_class=KtsAccessLogger,
            access_log_format=KtsAccessLogger.KTS_LOG_FORMAT,
            access_log=access_logger, handle_signals=True,
            reuse_address=None, reuse_port=None):
    return web.run_app(app=app, host=host, port=port, path=path,
                       sock=sock, shutdown_timeout=shutdown_timeout,
                       ssl_context=ssl_context, print=print, backlog=backlog,
                       access_log_class=access_log_class,
                       access_log_format=access_log_format,
                       access_log=access_log,
                       handle_signals=handle_signals,
                       reuse_address=reuse_address, reuse_port=reuse_port)