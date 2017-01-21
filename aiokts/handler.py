import aiohttp
from aiohttp import hdrs
from aiohttp.helpers import AccessLogger
from aiohttp.web_server import RequestHandler

from aiokts.error import ServerError
from aiokts.response import ApiErrorResponse
from aiokts.util.json_utils import json_dumps


class KtsAccessLogger(AccessLogger):
    def log_request(self, request, message, environ, response, transport, time):
        """Log access.

        :param message: Request object. May be None.
        :param environ: Environment dict. May be None.
        :param response: Response object.
        :param transport: Tansport object. May be None
        :param float time: Time taken to serve the request.
        """
        
        ctx = request.ctx if hasattr(request, 'ctx') else None
        if ctx is None:
            return self.log(message, environ, response, transport, time)
        
        try:
            fmt_info = self._format_line(
                [message, environ, response, transport, time])
            
            values = list()
            extra = dict()
            for key, value in fmt_info:
                values.append(value)
                
                if key.__class__ is str:
                    extra[key] = value
                else:
                    extra[key[0]] = {key[1]: value}
            
            ctx.logger.info(self._log_format % tuple(values), extra=extra)
        except Exception:
            ctx.logger.exception("Error in logging")


class KtsRequestHandler(RequestHandler):
    def __init__(self, *args, **kwargs):
        access_log = kwargs.get('access_log')
        access_log_format = kwargs.get('access_log_format')
        super(KtsRequestHandler, self).__init__(*args, **kwargs)
        
        if access_log:
            self.access_logger = KtsAccessLogger(access_log, access_log_format)
        else:
            self.access_logger = None
        self._middlewares = None
    
    def log_access(self, message, environ, response, time):
        if self.access_logger:
            if self._request is not None:
                self.access_logger.log_request(self._request, message, environ, response, self.transport, time)
            else:
                self.access_logger.log(message, environ, response, self.transport, time)
    
    def handle_error(self, status=500, message=None,
                     payload=None, exc=None, headers=None, reason=None):
        """rewrite of super() function

        Handle errors.

        Returns HTTP response with specific status code. Logs additional
        information. It always closes current connection."""
        now = self._loop.time()
        try:
            if self.transport is None:
                # client has been disconnected during writing.
                return ()
            
            self.log_exception("Error handling request")
            
            error = ServerError.INTERNAL_ERROR
            data = ApiErrorResponse.generate_response_dict(
                message=error.message,
                data=error.payload
            )
            data = json_dumps(data).encode()
            
            response = aiohttp.Response(self.writer, status, close=True)
            response.add_header(hdrs.CONTENT_TYPE, 'application/json; charset=utf-8')
            response.add_header(hdrs.CONTENT_LENGTH, str(len(data)))
            if headers is not None:
                for name, value in headers:
                    response.add_header(name, value)
            response.send_headers()
            
            response.write(data)
            # disable CORK, enable NODELAY if needed
            self.writer.set_tcp_nodelay(True)
            drain = response.write_eof()
            
            self.log_access(message, None, response, self._loop.time() - now)
            return drain
        finally:
            self.keep_alive(False)
