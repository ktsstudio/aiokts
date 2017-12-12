import warnings

from aiohttp.helpers import AccessLogger
from aiohttp.web_server import RequestHandler
from aiokts.web.error import ServerError

from aiokts.util.json_utils import json_dumps
from aiokts.web.response import ApiErrorResponse, KtsResponse


class KtsAccessLogger(AccessLogger):
    def log_request(self, ctx, request, response, time):
        """Log access.

        :param ctx: Requset context
        :param request: Request object.
        :param response: Response object.
        :param float time: Time taken to serve the request.
        """

        try:
            fmt_info = self._format_line(request, response, time)

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

    def log_access(self, request, response, time):
        if self.access_logger:
            if not hasattr(response, 'ctx'):
                self.access_logger.log(request, response, time)
                return

            ctx = response.ctx
            if ctx is None:
                warnings.warn('Response\'s ctx attribute is None')
                self.access_logger.log(request, response, time)
            else:
                self.access_logger.log_request(ctx, request, response, time)

    def handle_error(self, request, status=500, exc=None, message=None):
        """Handle errors.

            Returns HTTP response with specific status code. Logs additional
            information. It always closes current connection."""
        self.log_exception("Error handling request", exc_info=exc)

        if status == 500:
            error = ServerError.INTERNAL_ERROR
            data = ApiErrorResponse.generate_response_dict(
                message=error.message,
                data=error.payload
            )
            msg = json_dumps(data)
            content_type = 'application/json'
        else:
            msg = message
            content_type = 'text/plain'

        resp = KtsResponse(status=status, text=msg, content_type=content_type,
                           charset='utf-8', ctx=request.ctx)
        resp.force_close()

        # some data already got sent, connection is broken
        if request.writer.output_size > 0 or self.transport is None:
            self.force_close()

        return resp
