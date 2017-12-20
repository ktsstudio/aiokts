import weakref

from aiohttp import hdrs
from aiohttp.web import Response
from multidict import CIMultiDict, CIMultiDictProxy

from aiokts.util.json_utils import json_dumps

__all__ = [
    'JsonResponse',
    'ApiResponse',
    'ApiOkResponse',
    'ApiErrorResponse'
]


class KtsResponse(Response):
    def __init__(self, *, body=None, status=200, reason=None, text=None,
                 headers=None, content_type=None, charset=None, ctx=None):
        super().__init__(body=body, status=status, reason=reason, text=text,
                         headers=headers, content_type=content_type,
                         charset=charset)
        self._ctx = weakref.ref(ctx) if ctx is not None else None

    @property
    def ctx(self):
        return self._ctx() if self._ctx is not None else None


class JsonResponse(KtsResponse):
    def __init__(self, body=None, *,
                 status=200,
                 reason=None,
                 text=None,
                 headers=None,
                 charset=None,
                 json_dump_func=None,
                 ctx=None):
        if json_dump_func is None:
            json_dump_func = json_dumps

        body, status = self._process_body(body, status)

        if charset is None:
            charset = 'utf-8'

        body = json_dump_func(body).encode(charset)

        if headers is None:
            headers = CIMultiDict()
        elif not isinstance(headers, (CIMultiDict, CIMultiDictProxy)):
            headers = CIMultiDict(headers)

        if hdrs.CONTENT_TYPE in headers:
            headers.pop(hdrs.CONTENT_TYPE)

        super().__init__(
            body=body,
            status=status,
            reason=reason,
            text=text,
            headers=headers,
            content_type='application/json',
            charset=charset,
            ctx=ctx
        )

    def _process_body(self, body, status=200):
        return body, status


class RawApiResponse(JsonResponse):
    pass


class ApiResponse(RawApiResponse):
    def __init__(self, status='ok', data=None, http_status=200, *,
                 headers=None, charset=None, json_dump_func=None, ctx=None,
                 **kwargs):
        """
        :param status: "ok" or "error"
        :param data: data payload
        :param http_status: http code
        :param kwargs: extra kwargs will be inlined in the root response, so
                      if extra = { 'code': 'hello' } then response will be
                      {
                        'status': status,
                        'code': 'hello',
                        'data': data
                      }
        """
        if data is None:
            data = {}

        self._api_status = status
        self._api_data = data
        self._api_http_status = http_status
        self._api_extra = kwargs

        body = self.__class__.generate_response_dict(
            api_status=self._api_status,
            data=self._api_data,
            **self._api_extra
        )

        super(ApiResponse, self).__init__(
            body=body,
            status=self._api_http_status,
            headers=headers,
            charset=charset,
            json_dump_func=json_dump_func,
            ctx=ctx
        )

    @staticmethod
    def generate_response_dict(api_status='ok', data=None, **extra):
        if data is None:
            data = {}
        res = {
            'status': api_status,
            'data': data
        }
        res.update(extra)
        return res


class ApiOkResponse(ApiResponse):
    def __init__(self, data=None, http_status=200, *,
                 headers=None, charset=None, json_dump_func=None, ctx=None,
                 **kwargs):
        super(ApiOkResponse, self).__init__('ok', data, http_status,
                                            headers=headers,
                                            charset=charset,
                                            json_dump_func=json_dump_func,
                                            ctx=ctx,
                                            **kwargs)

    @staticmethod
    def generate_response_dict(api_status='ok', data=None, **extra):
        return ApiResponse.generate_response_dict(api_status=api_status,
                                                  data=data, **extra)


class ApiErrorResponse(ApiResponse):
    def __init__(self, message=None, data=None, http_status=500, *,
                 headers=None, charset=None, json_dump_func=None, ctx=None,
                 **kwargs):
        if message:
            kwargs['message'] = message
        super(ApiErrorResponse, self).__init__('error', data, http_status,
                                               headers=headers,
                                               charset=charset,
                                               json_dump_func=json_dump_func,
                                               ctx=ctx,
                                               **kwargs)

    @staticmethod
    def generate_response_dict(api_status='error', message=None, data=None,
                               **extra):
        if message:
            extra['message'] = message
        return ApiResponse.generate_response_dict(api_status=api_status,
                                                  data=data, **extra)
