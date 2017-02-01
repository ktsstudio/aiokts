from aiohttp.web import Response

from aiokts.util.json_utils import json_dumps

__all__ = [
    'JsonResponse',
    'ApiResponse',
    'ApiOkResponse',
    'ApiErrorResponse'
]


class JsonResponse(Response):
    def __init__(self, body=None, *,
                 status=200,
                 reason=None,
                 text=None,
                 headers=None,
                 charset=None,
                 json_dump_func=None):
        if json_dump_func is None:
            json_dump_func = json_dumps
            
        body, status = self._process_body(body, status)
        
        if charset is None:
            charset = 'utf-8'
        
        body = json_dump_func(body).encode(charset)
        
        super().__init__(
            body=body,
            status=status,
            reason=reason,
            text=text,
            headers=headers,
            content_type='application/json',
            charset=charset
        )
    
    def _process_body(self, body, status=200):
        return body, status


class RawApiResponse(JsonResponse):
    pass


class ApiResponse(RawApiResponse):
    def __init__(self, status='ok', data=None, http_status=200, *,
                 headers=None, charset=None, json_dump_func=None, **kwargs):
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
            data=self._api_data,
            **self._api_extra
        )
        
        super(ApiResponse, self).__init__(
            body=body,
            status=self._api_http_status,
            headers=headers,
            charset=charset,
            json_dump_func=json_dump_func
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
                 headers=None, charset=None, json_dump_func=None, **kwargs):
        super(ApiOkResponse, self).__init__('ok', data, http_status,
                                            headers=headers,
                                            charset=charset,
                                            json_dump_func=json_dump_func, **kwargs)

    @staticmethod
    def generate_response_dict(data=None, **extra):
        return ApiResponse.generate_response_dict(api_status='ok', data=data, **extra)


class ApiErrorResponse(ApiResponse):
    def __init__(self, message=None, data=None, http_status=500, *,
                 headers=None, charset=None, json_dump_func=None, **kwargs):
        if message:
            kwargs['message'] = message
        super(ApiErrorResponse, self).__init__('error', data, http_status,
                                               headers=headers,
                                               charset=charset,
                                               json_dump_func=json_dump_func, **kwargs)
    
    @staticmethod
    def generate_response_dict(message=None, data=None, **extra):
        if message:
            extra['message'] = message
        return ApiResponse.generate_response_dict(api_status='error', data=data, **extra)
