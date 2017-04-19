import copy


class Error:
    def __init__(self, code, message: str, payload: dict = None,
                 http_code=None):
        self.code = code
        self.message = message
        self.payload = payload
        self.http_code = http_code
        self.extra = None

    def __call__(self, message=None, payload=None, extra=None):
        c = copy.deepcopy(self)
        if message is not None:
            c.message = message
        if payload is not None:
            c.payload = payload
        if extra is not None:
            c.extra = extra
        return c

    def __repr__(self):
        return '<{} (code={}, message={}, extra={})>'.format(
            self.__class__.__name__,
            self.code,
            self.message,
            self.extra)

    def __str__(self):
        return self.message


class ServerError(Exception):
    NOT_AUTHORIZED = Error('not_authorized', 'Not Authorized', http_code=401)
    BAD_REQUEST = Error('bad_request', 'Bad Request', http_code=400)
    FORBIDDEN = Error('forbidden', 'Forbidden', http_code=403)
    NOT_FOUND = Error('not_found', 'Not Found', http_code=404)
    INTERNAL_ERROR = Error('internal_error', 'Internal Server Error',
                           http_code=500)

    def __init__(self, error: Error):
        self.error = error

    def __repr__(self):
        return '<{}: {}>'.format(self.__class__.__name__, repr(self.error))

    def __str__(self):
        return str(self.error)
