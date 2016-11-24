from aiohttp.web import Response
import json
import tarantool


class JsonResponse(Response):
    HTTP_STATUSES = (
        (200, 'ok'),
        (400, 'bad_request'),
        (401, 'unauth'),
        (403, 'forbidden'),
        (404, 'not_found'),
        (500, 'internal_error')
    )

    def __init__(self, body=None, status=200, reason=None, text=None, headers=None, content_type=None, charset=None):
        body, status = self._process_body(body, status)

        body = json.dumps(body).encode('utf-8')
        if not content_type:
            content_type = 'application/json'

        super().__init__(
            body=body,
            status=status,
            reason=reason,
            text=text,
            headers=headers,
            content_type=content_type,
            charset=charset
        )

    def _process_body(self, body, status=200):
        if isinstance(body, tarantool.DatabaseError) and len(body.args) > 1:
            field_status = body.args[0] if 200 <= body.args[0] < 599 else 500
            field_reason = 'internal_server_error'
            message = body.args[1] if 200 <= field_status < 500 else 'internal_server_error'

            for code, msg in self.HTTP_STATUSES:
                if code == field_status:
                    field_reason = msg
                    break

            body = {
                'status': field_status,
                'reason': field_reason,
            }

            if message is not None:
                body['msg'] = message

            if status == 200:
                status = field_status

        if isinstance(body, Exception):
            body = None
            status = 500 if status == 200 else status

        if body is None:
            for code, message in self.HTTP_STATUSES:
                if code == status:
                    body = {'status': code, 'reason': message}
                    break

        return body, status
