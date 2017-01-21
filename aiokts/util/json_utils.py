import abc
import json

import datetime
import time

_features = {
    'bson_object_id': False
}

try:
    from bson import ObjectId
    _features['bson_object_id'] = True
except ImportError:
    pass


class CustomJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, JsonSerializable):
            return obj.__to_json__()
        
        if isinstance(obj, datetime.datetime) or isinstance(obj, datetime.date):
            return int(time.mktime(obj.timetuple()))
        
        if _features['bson_object_id']:
            if isinstance(obj, ObjectId):
                return str(obj)
        
        return super(CustomJsonEncoder, self).default(obj)


class JsonSerializable:
    @abc.abstractmethod
    def __to_json__(self):
        raise NotImplementedError()


def json_dumps(obj, compact=True, **kwargs):
    kwargs['cls'] = kwargs.get('cls', CustomJsonEncoder)
    kwargs['ensure_ascii'] = kwargs.get('ensure_ascii', False)
    if compact:
        kwargs['separators'] = (',', ':')
    return json.dumps(obj, **kwargs)


def json_dump(fp, obj, compact=True, **kwargs):
    kwargs['cls'] = kwargs.get('cls', CustomJsonEncoder)
    kwargs['ensure_ascii'] = kwargs.get('ensure_ascii', False)
    if compact:
        kwargs['separators'] = (',', ':')
    return json.dump(fp, obj, **kwargs)


def json_loads(obj, **kwargs):
    kwargs['encoding'] = kwargs.get('encoding', 'utf-8')
    return json.loads(obj, **kwargs)


def json_load(fp, **kwargs):
    kwargs['encoding'] = kwargs.get('encoding', 'utf-8')
    return json.load(fp, **kwargs)


def ensure_json(obj, **kwargs):
    if obj is None:
        return None
    if isinstance(obj, list) or isinstance(obj, dict):
        return obj
    return json_loads(obj, **kwargs)
