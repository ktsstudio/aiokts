import collections

import logging

import datetime

from aiokts.util.json_utils import JsonSerializable


class Field:
    def __init__(self, default, private):
        self.name = None
        self.model = None
        self.default = default
        self.private = private

    def transform_in(self, value):
        return value

    def transform_to_json(self, value):
        return value

    def __call__(self, value):
        return self.transform_in(value)


class StringField(Field):
    def __init__(self, default=None, private=False):
        super().__init__(default, private)

    def transform_in(self, value):
        return str(value)


class IntField(Field):
    def __init__(self, default=None, private=False):
        super().__init__(default, private)

    def transform_in(self, value):
        return int(value)


class BooleanField(Field):
    def __init__(self, default=None, private=False):
        super().__init__(default, private)

    def transform_in(self, value):
        if value == 'true':
            value = True
        elif value == 'false':
            value = False
        return bool(value)


class UnixTimestampField(Field):
    def __init__(self, default=None, private=False):
        super().__init__(default, private)

    def transform_in(self, value):
        value = int(value)
        return datetime.datetime.fromtimestamp(value)


class IntEnumField(Field):
    def __init__(self, enum_cls, default=None, private=False, json_name=False):
        super().__init__(default, private)
        self.enum_cls = enum_cls
        self.json_name = json_name

    def transform_in(self, value):
        return self.enum_cls(value)

    def transform_to_json(self, value):
        if self.json_name:
            return value.name
        return value.value


class DictField(Field):
    def __init__(self, default=None, private=False):
        super().__init__(default, private)

    def transform_in(self, value):
        assert isinstance(value, dict), \
            'value is not dict (but {}) for {}.{}'.format(
                type(value), self.model.__name__, self.name
            )
        return value


class ForeignModelField(Field):
    def __init__(self, model_cls, default=None, private=False):
        super().__init__(default, private)
        self.model_cls = model_cls

    def transform_in(self, value):
        return self.model_cls.parse(value)

    def transform_to_json(self, value):
        return value.__to_json__()


class DoesNotExistBase(Exception):
    MODEL_CLS = None

    def __init__(self, entity_id=None, message=None):
        self.entity_id = entity_id
        if message is None:
            self.message = \
                "Entity of type '{}' with id {} not found".format(
                    self.MODEL_CLS.__name__, self.entity_id)
        else:
            self.message = message

    def __str__(self):
        return self.message


class ModelMetaclass(type):
    @classmethod
    def __prepare__(mcs, name, bases):
        return collections.OrderedDict()

    def __new__(mcs, class_name, bases, class_dict):
        if class_name != 'Model':
            fields = collections.OrderedDict()
            for name, value in class_dict.items():
                if not name.startswith('__') \
                        and isinstance(value, Field):
                    fields[name] = value
            for name in fields:
                del class_dict[name]

            class_dict['_fields'] = fields

        return super().__new__(mcs, class_name, bases, class_dict)

    def __init__(cls, class_name, bases, class_dict):
        class DoesNotExist(DoesNotExistBase):
            MODEL_CLS = cls
        cls.DoesNotExist = DoesNotExist
        fields = class_dict['_fields']
        if fields is not None:
            for name, f in fields.items():
                f.name = name
                f.model = cls
        super().__init__(class_name, bases, class_dict)


class Model(JsonSerializable, metaclass=ModelMetaclass):
    _fields = None
    DoesNotExist = None
    LOGGER = logging.getLogger('aiokts.models')

    def __init__(self, *args, **kwargs):
        i = 0

        used_kwargs = set()
        for name, field in self._fields.items():
            setattr(self, name, field.default)
            if len(args) <= i:
                # supplying kw arguments
                if name in kwargs:
                    v = kwargs[name]
                    if v is not None:
                        try:
                            v = field.transform_in(v)
                        except Exception as e:
                            raise Exception('{} for field `{}` in {}'.format(
                                str(e), name, self.__class__
                            ))
                        transformer = 'transform_{}'.format(name)
                        if hasattr(self, transformer) \
                                and callable(getattr(self, transformer)):
                            v = getattr(self, transformer)(v)
                    setattr(self, name, v)
                    used_kwargs.add(name)
            else:
                # supplying positional arguments
                v = args[i]
                if v is not None:
                    v = field.transform_in(v)
                setattr(self, name, v)
                i += 1

        if len(args) > i:
            self.LOGGER.warning(
                'Too many positional arguments passed. '
                'Expected %s max', len(self._fields)
            )
            return

        extra_kwargs = set(kwargs.keys()) - used_kwargs
        if len(extra_kwargs) > 0:
            self.LOGGER.warning(
                'Unknown fields passed: %s', extra_kwargs)

    def __to_json__(self):
        res = {}
        for name, field in self._fields.items():
            if field.private:
                continue
            v = getattr(self, name)
            if v is not None:
                v = field.transform_to_json(v)
                transformer = 'transform_json_{}'.format(name)
                if hasattr(self, transformer) \
                        and callable(getattr(self, transformer)):
                    v = getattr(self, transformer)(v)
            res[name] = v
        return res

    @classmethod
    def parse(cls, d: dict):
        if d is None:
            return None
        return cls(**d)

    @classmethod
    def parse_list(cls, l: list):
        if l is None or len(l) == 0:
            return []
        return list(map(lambda d: cls(**d), l))
