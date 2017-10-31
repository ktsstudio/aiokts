import functools

_sentinel = object()


class Argument:
    def __init__(self, required=False, default=None, type=None, to_type=None,
                 validator=None, validator_message='', filter=None):
        self.required = required
        self.default = default
        self.type = type
        self.to_type = to_type if to_type else type
        self.validator = validator
        self.validator_message = validator_message
        self.filter = filter


class ArgumentException(Exception):
    def __init__(self, field, message=_sentinel):
        super(ArgumentException, self).__init__(message)
        self.field = field
        if message is _sentinel:
            if self.field is not None:
                self.message = 'Argument `{}` is required'.format(self.field)
            else:
                self.message = 'Unknown problem with arguments'
        else:
            self.message = message


def arguments(arglist):
    """:Валидатор/фильтратор входных параметров

    Проверяет переданные параметры и отфильтровывает все лишние.
    Аргумент считается не указанным либо если его просто нет в входных аргментах, либо если он None

    Пример использования:
    <pre>
    @arguments({
        'arg_name': Argument(required=False, default=7, type=int, to_type=int,
        validator=lambda x: x > 5, validator_message='must be greater than 5'),
        ...
    })
    def func(self, *args, **kwargs):
        ...

    func(arg_name=1, ...)
    </pre>

    - required (bool)
        Требовать наличия параметра (кидать эксепшн) или тихо игнорировать его отсутствие

    - default
        Дефолтное значение в случае отсутствия параметра и required=False

    - type
        Требуемый тип значения, или None если проверять не надо

    - to_type
        Функция, используемая для приведения к требуемому типу (по умолчанию берётся из type)

    - validator
        функция-валидатор, которая применяется к значению. Если возвращает не True, райзится эксепшн.
        Применяется также и к дефолтному значению. None, если не нужен никакой этот ваш валидатор

    - validator_message
        сообщение, которое выводится в эксепшне, если значение аргумента не проходит проверку валидатором

    - filter
        функция-фильтр, которая принимает на вход переданное значение аргумента и возвращает то, которое будет
        передано в функцию. Например, так можно преобразовывать флаги из dict в int

    :raises ArgumentException
    """

    def _arguments(func):
        @functools.wraps(func)
        def inner(*args, **kwargs):
            checked_kwargs = check_arguments(arglist, kwargs)
            return func(*args, **checked_kwargs)

        inner._has_arguments_ = True
        inner.arglist = arglist
        return inner

    return _arguments


def check_arguments(arglist, kwargs, *, cast_type=False):
    if not isinstance(arglist, dict):
        raise ArgumentException(None, '@arguments expects arg definition as a dict with `Argument` class values')

    filtered_kwargs = {}
    for arg_name, arg_definition in arglist.items():
        if not isinstance(arg_definition, Argument):
            raise ArgumentException(None, '@arguments expects arg definition as a dict with `Argument` class values')

        # check argument existence
        is_default_value = False
        if arg_name not in kwargs and arg_definition.required:
            raise ArgumentException(arg_name, '`%s` argument is required' % arg_name)
        else:
            # not required argument is not specified
            arg_value = kwargs.get(arg_name)
            if arg_value is None:
                if arg_definition.default is not None:
                    arg_value = arg_definition.default
                    is_default_value = True
                else:
                    arg_value = None

        # check argument type
        if arg_definition.type is not None and arg_value is not None:  # None means "any type, do not check"
            if not cast_type:
                if not isinstance(arg_value, arg_definition.type):
                    raise ArgumentException(arg_name,
                                            '`%s` must be `%s`, but got `%s`' %
                                            (arg_name, str(arg_definition.type), str(type(arg_value))))
            else:
                try:
                    arg_value = arg_definition.to_type(arg_value)
                except Exception as e:
                    raise ArgumentException(arg_name,
                                            'Casting `%s` to type `%s` (which has type `%s`) failed: `%s`' %
                                            (arg_name, str(arg_definition.type), str(type(arg_value)), str(e))) from e

        # filter value
        if arg_value is not None and callable(arg_definition.filter):
            arg_value = arg_definition.filter(arg_value)

        # validate value
        if arg_value is not None and callable(arg_definition.validator):
            if not arg_definition.validator(arg_value):
                raise ArgumentException(arg_name,
                                        '%svalue `%s` for argument `%s` was rejected by validator: `%s`' %
                                        ('default ' if is_default_value else '', str(arg_value),
                                         arg_name,
                                         str(arg_definition.validator_message)))

        # all checks passed
        filtered_kwargs[arg_name] = arg_value
    return filtered_kwargs


def has_arguments(func):
    return func.__dict__.get('_has_arguments_', False) and hasattr(func, 'arglist')
