import inspect

from aiokts.util.arguments import ArgumentException, AsyncArgument, \
    ArgumentRequiredException, ArgumentTypeError, ArgumentCastException, \
    ArgumentValidationError, check_argument


async def check_argument_async(arg_name, arg_definition, kwargs, cast_type):
    if not isinstance(arg_definition, AsyncArgument):
        raise ArgumentException(None,
                                'All argument definitions should subclass `{}` '
                                'class'.format(AsyncArgument.__name__))

    # check argument existence
    is_default_value = False
    if arg_name not in kwargs and arg_definition.required:
        raise ArgumentRequiredException(arg_name=arg_name)
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
                raise ArgumentTypeError(arg_name=arg_name,
                                        req_type=str(arg_definition.type),
                                        actual_type=type(arg_value))
        else:
            try:
                arg_value = await arg_definition.to_type(arg_value)
            except Exception as exc:
                raise ArgumentCastException(arg_name=arg_name,
                                            req_type=str(arg_definition.type),
                                            actual_type=type(arg_value),
                                            exc=exc) from exc

    # filter value
    if arg_value is not None \
            and inspect.iscoroutinefunction(arg_definition.filter):
        arg_value = await arg_definition.filter(arg_value)

    # validate value
    if arg_value is not None \
            and inspect.iscoroutinefunction(arg_definition.validator):
        if not await arg_definition.validator(arg_value):
            raise ArgumentValidationError(arg_name=arg_name,
                                          arg_value=arg_value,
                                          message=arg_definition.validator_message,
                                          is_default_value=is_default_value)

    return arg_value


async def check_arguments_async(arglist, kwargs, *, cast_type=False):
    if not isinstance(arglist, dict):
        raise ArgumentException(field=None,
                                message='@arguments expects argument '
                                        'definitions as a dict')

    filtered_kwargs = {}
    for arg_name, arg_definition in arglist.items():
        if isinstance(arg_definition, AsyncArgument):
            arg_value = await check_argument_async(arg_name=arg_name,
                                                   arg_definition=arg_definition,
                                                   kwargs=kwargs,
                                                   cast_type=cast_type)
        else:
            arg_value = check_argument(arg_name=arg_name,
                                       arg_definition=arg_definition,
                                       kwargs=kwargs,
                                       cast_type=cast_type)

        # all checks passed
        filtered_kwargs[arg_name] = arg_value
    return filtered_kwargs
