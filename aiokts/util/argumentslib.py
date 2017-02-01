from aiokts.util.arguments import Argument, ArgumentException, check_arguments


class ListArg(Argument):
    def __init__(self, required=True, default=[], allow_empty=False, max_count=None):
        validator = None
        validator_message = ''
        if not allow_empty and max_count is None:
            def validator(x):
                return len(x) > 0
            validator_message = 'must be non-empty list.'
        elif max_count is not None:
            def validator(x):
                return 0 < len(x) < max_count
            validator_message = 'must be non-empty list with max_count = {}.'.format(max_count)
        elif allow_empty and max_count is not None:
            def validator(x):
                return len(x) < max_count
            validator_message = 'must be list with max_count = {}.'.format(max_count)
        else:
            pass
        
        super().__init__(
            required=required,
            type=list,
            default=default,
            validator=validator,
            validator_message=validator_message,
            filter=None
        )


class StringListArg(Argument):
    def __init__(self, allowed_fields=None, required=True, default=[], allow_empty=False):
        super().__init__(
            required=required,
            type=list,
            default=default,
            validator=(lambda x: len(x) > 0) if not allow_empty else None,
            validator_message='must be non-empty list. Valid fields: "%s"' % (
                '", "'.join(allowed_fields)) if allowed_fields is not None else '',
            filter=(lambda x: list(filter(lambda y: y in allowed_fields, x))) if allowed_fields is not None else None
        )


class IntArg(Argument):
    def __init__(self, required=True, default=0):
        super().__init__(
            required=required,
            type=int,
            default=default
        )


class PositiveIntArg(Argument):
    def __init__(self, required=True, default=0, max_value=None):
        super().__init__(
            required=required,
            type=int,
            default=default,
            validator=(lambda x: x > 0) if max_value is None else (lambda x: 0 < x <= max_value),
            validator_message='must be {}'.format('> 0' if max_value is None else '> 0 and <= {}'.format(max_value))
        )


class NonNegativeIntArg(Argument):
    def __init__(self, required=True, default=0, max_value=None):
        super().__init__(
            required=required,
            type=int,
            default=default,
            validator=(lambda x: x >= 0) if max_value is None else (lambda x: 0 <= x <= max_value),
            validator_message='must be {}'.format('> 0' if max_value is None else '> 0 and <= {}'.format(max_value))
        )


class BoolArg(Argument):
    def __init__(self, required=True, default=False):
        super().__init__(
            required=required,
            type=bool,
            default=default,
            validator=None,
            validator_message=None
        )


class FlagsArg(Argument):
    def __init__(self, flags, required=True, default=None):
        if not default:
            default = {}
        super().__init__(
            required=required,
            type=dict,
            default=default,
            filter=flags.dict_filter
        )


class StringArg(Argument):
    def __init__(self, required=True, strip=True, allow_empty=False, empty_is_none=False, default=None):
        if strip:
            if empty_is_none:
                def filter(x):
                    x = x.strip()
                    return None if x == '' else x
            else:
                def filter(x):
                    return x.strip()
        else:
            if empty_is_none:
                def filter(x):
                    return None if x == '' else x
            else:
                filter = None
        super().__init__(
            required=required,
            type=str,
            filter=filter,
            validator=(lambda x: len(x) > 0) if not allow_empty else None,
            validator_message='must be non-empty string' if not allow_empty else None
        )


class ListOfArg(Argument):
    def __init__(self, required=True, argument_inst=None):
        validator = None
        filter = None
        if argument_inst is not None:
            if not isinstance(argument_inst, Argument):
                raise ArgumentException("ListOfArg's argument_type must be an instance of Argument class")
            
            if callable(argument_inst.validator):
                def validator(x):
                    for i, el in enumerate(x):
                        validation_res = argument_inst.validator(el)
                        if not validation_res:
                            self.validator_message = \
                                "Validation for element #{} failed: {}".format(i, argument_inst.validator_message)
                            return False
                        return True
            
            if callable(argument_inst.filter):
                def filter(x):
                    filtered = []
                    for el in x:
                        filtered.append(argument_inst.filter(el))
                    return filtered
        
        super().__init__(
            required=required,
            type=list,
            filter=filter,
            validator=validator,
            validator_message=None
        )


class DictArg(Argument):
    def __init__(self, required=True, required_fields=None, filter=None):
        if required_fields is not None:
            def validator(x):
                missing_fields = []
                for f in required_fields:
                    if x.get(f) is None:
                        missing_fields.append(f)
                if len(missing_fields) > 0:
                    self.validator_message = "argument's required fields {} are missing".format(missing_fields)
                    return False
                return True
        else:
            validator = None
        super().__init__(
            required=required,
            type=dict,
            filter=filter,
            validator=validator,
            validator_message=None
        )


class DictWithSchemaArg(Argument):
    """
        schema - Схема в виде
        {
            "field1": Argument(...),
            ...
        }
    """
    
    def __init__(self, schema, required=True, max_size=None):
        if schema is None:
            raise ArgumentException("Schema cannot be None in {}".format(str(self.__class__)))
        
        validator = None
        if max_size is not None:
            def validator(d):
                self.validator_message = 'Dict size must be less than {}'.format(max_size)
                return len(d) <= max_size
        
        def filter(d):
            return check_arguments(schema, d)
        
        super().__init__(
            required=required,
            type=dict,
            filter=filter,
            validator=validator,
            validator_message=None
        )


class ListWithSchemaArg(Argument):
    """
        schema - Схема в виде
        {
            "field1": Argument(...),
            ...
        }
    """
    
    def __init__(self, schema, required=True, max_size=None, empty_is_none=False, default=None):
        if schema is None:
            raise ArgumentException("Schema cannot be None in {}".format(str(self.__class__)))
        
        validator = None
        if max_size is not None:
            def validator(l):
                self.validator_message = 'List size must be less than {}'.format(max_size)
                return len(l) <= max_size
        
        def filter(l):
            if empty_is_none and len(l) == 0:
                return None
            filtered = []
            for el in l:
                el = check_arguments(schema, el)
                filtered.append(el)
            return filtered
        
        super().__init__(
            required=required,
            type=list,
            default=default,
            filter=filter,
            validator=validator,
            validator_message=None
        )


class ListOfDictsArg(Argument):
    def __init__(self, required=True, required_fields=None, empty_is_none=False):
        if required_fields is not None:
            def validator(x):
                for i, x_item in enumerate(x):
                    if not isinstance(x_item, dict):
                        self.validator_message = "all elements must be dicts (failed on element #{})".format(i)
                        return False
                    else:
                        missing_fields = []
                        for f in required_fields:
                            if x_item.get(f) is None:
                                missing_fields.append(f)
                        if len(missing_fields) > 0:
                            self.validator_message = \
                                "argument's required fields {} are missing in element #{}".format(missing_fields, i)
                            return False
                return True
        else:
            def validator(x):
                for i, x_item in enumerate(x):
                    if not isinstance(x_item, dict):
                        self.validator_message = "all elements must be dicts (failed on element #{})".format(i)
                        return False
                return True
        
        if empty_is_none:
            def filter(l):
                return None if len(l) == 0 else l
        else:
            filter = None
        
        super().__init__(
            required=required,
            type=list,
            filter=filter,
            validator=validator,
            validator_message=None
        )
