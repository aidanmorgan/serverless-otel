from typing import Optional

TRUTHY_VALUES = ['true', '1', 'yes', 'on']
FALSY_VALUES = ['false', '0', 'no', 'off']


def str_as_bool(val: str, default: Optional[bool] = None) -> Optional[bool]:
    """
    Converts the provided string to an appropriate bool representation, returning the
    default value if the conversion isn't possible.

    This is needed because the default 'bool()' operator considers any string value to be 'True'
    which is definitely not what we want when we are reading environment variables.

    :param val: the string value to convert
    :param default: the default value to return if the conversion isn't possible
    :return: a bool representation of the provided string, or the default value
    """
    if val is None or len(val) == 0:
        return default

    if val.lower() in TRUTHY_VALUES:
        return True

    if val.lower() in FALSY_VALUES:
        return False

    return default
