from typing import Optional

TRUTHY_VALUES = ['true', '1', 'yes', 'on']
FALSY_VALUES = ['false', '0', 'no', 'off']

def str_as_bool(val: str, default:Optional[bool] = None) -> Optional[bool]:
    if val is None or len(val) == 0:
        return default

    if val.lower() in TRUTHY_VALUES:
        return True

    if val.lower() in FALSY_VALUES:
        return False

    return default
