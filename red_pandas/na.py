from typing import Iterable, Mapping
import math

NA = math.nan
NA_character_ = "__NA_character_"


def is_nan(x):
    try:
        if type(x) == str:
            raise TypeError()
        return [e == NA_character_ or (math.isfinite(e) and math.isnan(e)) for e in x]
    except TypeError:
        return x == NA_character_ or (math.isfinite(x) and math.isnan(x))


def ndim(x):
    if isinstance(x,str):
        return 0
    elif isinstance(x,Mapping):
        return 0
    elif isinstance(x,Iterable):
        if len(x)>0:
            if isinstance(x[0],str):
                return 1
            elif isinstance(x[0],Mapping):
                return 1
            elif isinstance(x[0],Iterable):
                return 2
            return 1
        return 1
    return 0

def is_scalar(x):
    return ndim(x) == 0


def is_finite(x):
    try:
        if type(x) == str:
            return False
        return [math.isfinite(e) for e in x]
    except TypeError:
        return math.isfinite(x)

