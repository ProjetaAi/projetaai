"""Utils for iterable manipulation."""

from functools import reduce
from typing import Dict, List
from flatten_dict import flatten, unflatten

from kedro_projetaai.utils.typing import T


def tolist(val: T) -> List[T]:
    """Transform an object into list if not list.

    Args:
        val (T)

    Returns:
        List[T]

    Example:
        >>> tolist(3)
        [3]
        >>> tolist([3])
        [3]
    """
    if isinstance(val, list):
        return val
    else:
        return [val]


def optionaltolist(val: T) -> List[T]:
    """Return a list of the value if it is not None, otherwise, an empty list.

    Args:
        val (T): The value to convert to list

    Returns:
        List[T]: The value as list

    Example:
        >>> optionaltolist(3)
        [3]

        >>> optionaltolist(None)
        []
    """
    if val is None:
        return []
    else:
        return tolist(val)


def get_nested(dictionary: Dict[str, T], key: str) -> T:
    """Return the value of a nested key in a dictionary.

    Args:
        dictionary (dict)
        key (str)

    Returns:
        T

    Example:
        >>> get_nested({'a': {'b': {'c': 3}}}, 'a.b.c')
        3
    """
    return reduce(lambda d, k: d[k], key.split("."), dictionary)


def mergedicts(a: dict, b: dict) -> dict:
    """Merges two dictionaries recursively.

    Args:
        a (dict)
        b (dict)

    Returns:
        dict

    Example:
        >>> mergedicts({'a': {'b': 2}}, {'a': {'c': 4}})
        {'a': {'b': 2, 'c': 4}}
    """
    fa = flatten(a)
    fb = flatten(b)
    return unflatten({**fa, **fb})
