"""Utilitary package for string manipulation."""
import re


def to_snake_case(string: str) -> str:
    """Converts a string to snake case.

    Args:
        string (str)

    Returns:
        str

    Example:
        >>> to_snake_case('CamelCase')
        'camel_case'
    """
    return re.sub(r'(?<!^)(?=[A-Z])', '_', string).lower()
