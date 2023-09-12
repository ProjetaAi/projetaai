"""
This module is used to find the date pattern in the path of the file.

this makes possible to filter the files by the date provided in the path
"""

import re
from itertools import product
from typing import Generator, Optional

BASE_PATTERN_DATE = "{Y}{sep}{m}{sep1}{d}"
POSSIBLE_SEPARATORS = ["/", "-", "_", ""]
POSSIBLE_FORMAT = [
    {"Y": r"\d{4}", "m": r"\d{2}", "d": r"\d{2}"},
    {"Y": r"\d{4}", "m": r"\d{2}", "d": r""},
]


def yield_patterns() -> Generator[tuple[str, str], None, None]:
    """Yields the possible patterns for the date."""
    for sep, date_format in product(POSSIBLE_SEPARATORS, POSSIBLE_FORMAT):
        sep1 = "" if date_format.get("d") == "" else sep
        yield BASE_PATTERN_DATE.format(
            Y=date_format["Y"],
            m=date_format["m"],
            d=date_format["d"],
            sep=sep,
            sep1=sep1,
        ), "-".join(["%" + i for i in date_format if date_format[i] != ""])


def string_format(string: str) -> str:
    """Returns the string without the file."""
    if string.endswith("/"):
        return string
    else:
        return string.split("/")[-1]


def return_last_match(pattern: str, string: str) -> Optional[str]:
    """Wrapper for re.findall that returns the last match."""
    match = re.findall(pattern, string)
    if match:
        return match[-1]
    return None


def match_date_pattern(string: str) -> tuple[str, str, str]:
    """Returns the date pattern that matches the string provided."""
    for pattern, date_format in yield_patterns():
        match = return_last_match(pattern, string)
        if match:
            return match, pattern, date_format
    else:
        raise ValueError(f"Date pattern not found in {string}")
