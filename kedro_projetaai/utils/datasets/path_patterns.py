import re
from itertools import product
from typing import Generator, Optional

BASE_PATTERN_DATE = "{Y}{sep}{m}{sep1}{d}"
POSSIBLE_SEPARATORS = ['/', '-', '_', '']
POSSIBLE_FORMAT = [{'Y': r'\d{4}', 'm': r'\d{2}', 'd': r'\d{2}'},
                   {'Y': r'\d{2}', 'm': r'\d{2}', 'd': r''}]

def yield_patterns() -> Generator[str, None, None]:
    for sep, date_format in product(POSSIBLE_SEPARATORS, POSSIBLE_FORMAT):
        sep1 = '' if date_format.get('d') == '' else sep
        yield BASE_PATTERN_DATE.format(Y=date_format['Y'], m=date_format['m'], d=date_format['d'], sep=sep, sep1=sep1)

def string_format(string: str) -> str:
    if string.endswith('/'):
        return string
    else:
        return string.split('/')[-1]

def return_last_match(pattern: str, string: str) -> Optional[str]:
    match = re.findall(pattern, string)
    if match:
        return match[-1]
    return None

def match_date_pattern(string: str) -> tuple[str, str]:
    for pattern in yield_patterns():
        match = return_last_match(pattern, string)
        if match:
            return match, pattern
    else:
        raise ValueError(f"Date pattern not found in {string}")