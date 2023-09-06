"""Autouse fixtures for tests."""
import logging
import pytest
from pprint import pprint
from kedro_pytest import TestKedro


@pytest.fixture(autouse=True)
def disable_logging(doctest_namespace: dict):
    """Disable logging for all doctests."""
    logging.disable(logging.ERROR)


@pytest.fixture(autouse=True)
def add_libs(doctest_namespace: dict, tkedro: TestKedro):
    """Add libraries to doctest namespace."""
    doctest_namespace["pprint"] = pprint
    doctest_namespace["kedro"] = tkedro
