"""
this module contains methods to load and dump pickle files from/to a filesystem
it exists so its already possible to be used with the other methods in the readers.py module
"""

import pickle
from typing import Optional
import fsspec
from fsspec.utils import infer_storage_options


def infer_filesystem_method(path: str, _filesystem_method: Optional[dict]) -> dict:

    """
    This function is used to infer the filesystem method from a given path
    """

    if _filesystem_method is None:
        _filesystem_method = infer_storage_options(path)
        _filesystem_method.pop("path")
    return _filesystem_method


def pickle_load(path: str, storage_options: dict, _filesystem_method: Optional[dict] = None, **kwargs) -> object:

    """

    """

    _filesystem_method = infer_filesystem_method(path, _filesystem_method)
    with fsspec.open(path, mode="rb", **_filesystem_method, **storage_options) as file:
        return pickle.load(file, **kwargs)  # type: ignore


def pickle_dump(data, path, storage_options, _filesystem_method=None, **kwargs) -> None:
    _filesystem_method = infer_filesystem_method(path, _filesystem_method)
    with fsspec.open(path, "wb", **_filesystem_method, **storage_options) as file:
        file.write(pickle.dumps(data, **kwargs))  # type: ignore
    return
