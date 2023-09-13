"""Module to load and dump pickle files from/to a filesystem.

Creates functions to load and dump pickle files from/to cloud.
"""

import pickle
from typing import Any, Optional
import fsspec
from fsspec.utils import infer_storage_options


def infer_filesystem_method(path: str, _filesystem_method: Optional[dict]) -> dict:
    """This function is used to infer the filesystem method from a given path."""
    if _filesystem_method is None:
        _filesystem_method = infer_storage_options(path)
        _filesystem_method.pop("path")
    return _filesystem_method


def pickle_load(
    path: str,
    storage_options: dict,
    _filesystem_method: Optional[dict] = None,
    **kwargs
) -> object:
    """Function to load a pickle file from a given path.

    Uses fsspec.open to open the file and pickle.load to load the data from cloud
    """
    _filesystem_method = infer_filesystem_method(path, _filesystem_method)
    with fsspec.open(path, mode="rb", **_filesystem_method, **storage_options) as file:
        return pickle.load(file, **kwargs)  # type: ignore


def pickle_dump(
    data: bytes,
    path: str,
    storage_options: dict[str, Any],
    _filesystem_method: Optional[dict[str, Any]] = None,
    **kwargs
) -> None:
    """Function to dump a pickle file to a given path.

    It uses fsspec.open to open the file and pickle.dump to dump the data to
    interact with cloud
    """
    _filesystem_method = infer_filesystem_method(path, _filesystem_method)
    with fsspec.open(path, "wb", **_filesystem_method, **storage_options) as file:
        file.write(pickle.dumps(data, **kwargs))  # type: ignore
    return
