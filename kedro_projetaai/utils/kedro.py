"""Utilitary functions for kedro data."""
import os
from pathlib import Path
from kedro.io import DataCatalog
from kedro.framework.session import KedroSession
from kedro_projetaai.utils.io import readtoml


def read_kedro_pyproject() -> dict:
    r"""Reads the kedro section from the pyproject.toml file.

    Raises:
        KeyError: If the kedro section is not found in the pyproject.toml file.

    Returns:
        dict: The kedro section of the pyproject.toml file.

    Example:
        >>> _ = fs.create_file('pyproject.toml', '[tool.kedro]\na=1')
        >>> read_kedro_pyproject()
        {'a': 1}

        >>> _ = fs.create_file('pyproject.toml', '')
        >>> read_kedro_pyproject()  # doctest: +ELLIPSIS
        Traceback (most recent call last):
        ...
        KeyError: 'No "tool.kedro" section in "pyproject.toml"'
    """
    pyproject = readtoml(str(Path.cwd() / 'pyproject.toml'))
    try:
        return pyproject['tool']['kedro']
    except KeyError:
        raise KeyError('No "tool.kedro" section in "pyproject.toml"')


def get_catalog() -> DataCatalog:
    r"""Get the catalog from the project.

    Returns:
        DataCatalog: The catalog from the project.

    Example:
        >>> _ = fs.create_kedro_project()
        >>> _ = fs.create_file(
        ...     'conf/base/catalog.yml',
        ...     'a:\n  type: pickle.PickleDataSet\n  filepath: a.pickle')
        >>> get_catalog()._data_sets  # doctest: +ELLIPSIS
        {...'a': <...PickleDataSet object at ...>...}
    """
    pyproject = read_kedro_pyproject()
    with KedroSession.create(
        package_name=pyproject['package_name'], project_path=os.getcwd()
    ) as session:
        return session.load_context()._get_catalog()
