"""Utilitary functions for kedro data."""
from pathlib import Path
from kedro.io import DataCatalog
from kedro.framework.session import KedroSession
from kedro_projetaai.utils.io import readtoml


def read_kedro_pyproject() -> dict:
    """Reads the kedro section from the pyproject.toml file.

    Raises:
        KeyError: If the kedro section is not found in the pyproject.toml file.

    Returns:
        dict: The kedro section of the pyproject.toml file.
    """
    pyproject = readtoml(str(Path.cwd() / 'pyproject.toml'))
    try:
        return pyproject['tool']['kedro']
    except KeyError as e:
        raise KeyError(
            'No "tool.kedro" section in "pyproject.toml"'
        ) from e


def get_catalog() -> DataCatalog:
    """Get the catalog from the project.

    Returns:
        DataCatalog: The catalog from the project.
    """
    project_path = read_kedro_pyproject()
    with KedroSession.create(project_path) as session:
        return session.load_context()._get_catalog()
