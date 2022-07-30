"""Run commands."""
from pathlib import Path
import click
from kedro.framework.project import configure_project
import sys
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


@click.group()
@click.pass_context
def run(ctx: click.Context):
    """Project execution."""
    sys.path.append(str(Path.cwd() / 'src'))
    section = read_kedro_pyproject()
    configure_project(section['package_name'])
