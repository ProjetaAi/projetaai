"""Run commands."""
import click
from kedro.framework.cli.project import run as kedro_run
from kedro.framework.project import configure_project
import sys
from projetaai.utils.io import readtoml
from projetaai.utils.constants import CWD


def read_kedro_pyproject() -> dict:
    """Reads the kedro section from the pyproject.toml file.

    Raises:
        KeyError: If the kedro section is not found in the pyproject.toml file.

    Returns:
        dict: The kedro section of the pyproject.toml file.
    """
    pyproject = readtoml(str(CWD / 'pyproject.toml'))
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
    sys.path.append(str(CWD / 'src'))
    section = read_kedro_pyproject()
    configure_project(section['package_name'])


kedro_run.help = 'Runs locally.'
run.add_command(kedro_run, name='kedro')
