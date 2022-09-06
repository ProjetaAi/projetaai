"""Run commands."""
from pathlib import Path
import click
import sys
from kedro_projetaai.utils.kedro import read_kedro_pyproject


@click.group()
@click.pass_context
def run(ctx: click.Context):
    """Project execution."""
    from kedro.framework.project import configure_project
    sys.path.append(str(Path.cwd() / 'src'))
    section = read_kedro_pyproject()
    configure_project(section['package_name'])
