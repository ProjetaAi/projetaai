"""Template creation commands."""
import os
from typing import Any
from cookiecutter.main import cookiecutter
import click
from kedro.framework.cli.starters import new as kedro_new
from .constants import PYTHON_VERSION
from kedro_projetaai.cli.starter import ci_templates
from kedro import __version__ as kedro_version

from ..utils.io import move_files


@click.group()
def new(*args: Any, **kwargs: Any):
    """Commands for creating templated structures."""
    pass


kedro_new.name = 'project'
new.add_command(kedro_new)


@click.command()
@click.option(
    '--starter',
    help='CI starter to use. Run kedro starter list ci to see available '
         'starters.',
    required=True
)
@click.option(
    '--checkout',
    help='Tag or branch to checkout',
    default=kedro_version
)
def ci(starter: str, checkout: str):
    """Creates a new CI configuration file."""
    template = ci_templates[starter]
    folder = cookiecutter(
        template.template_path,
        directory=template.directory,
        overwrite_if_exists=True,
        checkout=checkout,
        extra_context={
            "_pipelines": template.alias,
            "__python_version": PYTHON_VERSION
        },
    )

    if template.move_to_root:
        root = os.path.dirname(folder)
        move_files(folder, root)
        folder = root

    click.echo(f'CI configuration created in {folder}')
    click.echo('Please review the configuration and commit it to your repo.')
    click.echo('You may find instruction about how to use it in '
               f'{template.template_path} under '
               f'{template.directory}/README.md')


new.add_command(ci)
