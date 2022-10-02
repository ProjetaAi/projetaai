"""CI YAMLs manipulation commands."""
import importlib.metadata
import os
from typing import Dict, List, cast
from cookiecutter.main import cookiecutter
import click
from kedro_projetaai.cli.plugin import CIStarterSpec
from .constants import PYTHON_VERSION, ENTRY_POINTS
from kedro import __version__ as kedro_version

from ..utils.io import move_files


ci_templates: Dict[str, CIStarterSpec] = {}


@click.group()
def ci():
    """Commands for creating CI YAMLs."""
    global ci_templates
    ci_templates = {
        template.alias: template
        for plugin in importlib.metadata.entry_points().get(ENTRY_POINTS["CI"], [])
        for template in cast(List[CIStarterSpec], plugin.load())
    }


@ci.command()
@click.option(
    "--starter",
    help="CI starter to use. Run kedro starter list ci to see available " "starters.",
    required=True,
)
@click.option("--checkout", help="Tag or branch to checkout", default=kedro_version)
def new(starter: str, checkout: str):
    """Creates a new CI configuration file."""
    template = ci_templates[starter]
    folder = cookiecutter(
        template.template_path,
        directory=template.directory,
        overwrite_if_exists=True,
        checkout=checkout,
        extra_context={
            "_pipelines": template.alias,
            "__python_version": PYTHON_VERSION,
        },
    )

    if template.move_to_root:
        root = os.path.dirname(folder)
        move_files(folder, root)
        folder = root

    click.echo(f"CI configuration created in {folder}")
    click.echo("Please review the configuration and commit it to your repo.")
    click.echo(
        "You may find instruction about how to use it in "
        f"{template.template_path} under "
        f"{template.directory}/README.md"
    )


@ci.command()
def list():
    """Lists available CI starters."""
    click.echo("")
    for name, template in ci_templates.items():
        click.echo(f"{name}:")
        click.echo(f"  template_path: {template.template_path}")
        click.echo(f"  directory: {template.directory}")
    click.echo("")
