"""Starter commands."""
from typing import Dict, List, cast
import click
from kedro.framework.cli.starters import (
    starter,
    list_starters as kedro_list_starters,
)
import importlib.metadata
from kedro_projetaai.cli.constants import ENTRY_POINTS
from kedro_projetaai.cli.plugin import CIStarterSpec


def _get_ci_templates() -> Dict[str, CIStarterSpec]:
    return {
        template.alias: template
        for plugin in
        importlib.metadata.entry_points().get(ENTRY_POINTS['CI'], [])
        for template in cast(List[CIStarterSpec], plugin.load())
    }


ci_templates = _get_ci_templates()
starter.commands = {}


@click.group()
def list():
    """Lists available starters."""
    pass


kedro_list_starters.name = 'project'
list.add_command(kedro_list_starters)


@click.command()
def ci():
    """Lists available CI starters."""
    click.echo('')
    for name, template in ci_templates.items():
        click.echo(f'{name}:')
        click.echo(f'  template_path: {template.template_path}')
        click.echo(f'  directory: {template.directory}')
    click.echo('')


list.add_command(ci)

starter.add_command(list)
