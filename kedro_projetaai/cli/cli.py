"""ProjetaAi CLI extension pack setup."""
from functools import reduce
from typing import Dict, List, Type, Union
import click
import importlib
import importlib.metadata
from kedro_projetaai.cli.ci import ci

from kedro_projetaai.cli.plugin import ProjetaAiCLIPlugin
from kedro_projetaai.cli.constants import (
    ENTRY_POINTS,
)


@click.group()
@click.pass_context
def projetaai(ctx: click.Context):
    """ProjetaAI CLI."""  # noqa: D403
    pass


def _import_plugins() -> Dict[str, ProjetaAiCLIPlugin]:
    # Imports CLI plugins and sorts them into dict keys <plugin_name> : <plugin_cli>
    entry_points = importlib.metadata.entry_points()
    plugins: Dict[str, Type[ProjetaAiCLIPlugin]] = {
        plugin.name: plugin.load()
        for plugin in entry_points.get(ENTRY_POINTS["CLI"], [])
    }
    return {name: plugin() for name, plugin in plugins.items()}


def _simplify_groups(
    plugins: Dict[str, ProjetaAiCLIPlugin],
) -> List[click.Group]:
    # Reduce the number of groups if it contains only one command
    def recursion(
        parent: click.Group,
        child: Union[click.Command, click.Group],
    ):
        # Recursion for `_simplify_groups`
        if isinstance(child, click.Group):
            children = tuple(child.commands.values())
            command_count = reduce(
                lambda acc, x: acc + (not isinstance(x, click.Group)), children, 0
            )
            if len(children) == command_count and command_count == 1:
                command = children[0]
                parent.commands.pop(str(child.name), None)
                command.name = child.name
                parent.add_command(command)
            else:
                for command in children:
                    recursion(child, command)
                parent.add_command(child)
        else:
            parent.add_command(child)

    processed_plugins = []
    for plugin_name, cli_plugin in plugins.items():
        plugin = click.Group(name=cli_plugin.name or plugin_name, help=cli_plugin.help)
        for group in cli_plugin.get_commands():
            recursion(plugin, group)
        processed_plugins.append(plugin)
    return processed_plugins


def _install_plugins(
    entry: click.Group,
    plugins: Dict[str, ProjetaAiCLIPlugin],
):
    for plugin in _simplify_groups(plugins):
        entry.add_command(plugin)


def setup_cli() -> click.Group:
    """Setup CLI."""
    plugins = _import_plugins()
    _install_plugins(projetaai, plugins)
    projetaai.add_command(ci)
    return projetaai
