"""ProjetaAi CLI extension pack setup."""
from typing import Dict, List, Type
import click
import importlib
import importlib.metadata

from projetaai.plugins.cli.cli import CLIPlugin, _is_command_as_group

_CLI_MODULES = [
    'model',
    'pipeline',
    'credential',
    'catalog',
    'datastore',
    'run',
]


@click.group()
@click.pass_context
def projetaai(ctx: click.Context):
    """ProjetaAI CLI."""  # noqa: D403
    pass


def _import_subgroups() -> Dict[str, click.Group]:
    """Imports all subgroups and returns a dict of groups by name.

    Returns:
        Dict[str, click.Group]: Groups by name.
    """
    subgroups = {}
    for cli_module in _CLI_MODULES:
        try:
            cli_mod = importlib.import_module(
                f'projetaai._framework.cli.{cli_module}'
            )
            subgroups[cli_module] = getattr(cli_mod, cli_module)
        except Exception:
            pass
    return subgroups


def _import_plugins() -> Dict[str, Dict[str, List[click.Command]]]:
    """Imports plugins and returns a dict of commands by subgroup by plugins.

    Returns:
        Dict[str, Dict[str, List[click.Command]]]:
            Commands by subgroup by plugins.
    """
    entry_points = importlib.metadata.entry_points()
    plugins: Dict[str, Type[CLIPlugin]] = {
        plugin.name: plugin.load()
        for plugin in entry_points.get('projetaai.plugins.cli', [])
    }

    plugins_commands = {}
    for name, plugin in plugins.items():
        commands = {}
        for subgroup, command_list in plugin().get_commands().items():
            commands[subgroup] = commands.get(subgroup, []) + command_list
        plugins_commands[name] = commands

    return plugins_commands


def _install_plugins(
    entry: click.Group,
    subgroups: Dict[str, click.Group],
    plugins: Dict[str, Dict[str, List[click.Command]]]
):
    """Installs plugins into the CLI.

    Args:
        entry (click.Group): The CLI entry point.
        subgroups (Dict[str, click.Group]): Subgroups by name.
        plugins (Dict[str, Dict[str, List[click.Command]]]):
            Commands by subgroup by plugins.
    """
    for group_name, group in subgroups.items():
        for plugin_name, plugin_commands in plugins.items():
            plugin_group = click.Group(plugin_name)
            commands = plugin_commands.get(group_name, [])
            for command in commands:
                if _is_command_as_group(command):
                    assert len(commands) == 1, f'"{command}" is default but ' \
                        f'other commands are defined under "{group_name}" ' \
                        f'in the plugin "{plugin_name}"'
                    command.name = plugin_name
                    group.add_command(command)
                    break
                else:
                    plugin_group.add_command(command)
            else:
                group.add_command(plugin_group)
        entry.add_command(group)


def setup_cli() -> click.Group:
    """Setup CLI."""
    subgroups = _import_subgroups()
    plugins = _import_plugins()
    _install_plugins(projetaai, subgroups, plugins)

    return projetaai
