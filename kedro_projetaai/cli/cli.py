"""ProjetaAi CLI extension pack setup."""
from typing import Dict, List, Sequence, Type, Union
import click
import importlib
import importlib.metadata

from kedro_projetaai.cli.plugin import ProjetaAiCLIPlugin
from kedro_projetaai.cli.constants import (
    CLI_MODULES,
    CLI_MODULES_HELP,
    ENTRY_POINTS,
)


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
    for cli_module in CLI_MODULES:
        try:
            cli_mod = importlib.import_module(
                f'kedro_projetaai.cli.{cli_module}'
            )
            subgroups[cli_module] = getattr(cli_mod, cli_module)
        except Exception:
            subgroups[cli_module] = click.Group(
                cli_module, help=CLI_MODULES_HELP.get(cli_module, '')
            )
    return subgroups


def _import_plugins() -> Dict[str, Dict[str, List[click.Command]]]:
    """Imports plugins and returns a dict of commands by subgroup by plugins.

    Returns:
        Dict[str, Dict[str, List[click.Command]]]:
            Commands by subgroup by plugins.
    """
    entry_points = importlib.metadata.entry_points()
    plugins: Dict[str, Type[ProjetaAiCLIPlugin]] = {
        plugin.name: plugin.load()
        for plugin in entry_points.get(ENTRY_POINTS['CLI'], [])
    }

    plugins_commands = {}
    for name, plugin in plugins.items():
        commands = {}
        for subgroup, command_list in plugin().get_commands().items():
            commands[subgroup] = commands.get(subgroup, []) + command_list
        plugins_commands[name] = commands

    return plugins_commands


def _count_commands(
    commands: Sequence[Union[click.Command, click.Group]]
) -> int:
    """Count the number of commands in a list of commands or groups.

    Args:
        commands (Sequence[Union[click.Command, click.Group]]):
            List of commands.

    Returns:
        int: Number of commands.
    """
    return sum(not isinstance(command, click.Group) for command in commands)


def _preprocess_group(
    plugin: str,
    group: click.Group,
    command_or_group: Union[click.Command, click.Group],
    length: int,
):
    if isinstance(command_or_group, click.Group):
        commands = tuple(command_or_group.commands.values())
        for command in commands:
            _preprocess_group(
                plugin, command_or_group, command, _count_commands(commands)
            )
        group.add_command(command_or_group)
    elif length == 1:
        if command_or_group.name in group.commands:
            del group.commands[command_or_group.name]
        command_or_group.name = plugin
        group.add_command(command_or_group)
    else:
        plugin_group = group.commands.get(plugin, click.Group(plugin))
        plugin_group.add_command(command_or_group)
        group.add_command(plugin_group)


def _preprocess_plugins(
    groups: Dict[str, click.Group],
    plugins: Dict[str, Dict[str, List[Union[click.Command, click.Group]]]],
) -> Dict[str, Dict[str, click.Group]]:
    """Performs changes in raw groups from plugins.

    Args:
        groups (Dict[str, click.Group]): Subgroups by name.
        plugins (Dict[str, Dict[str, List[Union[click.Command, click.Group]]]])

    Returns:
        Dict[str, Dict[str, click.Group]]: Plugins subgroups by name.
    """
    processed_plugins = {}
    for plugin, plugin_groups in plugins.items():
        processed_groups = {}
        for group_name in groups:
            clean_group = click.Group(group_name)
            commands = plugin_groups.get(group_name, [])
            for group_or_command in commands:
                _preprocess_group(
                    plugin, clean_group, group_or_command,
                    _count_commands(commands)
                )
            processed_groups[group_name] = clean_group
        processed_plugins[plugin] = processed_groups
    return processed_plugins


def _merge_groups(a: Dict[str, click.Group], b: Dict[str, click.Group]):
    """Merge two click groups into the first.

    Args:
        a (Dict[str, click.Group]): First group.
        b (Dict[str, click.Group]): Second group.
    """
    for key in b:
        if (
            key in a and isinstance(a[key], click.Group)
            and isinstance(b[key], click.Group)
        ):
            _merge_groups(a[key].commands, b[key].commands)
        else:
            a[key] = b[key]


def _install_plugins(
    entry: click.Group, subgroups: Dict[str, click.Group],
    plugins: Dict[str, Dict[str, List[click.Command]]]
):
    """Installs plugins into the CLI.

    Args:
        entry (click.Group): The CLI entry point.
        subgroups (Dict[str, click.Group]): Subgroups by name.
        plugins (Dict[str, Dict[str, List[click.Command]]]):
            Commands by subgroup by plugins.
    """
    processed_plugins = _preprocess_plugins(subgroups, plugins)
    for plugin_subgroups in processed_plugins.values():
        _merge_groups(subgroups, plugin_subgroups)
    for subgroup in subgroups.values():
        entry.add_command(subgroup)


def setup_cli() -> click.Group:
    """Setup CLI."""
    subgroups = _import_subgroups()
    plugins = _import_plugins()
    _install_plugins(projetaai, subgroups, plugins)

    return projetaai
