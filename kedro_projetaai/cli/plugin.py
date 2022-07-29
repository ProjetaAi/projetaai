"""Package containing CLI plugin creation tools."""
from itertools import zip_longest
from typing import Dict, List, Union
from click import Command, Group
import click
from projetaai.utils.iterable import optionaltolist

_COMMAND_AS_GROUP_FLAG = 'command_as_group'


def command_as_group(cmd: Command) -> Command:
    """Decorator that sets a flag for a CLI command as the group default.

    Args:
        cmd (click.Command): CLI command to set as the group default.

    Returns:
        click.Command: CLI command with the group default flag set.
    """
    setattr(cmd, _COMMAND_AS_GROUP_FLAG, True)
    return cmd


def _is_command_as_group(cmd: Command) -> bool:
    """Returns whether a CLI command is the group default.

    Args:
        cmd (click.Command): CLI command to check.

    Returns:
        bool: Whether the CLI command is the group default.
    """
    return hasattr(cmd, _COMMAND_AS_GROUP_FLAG)


class ProjetaAiCLIPlugin:
    """Interface for creating a ProjetaAi CLI plugin.

    This class contains predefined properties for each subgroup of the CLI.
    By defining them, you may return a click command or a list of commands
    to be added to the property subgroup.

    Example:
        >>> import click
        >>> @click.command()
        ... @click.option('--option', default='default')
        ... def print_option(option):
        ...     print(option)

        >>> class MyPlugin(ProjetaAiCLIPlugin):
        ...    @property
        ...    def model(self) -> Union[Command, List[Command]]:
        ...        return print_option  # or as list if multiple
        ...    @property
        ...    def model_deploy_batch(self) -> Union[Command, List[Command]]:
        ...        return print_option  # or as list if multiple
        ...    @property
        ...    def model_register(self) -> Union[Command, List[Command]]:
        ...        return print_option  # or as list if multiple
        ...    @property
        ...    def pipeline_create(self) -> Union[Command, List[Command]]:
        ...        return print_option  # or as list if multiple
        ...    @property
        ...    def pipeline(self) -> Union[Command, List[Command]]:
        ...        return print_option  # or as list if multiple
        >>> MyPlugin().get_commands()  # doctest: +NORMALIZE_WHITESPACE
        {'model': [<Group register>, <Group deploy>, <Command print-option>],
         'pipeline': [<Group create>, <Command print-option>]}
    """

    @property
    def credential(self) -> Union[Command, List[Command]]:
        """Commands for credential management.

        Returns:
            List[Command]: List of credential commands.
        """
        return []

    @property
    def credential_create(self) -> Union[Command, List[Command]]:
        """Commands for credential creation.

        Returns:
            Union[Command, List[Command]]: Command or list of commands.
        """
        pass

    @property
    def credential_delete(self) -> Union[Command, List[Command]]:
        """Commands for credential deletion.

        Returns:
            Union[Command, List[Command]]: Command or list of commands.
        """
        pass

    @property
    def model(self) -> Union[Command, List[Command]]:
        """Commands for model management.

        Returns:
            List[Command]: List of model commands.
        """
        pass

    @property
    def model_register(self) -> Union[Command, List[Command]]:
        """Commands for registering models.

        Returns:
            Union[Command, List[Command]]: Command or list of commands.
        """
        pass

    @property
    def model_deploy_batch(self) -> Union[Command, List[Command]]:
        """Commands for creating a batch inference endpoint.

        Returns:
            Union[Command, List[Command]]: Command or list of commands.
        """
        pass

    @property
    def model_deploy_realtime(self) -> Union[Command, List[Command]]:
        """Commands for creating a realtime inference endpoint.

        Returns:
            Union[Command, List[Command]]: Command or list of commands.
        """
        pass

    @property
    def pipeline(self) -> Union[Command, List[Command]]:
        """Commands for pipeline management.

        Returns:
            List[Command]: List of pipeline commands.
        """
        pass

    @property
    def pipeline_create(self) -> Union[Command, List[Command]]:
        """Commands for creating a new pipeline.

        Returns:
            Union[Command, List[Command]]: Command or list of commands.
        """
        pass

    @property
    def pipeline_schedule(self) -> Union[Command, List[Command]]:
        """Commands for scheduling a pipeline.

        Returns:
            Union[Command, List[Command]]: Command or list of commands.
        """
        pass

    @property
    def run(self) -> Union[Command, List[Command]]:
        """Commands for running the project.

        Returns:
            Union[Command, List[Command]]: Command or list of commands.
        """
        pass

    @property
    def datastore(self) -> Union[Command, List[Command]]:
        """Commands for datastore management.

        Returns:
            Union[Command, List[Command]]: Command or list of commands.
        """
        pass

    @property
    def datastore_create(self) -> Union[Command, List[Command]]:
        """Commands for datastore creation.

        Returns:
            Union[Command, List[Command]]: Command or list of commands.
        """
        pass

    @property
    def catalog(self) -> Union[Command, List[Command]]:
        """Commands for catalog management.

        Returns:
            Union[Command, List[Command]]: Command or list of commands.
        """
        pass

    @property
    def catalog_assign_list(self) -> Union[Command, List[Command]]:
        """Commands that return a list of credential entries by dict path.

        Returns:
            Union[Command, List[Command]]: Command or list of commands.
        """
        pass

    def get_commands(self) -> Dict[str, List[Command]]:
        """Return all commands of this plugin.

        Returns:
            List[Command]: List of commands.
        """
        subgroups = {}
        for subgroup in [
            'credential',
            'model',
            'pipeline',
            'run',
            'datastore',
            'catalog',
        ]:
            stack: List[Group] = [click.Group(subgroup)]
            derived_methods: List[str] = sorted([
                method for method in dir(self) if method.startswith(subgroup)
            ], reverse=True)

            for method in derived_methods:

                method_cmds = getattr(self, method)
                if method_cmds:
                    for i, (part, group) in enumerate(
                        zip_longest(method.split('_'), stack, fillvalue=None)
                    ):
                        if part is None:  # no more parts
                            stack = stack[:i]
                            break

                        click_group = click.Group(part)
                        if not group:  # new group
                            stack[-1].add_command(click_group)
                            stack.append(click_group)

                        elif group.name != part:  # clean and new group
                            stack = stack[:i]
                            stack[-1].add_command(click_group)
                            stack.append(click_group)

                    for cmd in optionaltolist(method_cmds):
                        stack[-1].add_command(cmd)

            commands = list(stack[0].commands.values())
            if commands:
                subgroups[subgroup] = commands

        return subgroups
