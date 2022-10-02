"""Package containing CLI plugin creation tools."""
from typing import Iterator, List, Optional, Union, cast
from click import Command, Group
import click
from kedro_projetaai.utils.iterable import optionaltolist
from kedro_projetaai.cli.constants import CLI_MODULES
from kedro.framework.cli.starters import KedroStarterSpec
from attrs import define


class ProjetaAiCLIPlugin:
    """Interface for creating a ProjetaAi CLI plugin.

    This class contains predefined properties for each subgroup of the CLI.
    By defining them, you may return a click command or a list of commands
    to be added to the property subgroup. For example, if you wish to add
    the command ``kedro <plugin> model deploy``, just overwrite the ``model_deploy``
    property.

    If you return only one command, ProjetaAi will use the group as the command
    path. While if you return more than one command, ProjetaAi will nest them in its
    respective group. The same behaviour happens if you create subgroups.
    See the example tables below:

    | Method          | CLI command                       |
    | --------------- | --------------------------------- |
    | model_deploy.A  | kedro <plugin> model deploy       |

    | Method          | CLI command                       |
    | --------------- | --------------------------------- |
    | model_deploy.A  | kedro <plugin> model deploy A     |
    | model_deploy.B  | kedro <plugin> model deploy B     |

    | Method          | CLI command                       |
    | --------------- | --------------------------------- |
    | model_bar.A     | kedro <plugin> model bar A        |
    | model_bar_zee.B | kedro <plugin> model bar zee      |

    Note:
        Every method docstring is used for documenting the group help message. Thus, for
        creating more descriptive menus, don't forget to write docstrings for the
        intermediate groups.

    Note:
        You can find all CLI subgroups under
        ``kedro_projetaai.cli.constants.CLI_MODULES``.

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
        {'model': [<Command print-option>, <Group deploy>, <Group register>],
         'pipeline': [<Command print-option>, <Group create>]}
    """

    @property
    def name(self) -> Optional[str]:
        """CLI plugin name. If not specified, uses the ``importlib.metadata`` name."""
        pass

    @property
    def help(self) -> Optional[str]:
        """Help message this CLI plugin group."""
        pass

    @property
    def credential(self) -> Union[Command, List[Command]]:
        """Commands for credential management."""
        pass

    @property
    def credential_create(self) -> Union[Command, List[Command]]:
        """Commands for creating credentials."""
        pass

    @property
    def credential_delete(self) -> Union[Command, List[Command]]:
        """Commands for deleting credentials."""
        pass

    @property
    def model(self) -> Union[Command, List[Command]]:
        """Commands for model management."""
        pass

    @property
    def model_register(self) -> Union[Command, List[Command]]:
        """Commands for registering models."""
        pass

    @property
    def model_deploy(self) -> Union[Command, List[Command]]:
        """Commands for creating an inference services."""
        pass

    @property
    def pipeline(self) -> Union[Command, List[Command]]:
        """Commands for pipeline management."""
        pass

    @property
    def pipeline_create(self) -> Union[Command, List[Command]]:
        """Commands for creating new pipelines."""
        pass

    @property
    def pipeline_schedule(self) -> Union[Command, List[Command]]:
        """Commands for scheduling pipelines."""
        pass

    @property
    def run(self) -> Union[Command, List[Command]]:
        """Commands for running the project."""
        pass

    @property
    def datastore(self) -> Union[Command, List[Command]]:
        """Commands for datastore management."""
        pass

    @property
    def catalog(self) -> Union[Command, List[Command]]:
        """Commands for catalog management."""
        pass

    def _get_command_groups(self) -> List[Group]:
        def recursion(
            group: Optional[Group],
            commands: List[Union[Command, Group]],
            parts: Iterator[str],
            part: Optional[str],
        ) -> Group:
            if group is None or group.name != part:
                return recursion(click.Group(part), commands, parts, part)
            elif group.name == part:
                part = next(parts, None)
                if part is None:
                    for command in commands:
                        group.add_command(command)
                else:
                    group.add_command(
                        recursion(
                            cast(Optional[Group], group.commands.get(part, None)),
                            commands,
                            parts,
                            part,
                        )
                    )
            return group

        groups = []
        for group_name in CLI_MODULES:
            group = Group(group_name)
            for method_name in dir(self):
                if method_name.startswith(group_name):
                    commands = optionaltolist(getattr(self, method_name))
                    if commands:
                        method_parts = iter(method_name.split("_"))
                        recursion(group, commands, method_parts, next(method_parts))

            if group.commands:
                groups.append(group)

        return groups

    def _copy_docstrings(self, group: Group):
        def recursion(method: str, group: Group):
            for subname, subgroup in group.commands.items():
                if isinstance(subgroup, Group):
                    recursion(f"{method}_{subname}", subgroup)
            if hasattr(self, method):
                group.help = getattr(self.__class__, method).__doc__

        recursion(str(group.name), group)

    def get_commands(self) -> List[Group]:
        """Return all commands of this plugin.

        Returns:
            List[Group]: List of commands.
        """
        groups = self._get_command_groups()
        for group in groups:
            self._copy_docstrings(group)
        return groups


@define(order=True)
class CIStarterSpec(KedroStarterSpec):
    """Same as KedroStarterSpec, but for creating CI yamls.

    To create a CI starter, you must first create a variable that holds a list
    of CIStarterSpec objects. These objects contain the repository that stores
    the starter and what folder contains it.

    Example:
        >>> my_starters = [
        ...     CIStarterSpec(
        ...         alias="my-starter",
        ...         template_path="git+https://github.com/abc/def.git",
        ...         directory="my-starter/template")]

        Then you must point an entry point to this variable like this:

        .. code-block:: cfg

            #setup.cfg
            [options.entry_points]
            projetaai.starters.ci =
                myplugin = kedro_projetaai.starters:my_starters

    Attributes:
        alias (str): Alias for the starter.
        template_path (str): Path to the starter template.
        move_to_root (bool): Whether to move the cookiecutter folder contents
            to the root of the project.

    Note:
        When creating a CI starter, some variables builtin variables are
        available to the template. These are:

        - `_pipelines`: CIStarterSpec alias
        - `__python_version`: Python version

    See Also:
        [CI Starter Templates](https://github.com/ProjetaAi/projetaai-starters/tree/main/for_plugins/ci) # noqa: E501
    """

    move_to_root: bool = False
