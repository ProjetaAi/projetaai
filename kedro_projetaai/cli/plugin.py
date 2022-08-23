"""Package containing CLI plugin creation tools."""
from typing import Dict, Iterator, List, Union
from click import Command, Group
import click
from kedro_projetaai.utils.iterable import optionaltolist
from kedro_projetaai.cli.constants import CLI_MODULES
from kedro.framework.cli.starters import KedroStarterSpec
from attr import define


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
        {'model': [<Command print-option>, <Group deploy>, <Group register>],
         'pipeline': [<Command print-option>, <Group create>]}
    """

    @property
    def credential(self) -> Union[Command, List[Command]]:
        """Commands for credential management.

        Returns:
            List[Command]: List of credential commands.
        """
        pass

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
    def model_deploy(self) -> Union[Command, List[Command]]:
        """Commands for creating an inference endpoint.

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

    def _get_commands(
        self,
        group: Group,
        commands: List[Union[Command, Group]],
        parts: Iterator[str],
        part: str,
    ) -> Group:
        if group is None or group.name != part:
            return self._get_commands(click.Group(part), commands, parts, part)
        elif group.name == part:
            part = next(parts, None)
            if part is None:
                for command in commands:
                    group.add_command(command)
            else:
                group.add_command(
                    self._get_commands(
                        group.commands.get(part, None), commands, parts, part
                    )
                )
            return group

    def get_commands(self) -> Dict[str, List[Union[Command, Group]]]:
        """Return all commands of this plugin.

        Returns:
            List[Command]: List of commands.
        """
        groups = {}
        for group_name in CLI_MODULES:
            group = Group(group_name)
            for method in dir(self):
                if method.startswith(group_name):
                    commands = optionaltolist(getattr(self, method))
                    if commands:
                        method_parts = iter(method.split('_'))
                        self._get_commands(
                            group, commands, method_parts, next(method_parts)
                        )

            if group.commands:
                groups[group_name] = list(group.commands.values())

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
