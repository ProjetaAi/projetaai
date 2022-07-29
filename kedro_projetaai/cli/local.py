"""ProjetaAi local environment commands."""
from click import Command
from kedro_projetaai.cli import ProjetaAiCLIPlugin
from kedro.framework.cli.project import run
from kedro.framework.cli.pipeline import create_pipeline


class LocalCLI(ProjetaAiCLIPlugin):
    """ProjetaAi CLI plugin for local environment management."""

    @property
    def pipeline_create(self) -> Command:
        """Pipeline create command.

        Returns:
            Command
        """
        return create_pipeline

    @property
    def run(self) -> Command:
        """Kedro run command.

        Returns:
            Command
        """
        return run
