"""Kedro plugin interface."""
from kedro_projetaai.cli.cli import setup_cli
from kedro_projetaai.overrides import (
    override_get_credentials
)


class ProjetaAiOverrides:
    """Fake Kedro hook to enable ProjetaAi overrides."""

    def __init__(self):
        """Initialize ProjetaAi overrides."""
        override_get_credentials()


overrides = ProjetaAiOverrides()
cli = setup_cli()
