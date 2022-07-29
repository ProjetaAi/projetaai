"""Kedro plugin interface."""
from kedro_projetaai.cli.cli import setup_cli
from kedro_projetaai.overrides import (
    ProjetaAiOverrides,
)


overrides = ProjetaAiOverrides()
cli = setup_cli()
