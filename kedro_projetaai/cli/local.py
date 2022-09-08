"""ProjetaAi local environment commands."""
import click
from click import Command
from kedro_projetaai.cli import ProjetaAiCLIPlugin
from kedro.framework.cli.project import run
from kedro.framework.cli.pipeline import create_pipeline
from typing import Tuple


@click.command()
@click.option('--port', default=3000, type=int, help='Port to bind to.')
@click.option('--script', required=True, help='Path to the script.')
@click.option('--debug', default=False, is_flag=True, help='Debug mode.')
def serve_local(port: int, script: str, debug: bool):
    """Creates a local inference server serving on post "/"."""
    import waitress
    from flask import Flask, request
    from flask_cors import CORS
    from kedro_projetaai.cli.run import read_kedro_pyproject
    from kedro_projetaai.serving.model import (
        ValidResponses,
        Scorer
    )
    from kedro_projetaai.utils.kedro import get_catalog

    name = read_kedro_pyproject()['package_name']
    app = Flask(name)
    CORS(app)

    fn = Scorer(script, get_catalog())

    @app.post('/')
    def inference() -> Tuple[ValidResponses, int]:
        """Inference endpoint."""
        body = request.json
        return fn(body)

    if debug:
        app.run(port=port, debug=True)
    else:
        waitress.serve(app, port=port)


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

    @property
    def model_deploy(self) -> Command:
        """Model deploy command.

        Returns:
            Command: Local serving command.
        """
        return serve_local
