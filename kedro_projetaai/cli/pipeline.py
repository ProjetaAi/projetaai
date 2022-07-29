"""Pipeline command setup and forwardings."""
import click
from kedro.framework.cli.pipeline import delete_pipeline


@click.group()
@click.pass_context
def pipeline(ctx: click.Context):
    """Pipeline management."""
    pass


pipeline.add_command(delete_pipeline)
