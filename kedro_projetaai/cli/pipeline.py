"""Pipeline commands."""
import click


@click.group()
@click.pass_context
def pipeline(ctx: click.Context):
    """Pipeline management."""
    pass
