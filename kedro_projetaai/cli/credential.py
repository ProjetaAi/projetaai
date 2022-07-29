"""Credentials commands."""
import click


@click.group()
@click.pass_context
def credential(ctx: click.Context):
    """Credentials management."""
    pass
