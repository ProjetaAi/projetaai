"""Model commands."""
import click


@click.group()
@click.pass_context
def model(ctx: click.Context):
    """Model register and deploy."""
    pass
