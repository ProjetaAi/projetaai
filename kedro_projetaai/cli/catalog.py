"""Catalog setup and forwardings."""
import click
from kedro.framework.cli.catalog import create_catalog, list_datasets


@click.group()
@click.pass_context
def catalog(ctx: click.Context):
    """Catalog YML management."""
    pass


catalog.add_command(create_catalog)
catalog.add_command(list_datasets)
