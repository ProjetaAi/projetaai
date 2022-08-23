"""Model management commands."""
import click


@click.group()
def model():
    """Model management."""
    pass


@model.group()
def deploy():
    """Model serving for inference."""
    pass
