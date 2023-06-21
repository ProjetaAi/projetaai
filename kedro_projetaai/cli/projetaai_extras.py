import click
import os
import shutil
from kedro_projetaai.utils.datasets import extras
from kedro.framework.context import KedroContext
import toml


@click.group()
def projetaai_extras():
    """custom projetaai cli"""
    pass


@projetaai_extras.command()
def extra_datasets():
    """
    add custom datasets to kedro"""
    click.echo("copying extras datasets to kedro catalog")
    project_name = toml.load("pyproject.toml")["tool"]["kedro"]["project_name"]
    module_dir = os.path.dirname(extras.__file__)
    shutil.copytree(
        module_dir,
        os.path.join(os.getcwd(), f"src/{project_name}/extras/datasets"),
        dirs_exist_ok=True,
    )
    line_to_add = f"from {project_name}.extras.datasets.extras import * # NOQA"
    with open(os.path.join(os.getcwd(), f"src/{project_name}/__init__.py"), "r+") as f:
        lines = f.readlines()
        if line_to_add not in (line.strip() for line in lines):
            f.seek(0)
            f.write(line_to_add + "\n" + "".join(lines))
    click.echo("done copying extras datasets to kedro catalog")
