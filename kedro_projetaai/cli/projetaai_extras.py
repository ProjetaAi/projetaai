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
    click.echo('copying extras datasets to kedro catalog')
    toml_file = toml.load('pyproject.toml')['tool']['kedro']['project_name']
    module_dir = os.path.dirname(extras.__file__)
    shutil.copytree(module_dir,
                    os.path.join(os.getcwd(),
                                 f'src/{toml_file}/extras/datasets'),
                    dirs_exist_ok=True)
    click.echo('done copying extras datasets to kedro catalog')
