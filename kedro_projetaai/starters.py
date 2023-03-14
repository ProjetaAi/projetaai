"""Starter templates definition."""
from kedro.framework.cli.starters import KedroStarterSpec

STARTERS_REPO = "git+https://github.com/ProjetaAi/projetaai-starters.git"

project_starters = [
    KedroStarterSpec(
        alias="projetaai",
        template_path=STARTERS_REPO,
        directory="for_projetaai/project/projetaai",
    ),
    KedroStarterSpec(
        alias="projetaai_mp",
        template_path=STARTERS_REPO,
        directory="for_projetaai/project/projetaai_mp",
    ),
    KedroStarterSpec(
        alias="projetaai_adbc",
        template_path=STARTERS_REPO,
        directory="for_projetaai/project/projetaai_adbc",
    ),
]
