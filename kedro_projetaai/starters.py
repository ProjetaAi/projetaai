"""Starter templates definition."""
from kedro.framework.cli.starters import KedroStarterSpec

STARTERS_REPO = 'git+https://github.com/ProjetaAi/projetaai-starters.git'

# project_starters = [
#     KedroStarterSpec(
#         alias='projetaai',
#         template_path=STARTERS_REPO,
#         directory='for_projetaai/project/projetaai',
#     ),
#     KedroStarterSpec(
#         alias='projetaai-multipipeline',
#         template_path=STARTERS_REPO,
#         directory='multipipeline',
#     ),
# ]

project_starters = [
    KedroStarterSpec(
        alias='projetaai-multipipeline',
        template_path="C:\\Users\\gustavo.machado\\Documents\\06_dev\\projetaai-starters\\multipipeline"
    )
]
