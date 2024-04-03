"""CLI constants."""
import sys


CLI_MODULES = [
    "model",
    "pipeline",
    "credential",
    "datastore",
    "run",
    "new",
    "starter",
    "repodevops",
    "pipelinedevops",
    "parametersdevops",
]

CLI_MODULES_HELP = {
    "credential": "Credentials YML management.",
    "datastore": "Storage pointers management.",
}

ENTRY_POINTS = {
    "CLI": "projetaai.cli",
    "CI": "projetaai.ci",
    "PROJECT": "projetaai.projetaai_lab",
}

PYTHON_VERSION = (
    f"{sys.version_info.major}.{sys.version_info.minor}." f"{sys.version_info.micro}"
)
