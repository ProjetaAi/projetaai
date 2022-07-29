"""Package for io interaction."""
import tomli


def readtoml(filepath: str) -> dict:
    """Reads a `.toml` file as a dict.

    Args:
        filepath (str)

    Returns:
        dict
    """
    with open(filepath, 'rb') as f:
        return tomli.load(f)
