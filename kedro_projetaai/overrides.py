"""Kedro overrides package to extend Kedro capabilities."""
from typing import Dict
from kedro.io import data_catalog

from kedro_projetaai.utils.iterable import get_nested
from kedro_projetaai.utils.typing import T


class ProjetaAiOverrides:
    """Fake Kedro hook to enable ProjetaAi overrides."""

    def __init__(self):
        """Initialize ProjetaAi overrides."""
        override_get_credentials()


def override_get_credentials():
    """Enable nested dictionaries in get_credentials.

    Example:
        >>> override_get_credentials()
        >>> data_catalog._get_credentials('a.b.c', {'a': {'b': {'c': 1}}})
        1

        >>> data_catalog._get_credentials(
        ...     'a.b.c.d',
        ...     {'a': {'b': {'c': 1}}})  # doctest: +ELLIPSIS
        Traceback (most recent call last):
        ...
        KeyError: "Unable to find credentials 'a.b.c.d'..."
    """
    _kedro_get_credentials = data_catalog._get_credentials

    def _get_credentials(credential_name: str,
                         credentials: Dict[str, T]) -> T:
        """Get the credential from the credentials dictionary.

        Args:
            credential_name (str): Name of the credential.
            credentials (Dict[str, T]): Credentials dictionary.

        Returns:
            T: The credential.
        """
        try:
            return get_nested(credentials, credential_name)
        except Exception:
            return _kedro_get_credentials(credential_name, credentials)

    data_catalog._get_credentials = _get_credentials
