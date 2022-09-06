"""Inference script and model management backend."""
import inspect
import sys
from kedro.io import DataCatalog
import importlib.machinery
from typing import Callable, Protocol, Tuple, Union, runtime_checkable, Any


ValidResponses = Union[str, list, tuple, dict]


class ScriptException(Exception):
    """Exception for script errors.

    Example:
        >>> raise ScriptException('Script error')
        Traceback (most recent call last):
            ...
        kedro_projetaai.framework.model.ScriptException: Script error
    """

    pass


def assert_script(condition: bool, message: str = 'Invalid request'):
    """Asserts a condition and raises a ScriptException if it fails.

    Args:
        condition (bool): condition to assert.
        message (str, optional): response message.
            Defaults to 'Invalid request'.

    Raises:
        ScriptException: if the condition fails.

    Example:
        >>> assert_script(True, 'Invalid request')
        >>> assert_script(False, 'Invalid request')
        Traceback (most recent call last):
            ...
        kedro_projetaai.framework.model.ScriptException: Invalid request
    """
    if not condition:
        raise ScriptException(message)


@runtime_checkable
class ScriptSpec(Protocol):
    """Protocol for a script specification."""

    def init(catalog: DataCatalog) -> Any:
        """Obtains any necessary resources for the prediction.

        Args:
            catalog (DataCatalog): Kedro catalog.

        Returns:
            Any: Resource, usually the model.
        """
        ...  # pragma: no cover

    def prepare(data: Any) -> Any:
        """Prepares the request data for prediction.

        Args:
            data (Any): parsed request data.

        Returns:
            Any: prepared data.
        """
        ...  # pragma: no cover

    def predict(model: Any, data: Any) -> ValidResponses:
        """Makes a prediction of a given request.

        Args:
            model (Any): init return.
            data (Any): prepare return.

        Returns:
            Any: prediction.
        """
        ...  # pragma: no cover


def _get_script(script: str) -> ScriptSpec:
    """Get the functions from the script.

    Args:
        script (str): path to the script.

    Returns:
        ScriptSpec: script module.
    """
    if 'script' in sys.modules:
        del sys.modules['script']

    mod = importlib.machinery.SourceFileLoader("script", script).load_module()

    assert isinstance(mod, ScriptSpec),\
        ('Script doesn\'t implement all the following functions:\n'
         + '\n'.join([str(inspect.signature(getattr(ScriptSpec, fn)))
                      for fn in dir(ScriptSpec)
                      if not fn.startswith('_')]))

    return mod


def generate_inference_func(
    script: str, catalog: DataCatalog
) -> Callable[[Any], Tuple[Any, int]]:
    """Generates the inference function.

    Args:
        script (str): path to the script.
        catalog (DataCatalog): a catalog for loading models.

    Returns:
        Callable[[Any], Tuple[Any, int]]: inference function. This function
            takes the request data and returns a tuple with the response and
            the status code.
    """
    mod = _get_script(script)
    model = mod.init(catalog)

    def inference(body: Any) -> Tuple[ValidResponses, int]:
        """Inference endpoint."""
        try:
            data = mod.prepare(body)
            result = mod.predict(model, data)
            return result, 200  # OK
        except ScriptException as e:
            return str(e), 400  # bad request
        except Exception as e:
            return str(e), 500  # internal server error

    return inference
