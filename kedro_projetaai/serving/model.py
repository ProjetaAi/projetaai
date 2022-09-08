"""Inference script and model management backend."""
import inspect
from logging import Logger, getLogger
import sys
from uuid import uuid4
from kedro.io import DataCatalog
import importlib.machinery
from typing import (
    Callable,
    Optional,
    Protocol,
    Tuple,
    Union,
    runtime_checkable,
    Any,
)
from pydantic import BaseModel

ValidResponses = Union[str, list, tuple, dict]


class ScriptException(Exception):
    """Exception for script errors.

    Example:
        >>> raise ScriptException('Script error')
        Traceback (most recent call last):
            ...
        kedro_projetaai.serving.model.ScriptException: Script error
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
        kedro_projetaai.serving.model.ScriptException: Invalid request
    """
    if not condition:
        raise ScriptException(message)


@runtime_checkable
class ScriptSpec(Protocol):
    """Protocol for a script specification.

    Variables:
        payload (Optional[BaseModel]): specifies how the request body should
            be.

    Methods:
        init (Callable[[DataCatalog], Any]): gets the model for scoring.
            it is only executed once, when the server starts.
        prepare (Callable[[Union[BaseModel, Any]], Any]): prepares the payload
            for scoring.
        predict (Callable[[Any, Any], Any]): receives the model and the
            prepared data, and returns the inference result.

    Raises:
        ScriptException: any controlled exception. When this exception is
            raised it tells the runner to return a bad request response. See
            `assert_script` and `ScriptException` for more information.
        Exception: any uncontrolled exception. When this exception is raised,
            it tells the runner to return an internal server error.
    """

    payload: Optional[BaseModel]

    def init(catalog: DataCatalog) -> Any:
        """Obtains any necessary resources for the prediction.

        Args:
            catalog (DataCatalog): Kedro catalog.

        Returns:
            Any: Resource, usually the model.
        """
        ...  # pragma: no cover

    def prepare(data: Union[BaseModel, Any]) -> Any:
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


class Scorer(Callable):
    """Scores a model given an inference script.

    Attributes:
        path (str): Path to the script file.
        catalog (DataCatalog): Catalog passed to the script init.
        _script (ScriptSpec): Script module.
        _model (Any): Return of the script init.
    """

    def __init__(self, path: str, catalog: DataCatalog):
        """Initializes a script Scorer.

        Args:
            path (str): Path to script file.
            catalog (DataCatalog): Catalog passed to the script init.
        """
        self.path = path
        self.catalog = catalog
        self._script: Union[None, ScriptSpec] = None
        self._model: Union[None, Any] = None

        # init
        self.script
        self.model

    @property
    def script(self) -> ScriptSpec:
        """Imports the script module.

        Returns:
            ScriptSpec: script module.
        """
        if not self._script:
            if 'script' in sys.modules:
                del sys.modules['script']

            mod = importlib.machinery.SourceFileLoader(
                "script",
                self.path,
            ).load_module()

            hasattr(mod, 'payload') or setattr(mod, 'payload', Any)

            assert isinstance(mod, ScriptSpec),\
                ('Script doesn\'t implement all the following functions:\n'
                 + '\n'.join([str(inspect.signature(getattr(ScriptSpec, fn)))
                              for fn in dir(ScriptSpec)
                              if not fn.startswith('_')]))
            self._script = mod

        return self._script

    @property
    def payload(self) -> Union[BaseModel, Any]:
        """Gets the specification for the request body.

        Returns:
            Union[BaseModel, Any]
        """
        return self.script.payload

    @property
    def model(self) -> Any:
        """Loads the model.

        Returns:
            Any: model.
        """
        if not self._model:
            self._model = self.script.init(self.catalog)

        return self._model

    @property
    def prepare(self) -> Callable[[Any], Any]:
        """Returns the prepare function.

        Returns:
            Callable[[Any], Any]: prepare function.
        """
        return self.script.prepare

    @property
    def predict(self) -> Callable[[Any, Any], ValidResponses]:
        """Returns the predict function.

        Returns:
            Callable[[Any, Any], ValidResponses]: predict function.
        """
        return self.script.predict

    @property
    def _request_id(self) -> str:
        """Generates a unique ID for the request.

        Returns:
            str: unique ID.
        """
        return str(uuid4())

    @property
    def logger(self) -> Logger:
        """Gets the logger.

        Returns:
            Logger: logger.
        """
        return getLogger(__name__)

    def __call__(self, body: Any) -> Tuple[ValidResponses, int]:
        """Makes a prediction for a given request.

        Args:
            body (Any): request data.

        Returns:
            Tuple[ValidResponses, int]: prediction and status code.
        """
        uuid = self._request_id
        try:
            self.logger.info(f'[{uuid}] Request received')
            data = self.prepare(body)
            result = self.predict(self.model, data)
            self.logger.info(f'[{uuid}] Request processed')
            return result, 200  # OK
        except ScriptException as e:
            self.logger.info(f'[{uuid}] Invalid request: "{str(e)}"')
            return str(e), 400  # bad request
        except Exception as e:
            self.logger.info(f'[{uuid}] Script errored: "{str(e)}"')
            return str(e), 500  # internal server error
