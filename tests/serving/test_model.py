"""Tests model module."""
from pathlib import Path
import pytest
from kedro_projetaai.serving.model import (
    Scorer,
    ScriptSpec,
)
from kedro.io import DataCatalog
from kedro.io import MemoryDataSet


def create_script(path: Path, lines: list) -> str:
    """Creates a script file.

    Args:
        path (Path): path to the script.
        lines (list): lines of the script.

    Returns:
        str: path to the script.
    """
    content = "\n".join(lines)
    path.write_text(content)
    return str(path)


@pytest.fixture()
def fn_script(tmp_path: Path) -> str:
    """Function model script."""
    path = tmp_path / "script.py"
    return create_script(
        path,
        [
            "def init(catalog):",
            "   return lambda x: x+10",
            "def prepare(data):",
            "   return data + 1",
            "def predict(model, prepared_data):",
            "   return model(prepared_data)",
        ],
    )


def test_get_script(fn_script: str):
    """Tests if Scorer returns a ScriptSpec."""
    scr = Scorer(fn_script, DataCatalog())
    mod = scr.script
    assert isinstance(mod, ScriptSpec)


@pytest.fixture
def fn_script_missing_init(tmp_path: Path) -> str:
    """Script missing init."""
    return create_script(
        tmp_path / "script.py",
        [
            "def prepare(data):",
            "   return data + 1",
            "def predict(model, prepared_data):",
            "   return model(prepared_data)",
        ],
    )


def test_protocol(fn_script_missing_init: str):
    """Tests if _get_script checks the protocol implementation."""
    with pytest.raises(AssertionError):
        scr = Scorer(fn_script_missing_init, DataCatalog())
        scr.script


@pytest.fixture
def catalog_script(tmp_path: Path) -> str:
    """Creates a fake script that uses the data catalog."""
    return create_script(
        tmp_path / "script.py",
        [
            "def init(catalog):",
            "   return catalog.load('model')",
            "def prepare(data):",
            "   return data + 1",
            "def predict(model, prepared_data):",
            "   return model(prepared_data)",
        ],
    )


def test_inference_fn(catalog_script: str):
    """Tests if generate_inference_func returns the correct function."""
    catalog = DataCatalog({"model": MemoryDataSet(lambda x: x + 10)})
    scr = Scorer(catalog_script, catalog)
    assert scr(10) == (21, 200)


@pytest.fixture
def catalog_assert_script(tmp_path: Path) -> str:
    """Creates a script that bad requests strings."""
    return create_script(
        tmp_path / "script.py",
        [
            "from kedro_projetaai.serving.model import assert_script",
            "def init(catalog):",
            "   return catalog.load('model')",
            "def prepare(data):",
            "   assert_script(isinstance(data, int), 'data must be int')",
            "   return data + 1",
            "def predict(model, prepared_data):",
            "   return model(prepared_data)",
        ],
    )


def test_inference_bad_request(catalog_assert_script: str):
    """Tests if assertion errors bad requests."""
    catalog = DataCatalog({"model": MemoryDataSet(lambda x: x + 10)})
    scr = Scorer(catalog_assert_script, catalog)
    assert scr("10") == ("data must be int", 400)


@pytest.fixture
def internal_server_error_script(tmp_path: Path) -> str:
    """Creates a script that raises an internal server error."""
    return create_script(
        tmp_path / "script.py",
        [
            "def init(catalog):",
            "   return lambda x: x + 10",
            "def prepare(data):",
            "   return data + 1",
            "def predict(model, prepared_data):",
            "   return [1/0]",
        ],
    )


def test_internal_server_error(internal_server_error_script: str):
    """Tests if internal server errors are handled."""
    catalog = DataCatalog({"model": MemoryDataSet(lambda x: x + 10)})
    scr = Scorer(internal_server_error_script, catalog)
    assert scr(10) == ("division by zero", 500)
