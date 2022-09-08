"""Package for io interaction."""
import os
import shutil
import tomli
from flatten_dict import flatten, unflatten
import yaml


def readtoml(filepath: str) -> dict:
    r"""Reads a `.toml` file as a dict.

    Args:
        filepath (str)

    Returns:
        dict

    Example:
        >>> path = fs.write('test.toml', '[a]\nb = 1')
        >>> readtoml(path)
        {'a': {'b': 1}}
    """
    with open(filepath, 'rb') as f:
        return tomli.load(f)


# I want a function that takes files and folders from one location and moves it
# to another location. I want the original folder to be deleted after the move.
# I want to overwrite the moved files if they already exist.
def move_files(source: str, destination: str):
    """Move files from one location to another.

    Args:
        source (str): Source folder
        destination (str): Destination folder

    Example:
        >>> source = fs.mkdir('source')
        >>> _ = fs.write('source/folder/file1.txt', 'content')
        >>> _ = fs.write('source/file1.txt', 'content')
        >>> _ = fs.write('source/file2.txt', 'content')
        >>> destination = fs.mkdir('destination')

        >>> _ = fs.write('destination/file1.txt', 'overwriteme')
        >>> _ = fs.write('destination/folder/file1.txt', 'overwriteme')
        >>> move_files(source, destination)
        >>> fs.ls('destination')
        ['file1.txt', 'file2.txt', 'folder']

        Overwrites files and folders if they already exist.

        >>> fs.read('destination/file1.txt')
        'content'
        >>> fs.read('destination/folder/file1.txt')
        'content'
    """
    for file in os.listdir(source):
        dstfile = os.path.join(destination, file)
        if os.path.isfile(dstfile):
            os.remove(dstfile)
        elif os.path.isdir(dstfile):
            shutil.rmtree(dstfile)
        shutil.move(os.path.join(source, file), destination)
    shutil.rmtree(source)


def readyml(filepath: str) -> dict:
    """Reads a `.yml` file as a dict.

    Args:
        filepath (str)

    Returns:
        dict

    Example:
        >>> path = fs.write('test.yml', 'a: 1')
        >>> readyml(path)
        {'a': 1}
    """
    with open(filepath, 'r') as f:
        return yaml.load(f, Loader=yaml.FullLoader)


def writeyml(filepath: str, data: dict):
    """Writes a dict to a `.yml` file.

    Args:
        filepath (str)
        data (dict)

    Example:
        >>> path = fs.write('test.yml', 'a: 1')
        >>> writeyml(path, {'b': 2})
        >>> fs.cat(path)
        b: 2
        <BLANKLINE>
    """
    with open(filepath, 'w') as f:
        yaml.dump(data, f)


def updateyml(filepath: str, data: dict):
    """Update a yml file with new data recursively.

    Args:
        filepath (str): Path to yml file
        data (dict): Data to update yml file with

    Example:
        >>> path = fs.write('test.yml', 'a: 1')
        >>> updateyml(path, {'b': 2})
        >>> fs.cat(path)
        a: 1
        b: 2
        <BLANKLINE>

        >>> writeyml(path, {'a': {'b': 1}})
        >>> updateyml(path, {'a': {'c': 2}})
        >>> fs.cat(path)
        a:
          b: 1
          c: 2
        <BLANKLINE>
    """
    existing_data = readyml(filepath)
    existing_data = flatten(existing_data)
    data = flatten(data)
    existing_data.update(data)
    existing_data = unflatten(existing_data)
    writeyml(filepath, existing_data)
