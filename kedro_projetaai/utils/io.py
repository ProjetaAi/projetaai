"""Package for io interaction."""
import os
import shutil
import tomli


def readtoml(filepath: str) -> dict:
    r"""Reads a `.toml` file as a dict.

    Args:
        filepath (str)

    Returns:
        dict

    Example:
        >>> path = fs.create_file('test.toml', '[a]\nb = 1')
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
        >>> source = fs.create_folder('source')
        >>> _ = fs.create_file('source/folder/file1.txt', 'content')
        >>> _ = fs.create_file('source/file1.txt', 'content')
        >>> _ = fs.create_file('source/file2.txt', 'content')
        >>> destination = fs.create_folder('destination')

        >>> _ = fs.create_file('destination/file1.txt', 'overwriteme')
        >>> _ = fs.create_file('destination/folder/file1.txt', 'overwriteme')
        >>> move_files(source, destination)
        >>> sorted(os.listdir(destination))
        ['file1.txt', 'file2.txt', 'folder']

        Overwrites files and folders if they already exist.

        >>> with open('destination/file1.txt', 'r') as f:
        ...     f.read()
        'content'
        >>> with open('destination/folder/file1.txt', 'r') as f:
        ...     f.read()
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
