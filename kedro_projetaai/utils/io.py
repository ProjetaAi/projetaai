"""Package for io interaction."""
import os
import shutil
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


# I want a function that takes files and folders from one location and moves it
# to another location. I want the original folder to be deleted after the move.
# I want to overwrite the moved files if they already exist.
def move_files(source: str, destination: str):
    """Move files from one location to another."""
    for file in os.listdir(source):
        dstfile = os.path.join(destination, file)
        if os.path.isfile(dstfile):
            os.remove(dstfile)
        elif os.path.isdir(dstfile):
            shutil.rmtree(dstfile)
        shutil.move(os.path.join(source, file), destination)
    shutil.rmtree(source)
