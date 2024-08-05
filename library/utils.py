import os


def file_exists(file_path: str) -> bool:
    """
    Check if a file exists.

    Parameters:
        file_path (str): The path to the file.

    Returns:
        bool: True if the file exists, False otherwise.
    """
    return os.path.isfile(file_path)


def path_exists(path: str) -> bool:
    """
    Check if a path exists.

    Parameters:
        path (str): The path to check.

    Returns:
        bool: True if the path exists, False otherwise.
    """
    return os.path.exists(path)


def create_dir(path: str) -> None:
    """
    Create a directory.

    Parameters:
        path (str): The path to the directory.

    Returns:
        None
    """
    os.makedirs(path, exist_ok=True)
