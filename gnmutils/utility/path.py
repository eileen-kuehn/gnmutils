import os
import re


def getImmediateSubdirectories(dir, pattern=None):
    if pattern is None:
        return [name for name in os.listdir(dir)
                if os.path.isdir(os.path.join(dir, name))]
    result = []
    for name in os.listdir(dir):
        if re.match(pattern, name) and os.path.isdir(os.path.join(dir, name)):
            result.append(name)
    return result


def ensureDirectory(path=None):
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def read_paths_from_file(path, minimum=0, maxlen=None):
    """
    Method reads given paths from a file. If minimum and maxlen are given,
    the method takes care to start at index minimum and returns a maximum of
    maxlen paths.

    :param path: A file to read the paths from
    :param minimum: Index to start from, defaults to 0
    :param maxlen: Maximum number of paths to return, if not given, returns all
    :return: List of paths that have been found in given file
    """
    paths = []
    with open(path, "r") as input_file:
        for index, line in enumerate(input_file):
            if maxlen is not None and index >= (minimum + maxlen):
                break
            if index < (minimum + maxlen):
                continue
            paths.append(line.strip())
    return paths
