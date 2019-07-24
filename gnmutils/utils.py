import os
import re

from gnmutils.exceptions import NoGNMDirectoryStructure
from gnmutils.utility import path as pathutils


BASE_LEVEL = object()
WORKERNODE_LEVEL = object()
RUN_LEVEL = object()
FILE_LEVEL = object()


def directory_level(path=None):
    path, filename = _to_directory(path)
    if _match_workernode_level(path):
        return WORKERNODE_LEVEL
    splitted_path = os.path.split(path)
    if _match_workernode_level(splitted_path[0]):
        if filename is not None:
            return FILE_LEVEL
        return RUN_LEVEL
    workernode_subdirs = pathutils.getImmediateSubdirectories(path)
    for workernode_subdir in workernode_subdirs:
        if _match_workernode_level(os.path.join(path, workernode_subdir)):
            return BASE_LEVEL
    raise NoGNMDirectoryStructure


def path_components(path=None):
    path, filename = _to_directory(path)
    level = directory_level(path)
    if level == BASE_LEVEL:
        return path, None, None
    if level == WORKERNODE_LEVEL:
        components = os.path.split(path)
        return components[0], components[1]
    if level == RUN_LEVEL or level == FILE_LEVEL:
        run = os.path.split(path)
        components = os.path.split(run[0])
        return components[0], components[1], run[1]


def relevant_directories(path=None):
    path, filename = _to_directory(path)
    try:
        level = directory_level(path)
    except NoGNMDirectoryStructure:
        pass
    else:
        if level == BASE_LEVEL:
            # get workernode directories and also run directories inside path
            for workernode_subdir in pathutils.getImmediateSubdirectories(path):
                directory = os.path.join(path, workernode_subdir)
                if _match_workernode_level(directory):
                    for run_subdir in pathutils.getImmediateSubdirectories(directory):
                        if "unzipped" not in run_subdir:
                            yield path, workernode_subdir, run_subdir, filename
        if level == WORKERNODE_LEVEL:
            # get run directories
            for run_subdir in pathutils.getImmediateSubdirectories(path):
                if "unzipped" not in run_subdir:
                    splitted = os.path.split(path)
                    yield splitted[0], splitted[1], run_subdir, filename
        if level == RUN_LEVEL:
            # identify former directories
            run = os.path.split(path)
            splitted = os.path.split(run[0])
            yield splitted[0], splitted[1], run[1], filename


def _match_workernode_level(path=None):
    return re.match("c\d*-\d*-\d*", os.path.split(path)[1]) and \
           os.path.isdir(path)


def _to_directory(path=None):
    if os.path.isfile(path):
        return os.path.dirname(path), os.path.basename(path)
    return os.path.abspath(path), None
