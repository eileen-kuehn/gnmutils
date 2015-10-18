from gnmutils.exceptions import ParserNotInitializedException
from evenmoreutils.tree import Node

from utility.exceptions import *


class DataParser(object):
    def __init__(self, **kwargs):
        self._data_source = kwargs.get("data_source", None)
        if self._data_source is not None:
            self._data = self._data_source.object_data(
                pattern="data.pkl",
                path=kwargs.get("path", None)
            ).next()
            self._configuration = self._data_source.object_data(
                pattern="configuration.pkl",
                path=kwargs.get("path", None)
            ).next()
            self._parsed_data = self._data_source.object_data(
                pattern="parsed_data.pkl",
                path=kwargs.get("path", None)
            ).next() or set()
        else:
            self._data = None
            self._configuration = None
            self._parsed_data = set()

    @property
    def parsed_data(self):
        return self._parsed_data

    @property
    def configuration(self):
        return self._configuration

    @configuration.setter
    def configuration(self, value):
        self._configuration = value

    @property
    def data(self):
        return self._data

    def check_caches(self):
        raise NotImplementedError

    def clear_caches(self):
        raise NotImplementedError

    def archive_state(self, **kwargs):
        if self._data_source is not None:
            self._data_source.write_object_data(data=self._data, name="data", **kwargs)
            self._data_source.write_object_data(data=self._configuration, name="configuration", **kwargs)
            self._data_source.write_object_data(data=self._parsed_data, name="parsed_data", **kwargs)
        else:
            logging.getLogger(self.__class__.__name__).warning("Archiving not done because of missing data_source")

    def parse(self, *args, **kwargs):
        data_dict = {}
        for arg in args:
            for key in kwargs["header"]:
                data_dict[key] = arg[kwargs["header"][key]]
            piece = self._piece_from_dict(data_dict)
            data = self.add_piece(piece=piece)
            if data is not None:
                yield data

    def add_piece(self, piece=None):
        """
        This method adds patial data to the current data object managed by the Parser.

        :param piece: Partial data to be added
        :raises ParserNotInitializedException: if the data object has not been initialized with specific type
        """
        if self._data is None:
            raise ParserNotInitializedException
        return self._add_piece(piece)

    def _piece_from_dict(self, piece=None):
        raise NotImplementedError

    def _add_piece(self, piece=None):
        node = Node(value=piece)
        self._data.add_node_object(node)
