from gnmutils.exceptions import ParserNotInitializedException
from evenmoreutils.tree import Node

from utility.exceptions import *


class DataParser(object):
    def __init__(self, data_source=None, data_reader=None, **kwargs):
        self._data_source = data_source
        self.data_reader = data_reader
        self._changed = True
        self._data = None
        self._configuration = None
        self._parsed_data = set()
        self.load_archive_state(path=kwargs.get("path", None))

    def load_archive_state(self, path=None):
        if self._data_source is not None:
            self._data = next(self._data_source.object_data(
                pattern="^data.pkl",
                path=path
            ), None)
            self._configuration = next(self._data_source.object_data(
                pattern="^configuration.pkl",
                path=path
            ), None)
            self._parsed_data = next(self._data_source.object_data(
                pattern="^parsed_data.pkl",
                path=path
            ), set())
            self._changed = False

    def data_id(self, value):
        raise NotImplementedError

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

    def pop_data(self):
        raise NotImplementedError

    def check_caches(self, **kwargs):
        raise NotImplementedError

    def clear_caches(self):
        raise NotImplementedError

    def archive_state(self, **kwargs):
        if self._data_source is not None:
            self._data_source.write_object_data(
                data=self._data,
                name="data",
                **kwargs
            )
            self._data_source.write_object_data(
                data=self._configuration,
                name="configuration",
                **kwargs
            )
            self._data_source.write_object_data(
                data=self._parsed_data,
                name="parsed_data",
                **kwargs
            )
        else:
            logging.getLogger(self.__class__.__name__).warning(
                "Archiving not done because of missing data_source"
            )

    def parse(self, **kwargs):
        """
        This method instantiates the parsing for file given by :py:param:`path` on specified
        :py:class:`DataReader`. It acts as a generator and yields all finished objects.

        :param path:
        :return:
        """
        for data_dict in self.data_reader.data(path=kwargs.get("path", None)):
            if data_dict is not None:
                piece = self._piece_from_dict(data_dict)
                data = self.add_piece(piece=piece)
                if data is not None:
                    yield data
        for new_data in self._parsing_finished():
            if new_data is not None:
                yield new_data

    def add_piece(self, piece=None):
        """
        This method adds partial data to the current data object managed by the Parser.

        :param piece: Partial data to be added
        :raises ParserNotInitializedException: if the data object has not been initialized with
        specific type
        :return:
        """
        if self._data is None:
            raise ParserNotInitializedException
        return self._add_piece(piece)

    def _piece_from_dict(self, piece=None):
        raise NotImplementedError

    def _add_piece(self, piece=None):
        node = Node(value=piece)
        self._data.add_node_object(node)

    def _parsing_finished(self):
        yield None
