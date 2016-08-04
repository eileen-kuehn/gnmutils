"""
Module offers abstract definition of a :py:class:`DataParser`.
"""
import logging

from gnmutils.exceptions import ParserNotInitializedException, FilePathException

from evenmoreutils.tree import Node


class DataParser(object):
    """
    A :py:class:`DataParser` is able to parse different formats produced by the GNM tool. Different
    use cases need to be considered:

    * splitting of first initial stream into several jobs
    * accumulation of different events into single events (e.g. for processes)
    * splitting of payloads
    * etc.
    """
    def __init__(self, data_source=None, data_reader=None, path=None, **kwargs):
        self.data_source = data_source
        self.data_reader = data_reader
        self._changed = True
        self._data = None
        self.configuration = None
        self._parsed_data = set()
        if path and kwargs.get("name", None) is None:
            self.load_archive_state(path=path)

    def load_archive_state(self, path):
        """
        Method takes care that an eventually saved state is loaded before going on with parsing.
        This is especially useful when considering different files and when stopping in the
        middle of processing.

        :param str path: path to read archive files from
        """
        if path is None:
            raise FilePathException(value="path=%s" % path)
        if self.data_source is not None:
            self._data = next(self.data_source.object_data(
                pattern="data.pkl",
                path=path
            ), None)
            self.configuration = next(self.data_source.object_data(
                pattern="configuration.pkl",
                path=path
            ), None)
            self._parsed_data = next(self.data_source.object_data(
                pattern="parsed_data.pkl",
                path=path
            ), set())
            self._changed = False

    def data_id(self, value):
        """
        Method to set the id of data that is parsed. As there might be different objects to
        be parsed, this differs for different implementations. E.g. job id, payload id, ...

        :param value: the id to set
        # TODO: this might maybe removed, it is only used for job parser...
        """
        raise NotImplementedError

    @property
    def parsed_data(self):
        """
        Method that returns the set of paths of actual parsed data.

        :return: parsed data paths
        :rtype: set
        """
        return self._parsed_data

    @property
    def data(self):
        """
        Method that returns the actual parsed data.

        :return: data
        """
        return self._data

    def pop_data(self):
        """
        Method that pops the actual parsed data.

        :return: generator to access data
        """
        raise NotImplementedError

    def check_caches(self, **kwargs):
        """
        Method that triggers the check of current caches in use. Maybe there are still some data
        that can be evaluated now?

        :param kwargs: additional parameters
        """
        raise NotImplementedError

    def clear_caches(self):
        """
        Method that clears all caches in use.
        """
        raise NotImplementedError

    def archive_state(self, path, **kwargs):
        """
        Method that archives the current state of the :py:class:`DataParser`. It includes the
        current :py:attr:`data` that was read, the :py:attr:`configuration` as well as already
        :py:attr:`parsed_data`.

        :param kwargs: additional attributes
        """
        if self.data_source is not None:
            self.data_source.write_object_data(
                data=self._data,
                name="data",
                path=path,
                **kwargs
            )
            self.data_source.write_object_data(
                data=self.configuration,
                name="configuration",
                path=path,
                **kwargs
            )
            self.data_source.write_object_data(
                data=self._parsed_data,
                name="parsed_data",
                path=path,
                **kwargs
            )
        else:
            logging.getLogger(self.__class__.__name__).warning(
                "Archiving not done because of missing data_source"
            )

    def parse(self, path, **kwargs):
        """
        This method instantiates the parsing for file given by :py:param:`path` on specified
        :py:class:`DataReader`. It acts as a generator and yields all finished objects.

        :param path:
        :return:
        """
        for data_dict in self.data_reader.data(path=path):
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
