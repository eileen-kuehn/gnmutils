class DataReader(object):
    def __init__(self, parser=None):
        self._parser = parser

    @property
    def parser(self):
        """
        This methods returns the parser that is currently in use for parsing the valid rows found.

        :return: parser being used for row parsing
        :rtype: :py:class:`JobParser`, :py:class:`ProcessParser` or :py:class:`TrafficParser`
        """
        self._parser

    @parser.setter
    def parser(self, value):
        """
        Setting the actual parser being used by :py:class:`DataReader`.

        :param value: the parser to be used
        :type value: Instance of subclass of :py:class:`DataParser`
        """
        if value is not None:
            value.data_reader = self
        self._parser = value

    def read(self, **kwargs):
        """
        Method to be overwritten by actual subclasses.

        :param kwargs:
        :raises: `NotImplementedError`
        """
        raise NotImplementedError

    def data(self, **kwargs):
        raise NotImplementedError

    def parser_name(self):
        """
        Returns the name of the currently used parser.

        :return: Name of the parser
        :rtype: str
        """
        return self._parser.__class__.__name__
