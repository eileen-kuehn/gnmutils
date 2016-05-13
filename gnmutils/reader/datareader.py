class DataReader(object):
    def __init__(self, parser=None):
        self._parser = parser

    @property
    def parser(self):
        self._parser

    @parser.setter
    def parser(self, value):
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
