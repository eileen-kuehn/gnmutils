class DataReader(object):
    def __init__(self, parser=None):
        self._parser = parser

    def read(self, **kwargs):
        """
        Method to be overwritten by actual subclasses.

        :param kwargs:
        :raises: `NotImplementedError`
        """
        raise NotImplementedError

    def parser_name(self):
        """
        Returns the name of the currently used parser.

        :return: Name of the parser
        :rtype: str
        """
        return self._parser.__class__.__name__
