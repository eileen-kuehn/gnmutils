from gnmutils.parser.dataparser import DataParser
from gnmutils.process import Process
from gnmutils.job import Job


class JobParser(DataParser):
    """
    The :py:class:`JobParser` is able to :py:func:`parse` raw information on :py:class:`gnmutils.process.Process` es as
    well as :py:class:`gnmutils.traffic.Traffic` into a single :py:class:`gnmutils.job.Job`.
    """
    def __init__(self):
        DataParser.__init__(self)
        self._data = Job()

    def clear_caches(self):
        """
        Overwrites method not implemented by :py:class:`DataParser`. The implementation takes care of deleting
        the actual data and creating a new :py:class:`Job`.
        """
        self._data.clear_caches()

    def parse(self, *args, **kwargs):
        """
        Method to parse the given rows into a single :py:class:`gnmutils.job.Job`.

        :param args: rows to be parsed
        :param id: the id to be used to initialize the :py:class:`gnmutils.job.Job`
        :type id: str
        :param header: the header to be used to parse the given row
        :type header: dict

        If no ``header`` is given, :py:func:`gnmutils.job.Job.default_header` is used to initialize the parsing process.
        """
        if kwargs["id"]:
            self._data.job_id = kwargs["id"]
        if not kwargs["header"]:
            kwargs["header"] = self._data.default_header()
        DataParser.parse(self, *args, **kwargs)

    def _piece_from_dict(self, data_dict=None):
        return Process(**data_dict)
