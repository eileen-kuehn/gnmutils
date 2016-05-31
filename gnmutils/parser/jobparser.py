from gnmutils.parser.dataparser import DataParser
from gnmutils.objects.process import Process
from gnmutils.job import Job
from gnmutils.utils import *


class JobParser(DataParser):
    """
    The :py:class:`JobParser` is able to :py:func:`parse` raw information on
    :py:class:`gnmutils.process.Process` es as well as :py:class:`gnmutils.traffic.Traffic` into
    a single :py:class:`gnmutils.job.Job`.
    """
    def __init__(self, data_source=None, data_reader=None, **kwargs):
        DataParser.__init__(self, data_source, data_reader, **kwargs)
        self._data = Job(data_source=self._data_source, path=kwargs.get("path", None))

    def data_id(self, value):
        self._data.db_id = value

    def check_caches(self, **kwargs):
        pass

    def clear_caches(self):
        """
        Overwrites method not implemented by :py:class:`DataParser`. The implementation takes
        care of deleting the actual data and creating a new :py:class:`Job`.
        """
        self._data.clear_caches()

    def parse(self, **kwargs):
        path = kwargs.get("path", None)
        self._data.db_id = re.match("(\d*)-process.csv", os.path.split(path)[1]).group(1)
        base_path, workernode, run = path_components(path)
        self._data.workernode = workernode
        self._data.run = run
        return DataParser.parse(self, **kwargs)

    def _piece_from_dict(self, data_dict=None):
        return Process(**data_dict)

    def _parsing_finished(self):
        if self._data.is_valid() and self._data.is_complete():
            yield self._data
        else:
            yield None