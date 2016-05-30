import re
import os

from gnmutils.parser.dataparser import DataParser
from gnmutils.networkstatistics import NetworkStatistics

from gnmutils.traffic import Traffic
from gnmutils.job import Job

from utility.exceptions import *


class NetworkStatisticsParser(DataParser):
    def __init__(self, data_source=None, data_reader=None, workernode=None, run=None, **kwargs):
        DataParser.__init__(self, data_source, data_reader, **kwargs)
        self._data = self._data or {}
        self._workernode = workernode
        self._run = run
        self._base_tme = 0

    def defaultHeader(self, **kwargs):
        length = kwargs.get("length", 0)
        row = kwargs.get("row", None)
        if row is not None and re.match(".*[A-Za-z]", row):
            # I guess it is a job
            return Job.default_header(length=length)
        traffic_header = Traffic.default_header(length=length)
        if len(traffic_header) == length:
            return traffic_header
        return Job.default_header(length=length)

    def load_archive_state(self, path=None):
        if self._data_source is not None:
            self._configuration = next(self._data_source.object_data(
                pattern="^configuration.pkl",
                path=path
            ), None)
            self._parsed_data = next(self._data_source.object_data(
                pattern="statistics_parsed_data.pkl",
                path=path
            ), set())

    def archive_state(self, **kwargs):
        if self._data_source is not None:
            self._data_source.write_object_data(
                data=self._parsed_data,
                name="statistics_parsed_data",
                **kwargs
            )
        else:
            logging.getLogger(self.__class__.__name__).warning(
                "Archiving not done because of missing data_source"
            )

    def check_caches(self, **kwargs):
        pass

    def clear_caches(self):
        self._data = {}

    def parse(self, **kwargs):
        path = kwargs.get("path", None)
        if self._base_tme == 0:
            self._base_tme = int(re.match(
                "(\d*)-(process|traffic).log-[0-9]{8}",
                os.path.split(path)[1]
            ).group(1))
        return DataParser.parse(self, **kwargs)

    def _get_interval(self):
        if self._configuration is None:
            return 20
        return self._configuration.interval

    def _get_matching_tme(self, tme=None):
        value = (tme - self._base_tme) // self._get_interval()
        return self._base_tme + (value * self._get_interval())

    def _add_piece(self, piece=None):
        tme = self._get_matching_tme(tme=int(piece.get("tme", 0)))
        statistics = self._data.get(
            tme,
            NetworkStatistics(
                workernode=self._workernode,
                run=self._run,
                tme=tme)
        )
        statistics.add(piece)
        self._data[tme] = statistics

    def _piece_from_dict(self, data_dict=None):
        return data_dict

    def pop_data(self):
        yield self._clean_data()

    def _clean_data(self):
        data = self._data
        self._data = {}
        return data
