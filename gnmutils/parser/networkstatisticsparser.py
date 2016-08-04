"""
Module offers an implementation to parse CSV files and accumulate network statistics for further
simulation purposes.
"""
import re
import os
import logging

from gnmutils.parser.dataparser import DataParser
from gnmutils.networkstatistics import NetworkStatistics

from gnmutils.objects.traffic import Traffic
from gnmutils.objects.job import Job


class NetworkStatisticsParser(DataParser):
    """
    The :py:class:`NetworkStatisticsParser` parses process and traffic files from original GNM
    monitoring data and accumulates the bytes needed to transfer all those data over the network.
    Main purpose for this implementation is the generation of data that can be used for simulation.
    The results can further be saved into CSV files.
    """
    def __init__(self, data_source=None, data_reader=None, workernode=None, run=None, **kwargs):
        DataParser.__init__(self, data_source, data_reader, **kwargs)
        self._data = self._data or {}
        self._workernode = workernode
        self._run = run
        self._base_tme = 0

    def data_id(self, value):
        pass

    def defaultHeader(self, length, **kwargs):
        """
        Method returns the default header for compatibility reasons.

        :param length: expected length of header
        :param kwargs: additional arguments
        :return: header for network statistics
        :rtype: dict
        """
        row = kwargs.get("row", None)
        if row is not None and re.match(".*[A-Za-z]", row):
            # I guess it is a job
            return Job.default_header(length=length)
        traffic_header = Traffic.default_header(length=length)
        if len(traffic_header) == length:
            return traffic_header
        return Job.default_header(length=length)

    def load_archive_state(self, path=None):
        if self.data_source is not None:
            self.configuration = next(self.data_source.object_data(
                pattern="configuration.pkl",
                path=path
            ), None)
            self._parsed_data = next(self.data_source.object_data(
                pattern="statistics_parsed_data.pkl",
                path=path
            ), set())

    def archive_state(self, **kwargs):
        if self.data_source is not None:
            self.data_source.write_object_data(
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

    def parse(self, path, **kwargs):
        if self._base_tme == 0:
            self._base_tme = int(re.match(
                "(\d*)-(process|traffic).log-[0-9]{8}",
                os.path.split(path)[1]
            ).group(1))
        return DataParser.parse(self, path=path, **kwargs)

    def _get_interval(self):
        if self.configuration is None:
            return 20
        return self.configuration.interval

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
