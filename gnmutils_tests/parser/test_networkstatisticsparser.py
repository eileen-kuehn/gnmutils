import unittest
import os
import gnmutils_tests

from gnmutils.parser.networkstatisticsparser import NetworkStatisticsParser
from gnmutils.sources.filedatasource import FileDataSource
from gnmutils.reader.csvreader import CSVReader


class TestNetworkStatisticsParser(unittest.TestCase):
    def test_creation(self):
        parser = NetworkStatisticsParser()
        self.assertIsNotNone(parser)

    def test_parsing(self):
        data_source = FileDataSource()
        data_reader = CSVReader()
        parser = NetworkStatisticsParser(data_source=data_source)
        data_reader.parser = parser
        for _ in parser.parse(path=self.traffic_file_path()):  # nothing is returned by networkstatisticsparser
            pass
        for _ in parser.parse(path=self.process_file_path()):
            pass
        count = 0
        for data in parser.pop_data():
            for networkstats in data.values():
                count += networkstats.event_count
        self.assertEqual(count, 19998)

    def traffic_file_path(self):
        return os.path.join(
            os.path.dirname(gnmutils_tests.__file__),
            "data/c00-001-001/1/1406555483-traffic.log-20140730"
        )

    def process_file_path(self):
        return os.path.join(
            os.path.dirname(gnmutils_tests.__file__),
            "data/c00-001-001/1/1406555483-process.log-20140730"
        )
