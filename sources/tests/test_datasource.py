import unittest

from gnmutils.sources.datasource import DataSource
from gnmutils.sources.dbbackedfiledatasource import DBBackedFileDataSource


class TestDataSource(unittest.TestCase):
    def test_creation(self):
        data_source = DataSource.best_available_data_source()
        self.assertIsNotNone(data_source)
        self.assertTrue(data_source.is_available())
        self.assertTrue(isinstance(data_source, DBBackedFileDataSource))
