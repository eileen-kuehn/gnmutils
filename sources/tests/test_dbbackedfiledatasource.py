import unittest

from gnmutils.sources.dbbackedfiledatasource import DBBackedFileDataSource
from utility.exceptions import *


class TestDBBackedFileDataSource(unittest.TestCase):
    def setUp(self):
        self.dataSource = DBBackedFileDataSource()

    def test_setUp(self):
        connection = None
        try:
            connection = self.dataSource._db_data_source.getNewConnection()
        except BasicError:
            pass
        finally:
            self.assertIsNotNone(connection)
        connection.close()
        connection = None

        # test resetup
        self.assertIsNone(connection)
        connection = self.dataSource._db_data_source.getNewConnection()
        self.assertIsNotNone(connection)

    def test_isAvailable(self):
        self.assertTrue(self.dataSource.is_available())

    def test_jobs(self):
        count = 0
        for job in self.dataSource.jobs():
            count += 1
        self.assertEqual(0, count, "Count should be zero with empty database")


if __name__ == '__main__':
    unittest.main()
