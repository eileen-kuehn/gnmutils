import unittest
import os
import gnmutils_tests

from gnmutils.sources.filedatasource import FileDataSource


class TestFileDataSource(unittest.TestCase):
    def setUp(self):
        self.dataSource = FileDataSource()
        self.path = os.path.join(
            os.path.dirname(gnmutils_tests.__file__),
            "data/c00-001-001"
        )

    def test_isAvailable(self):
        self.assertTrue(self.dataSource.is_available())

    def test_jobs(self):
        index = -1
        for index, job in enumerate(self.dataSource.jobs(path=self.path)):
            self.assertIsNotNone(job)
        self.assertEqual(index, 0)


if __name__ == '__main__':
    unittest.main()
