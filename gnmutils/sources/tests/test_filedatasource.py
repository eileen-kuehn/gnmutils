import unittest

from gnmutils.sources.filedatasource import FileDataSource


class TestFileDataSource(unittest.TestCase):
    def setUp(self):
        self.dataSource = FileDataSource()

    def test_isAvailable(self):
        self.assertTrue(self.dataSource.is_available())

    def test_jobs(self):
        for job in self.dataSource.jobs():
            print("%s: Process count %d" % (job.job_id, job.process_count()))


if __name__ == '__main__':
    unittest.main()
